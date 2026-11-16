#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/smgr.h"
#include "utils/guc.h"

#include "communicator.h"
#include "lfc.h"

#include <string.h>

PG_MODULE_MAGIC;

/* GUCs */
static bool air_smgr_enabled = false;
static char *air_pageserver_socket = NULL;
static char *air_tenant_id = NULL;
static char *air_lfc_dir = NULL;
static int	air_lfc_max_mb = 256;

static int	air_smgr_id = -1;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static XLogRecPtr
current_read_lsn(void)
{
	if (RecoveryInProgress())
		return GetXLogReplayRecPtr(NULL);
	return GetXLogInsertRecPtr();
}

static XLogRecPtr
current_write_lsn(void)
{
	XLogRecPtr	lsn = GetXLogInsertRecPtr();

	if (lsn == InvalidXLogRecPtr && RecoveryInProgress())
		lsn = GetXLogReplayRecPtr(NULL);
	return lsn;
}

static void
air_init(void)
{
	/* Nothing to initialize per-backend; sockets and LFC fd open lazily. */
}

static void
air_shutdown(void)
{
	air_communicator_close();
	air_lfc_close();
}

static void
air_open(SMgrRelation reln)
{
	/* Pageserver relations do not require local file descriptors. */
	(void) reln;
}

static void
air_close(SMgrRelation reln, ForkNumber forknum)
{
	(void) reln;
	(void) forknum;
}

static void
air_create(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	/* Relation creation is recorded via WAL; no local base/ file needed. */
	(void) reln;
	(void) forknum;
	(void) isRedo;
}

static bool
air_exists(SMgrRelation reln, ForkNumber forknum)
{
	char		errbuf[256];
	bool		exists = false;

	if (!air_ps_get_rel_exists(air_pageserver_socket,
							   air_tenant_id,
							   reln->smgr_rlocator.locator,
							   forknum,
							   current_read_lsn(),
							   &exists,
							   errbuf,
							   sizeof(errbuf)))
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("air_smgr exists failed: %s", errbuf)));

	return exists;
}

static void
air_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	(void) isRedo;
	air_lfc_invalidate_rel(rlocator.locator, forknum, 0);
}

static void
air_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   const void *buffer, bool skipFsync)
{
	(void) skipFsync;
	air_lfc_insert(air_lfc_dir,
				   air_tenant_id,
				   reln->smgr_rlocator.locator,
				   forknum,
				   blocknum,
				   current_write_lsn(),
				   buffer);
}

static void
air_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			   int nblocks, bool skipFsync)
{
	static PGAlignedBlock zero_block;
	static bool zero_inited = false;
	XLogRecPtr	lsn = current_write_lsn();

	(void) skipFsync;

	if (!zero_inited)
	{
		memset(zero_block.data, 0, BLCKSZ);
		zero_inited = true;
	}

	for (int i = 0; i < nblocks; i++)
		air_lfc_insert(air_lfc_dir,
					   air_tenant_id,
					   reln->smgr_rlocator.locator,
					   forknum,
					   blocknum + (BlockNumber) i,
					   lsn,
					   zero_block.data);
}

static bool
air_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, int nblocks)
{
	(void) reln;
	(void) forknum;
	(void) blocknum;
	(void) nblocks;
	return true;
}

static void
air_readv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		  void **buffers, BlockNumber nblocks)
{
	XLogRecPtr	lsn = current_read_lsn();
	BlockNumber i = 0;
	char		errbuf[256];

	while (i < nblocks)
	{
		if (air_lfc_lookup(air_lfc_dir,
						   air_tenant_id,
						   reln->smgr_rlocator.locator,
						   forknum,
						   blocknum + i,
						   lsn,
						   buffers[i]))
		{
			i++;
			continue;
		}

		/* Coalesce contiguous LFC misses into one Pageserver batch RPC */
		{
			BlockNumber run_start = i;
			BlockNumber run_len = 1;

			while (run_start + run_len < nblocks)
			{
				if (air_lfc_lookup(air_lfc_dir,
								   air_tenant_id,
								   reln->smgr_rlocator.locator,
								   forknum,
								   blocknum + run_start + run_len,
								   lsn,
								   buffers[run_start + run_len]))
					break;
				run_len++;
			}

			if (!air_ps_get_pages(air_pageserver_socket,
								  air_tenant_id,
								  reln->smgr_rlocator.locator,
								  forknum,
								  blocknum + run_start,
								  run_len,
								  lsn,
								  &buffers[run_start],
								  errbuf,
								  sizeof(errbuf)))
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("air_smgr readv failed: %s", errbuf)));

			for (BlockNumber j = 0; j < run_len; j++)
				air_lfc_insert(air_lfc_dir,
							   air_tenant_id,
							   reln->smgr_rlocator.locator,
							   forknum,
							   blocknum + run_start + j,
							   lsn,
							   buffers[run_start + j]);

			i = run_start + run_len;
			if (i < nblocks)
				i++;			/* The block that ended the miss run already hit LFC */
		}
	}
}

static void
air_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	XLogRecPtr	lsn = current_write_lsn();

	(void) skipFsync;

	for (BlockNumber i = 0; i < nblocks; i++)
		air_lfc_insert(air_lfc_dir,
					   air_tenant_id,
					   reln->smgr_rlocator.locator,
					   forknum,
					   blocknum + i,
					   lsn,
					   buffers[i]);
}

static void
air_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks)
{
	(void) reln;
	(void) forknum;
	(void) blocknum;
	(void) nblocks;
}

static BlockNumber
air_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	BlockNumber nblocks = 0;
	char		errbuf[256];

	if (!air_ps_get_rel_size(air_pageserver_socket,
							 air_tenant_id,
							 reln->smgr_rlocator.locator,
							 forknum,
							 current_read_lsn(),
							 &nblocks,
							 errbuf,
							 sizeof(errbuf)))
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("air_smgr nblocks failed: %s", errbuf)));

	return nblocks;
}

static void
air_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old_blocks, BlockNumber nblocks)
{
	(void) old_blocks;
	air_lfc_invalidate_rel(reln->smgr_rlocator.locator, forknum, nblocks);
}

static void
air_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	(void) reln;
	(void) forknum;
	/* Durability is guaranteed by WAL stream to Pageserver. */
	if (!RecoveryInProgress())
		XLogFlush(GetXLogInsertRecPtr());
}

static void
air_registersync(SMgrRelation reln, ForkNumber forknum)
{
	(void) reln;
	(void) forknum;
}

static const f_smgr air_smgr_impl = {
	.smgr_init = air_init,
	.smgr_shutdown = air_shutdown,
	.smgr_open = air_open,
	.smgr_close = air_close,
	.smgr_create = air_create,
	.smgr_exists = air_exists,
	.smgr_unlink = air_unlink,
	.smgr_extend = air_extend,
	.smgr_zeroextend = air_zeroextend,
	.smgr_prefetch = air_prefetch,
	.smgr_readv = air_readv,
	.smgr_writev = air_writev,
	.smgr_writeback = air_writeback,
	.smgr_nblocks = air_nblocks,
	.smgr_truncate = air_truncate,
	.smgr_immedsync = air_immedsync,
	.smgr_registersync = air_registersync,
};

static int
air_smgr_hook(RelFileLocator rlocator, ProcNumber backend)
{
	if (!air_smgr_enabled || backend != INVALID_PROC_NUMBER)
		return 0;				/* standard md.c */

	(void) rlocator;
	return air_smgr_id;
}

static void
air_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	air_lfc_shmem_request(air_lfc_max_mb);
}

static void
air_shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	air_lfc_shmem_startup(air_lfc_max_mb);
}

void
_PG_init(void)
{
	DefineCustomBoolVariable("air_smgr.enabled",
							 "Enable AIR Pageserver storage manager for persistent relations.",
							 NULL,
							 &air_smgr_enabled,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("air_smgr.pageserver_socket",
							   "Unix socket path for AIR Pageserver.",
							   NULL,
							   &air_pageserver_socket,
							   "/var/lib/air/pageserver.sock",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("air_smgr.tenant_id",
							   "AIR tenant identifier for Pageserver requests.",
							   NULL,
							   &air_tenant_id,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("air_smgr.lfc_dir",
							   "Directory for Local File Cache files.",
							   NULL,
							   &air_lfc_dir,
							   "/var/lib/air/lfc",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable("air_smgr.lfc_max_mb",
							"Maximum size of Local File Cache in MB (0 disables LFC).",
							NULL,
							&air_lfc_max_mb,
							256,
							0,
							4096,
							PGC_POSTMASTER,
							GUC_UNIT_MB,
							NULL, NULL, NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	air_smgr_id = smgr_register(&air_smgr_impl);
	smgr_hook = air_smgr_hook;

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = air_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = air_shmem_startup;
}
