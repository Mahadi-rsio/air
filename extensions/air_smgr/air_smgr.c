#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/ipc.h"
#include "storage/smgr.h"
#include "utils/guc.h"

#include "communicator.h"
#include "lfc.h"

#include <string.h>

PG_MODULE_MAGIC;

/* GUCs */
static bool air_smgr_enabled = false;
static bool air_smgr_read_only = false;
static char *air_pageserver_socket = NULL;
static char *air_tenant_id = NULL;
static char *air_lfc_dir = NULL;
static int	air_lfc_max_mb = 64;

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
air_reject_readonly_write(const char *op)
{
	ereport(ERROR,
			(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
			 errmsg("air_smgr is in read-only mode: persistent %s rejected", op)));
}

static void
air_create(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	if (air_smgr_read_only)
		air_reject_readonly_write("create");
	/* Relation creation is recorded via WAL; no local base/ file needed. */
	(void) isRedo;
	air_relmeta_set_create(reln->smgr_rlocator.locator, forknum);
}

static bool
air_exists(SMgrRelation reln, ForkNumber forknum)
{
	char		errbuf[256];
	bool		exists = false;
	BlockNumber local_nblocks = 0;
	bool		local_exact = false;

	if (air_relmeta_lookup(reln->smgr_rlocator.locator, forknum,
						   &exists, &local_nblocks, &local_exact))
		return exists;

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
	char		errbuf[256];

	if (air_smgr_read_only && rlocator.backend == INVALID_PROC_NUMBER)
		air_reject_readonly_write("unlink");

	air_lfc_invalidate_rel(rlocator.locator, forknum, 0);
	air_relmeta_unlink(rlocator.locator, forknum);
	if (!isRedo && air_smgr_enabled && rlocator.backend == INVALID_PROC_NUMBER)
	{
		if (!air_ps_unlink_rel(air_pageserver_socket,
							   air_tenant_id,
							   rlocator.locator,
							   forknum,
							   current_write_lsn(),
							   errbuf,
							   sizeof(errbuf)))
			ereport(WARNING,
					(errmsg("air_smgr unlink failed: %s", errbuf)));
	}
}

static void
air_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   const void *buffer, bool skipFsync)
{
	XLogRecPtr	lsn;
	char		errbuf[256];

	if (air_smgr_read_only)
		air_reject_readonly_write("extend");

	lsn = current_write_lsn();

	(void) skipFsync;
	if (!air_ps_put_pages(air_pageserver_socket,
						  air_tenant_id,
						  reln->smgr_rlocator.locator,
						  forknum,
						  blocknum,
						  1,
						  lsn,
						  &buffer,
						  errbuf,
						  sizeof(errbuf)))
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("air_smgr extend failed: %s", errbuf)));

	air_lfc_insert(air_lfc_dir,
				   air_tenant_id,
				   reln->smgr_rlocator.locator,
				   forknum,
				   blocknum,
				   lsn,
				   buffer);
	air_relmeta_extend(reln->smgr_rlocator.locator, forknum, blocknum + 1);
}

static void
air_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			   int nblocks, bool skipFsync)
{
	static PGAlignedBlock zero_block;
	static bool zero_inited = false;
	const void *stack_ptrs[128];
	const void **ptrs = stack_ptrs;
	XLogRecPtr	lsn;
	char		errbuf[256];
	bool		ok;

	if (air_smgr_read_only)
		air_reject_readonly_write("zeroextend");

	(void) skipFsync;
	if (nblocks <= 0)
		return;

	lsn = current_write_lsn();

	if (!zero_inited)
	{
		memset(zero_block.data, 0, BLCKSZ);
		zero_inited = true;
	}

	if (nblocks > 128)
		ptrs = (const void **) palloc(sizeof(void *) * (size_t) nblocks);
	for (int i = 0; i < nblocks; i++)
		ptrs[i] = zero_block.data;

	ok = air_ps_put_pages(air_pageserver_socket,
						  air_tenant_id,
						  reln->smgr_rlocator.locator,
						  forknum,
						  blocknum,
						  (BlockNumber) nblocks,
						  lsn,
						  ptrs,
						  errbuf,
						  sizeof(errbuf));
	if (ptrs != stack_ptrs)
		pfree(ptrs);

	if (!ok)
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("air_smgr zeroextend failed: %s", errbuf)));

	for (int i = 0; i < nblocks; i++)
		air_lfc_insert(air_lfc_dir,
					   air_tenant_id,
					   reln->smgr_rlocator.locator,
					   forknum,
					   blocknum + (BlockNumber) i,
					   lsn,
					   zero_block.data);

	air_relmeta_extend(reln->smgr_rlocator.locator, forknum,
					   blocknum + (BlockNumber) nblocks);
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
			{
				PageSetChecksumInplace((Page) buffers[run_start + j],
									   blocknum + run_start + j);
				air_lfc_insert(air_lfc_dir,
							   air_tenant_id,
							   reln->smgr_rlocator.locator,
							   forknum,
							   blocknum + run_start + j,
							   lsn,
							   buffers[run_start + j]);
			}

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
	XLogRecPtr	lsn;
	char		errbuf[256];

	if (air_smgr_read_only)
		air_reject_readonly_write("writev");

	lsn = current_write_lsn();

	(void) skipFsync;

	if (nblocks > 0)
	{
		if (!air_ps_put_pages(air_pageserver_socket,
							  air_tenant_id,
							  reln->smgr_rlocator.locator,
							  forknum,
							  blocknum,
							  nblocks,
							  lsn,
							  buffers,
							  errbuf,
							  sizeof(errbuf)))
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("air_smgr writev failed: %s", errbuf)));

		for (BlockNumber i = 0; i < nblocks; i++)
			air_lfc_insert(air_lfc_dir,
						   air_tenant_id,
						   reln->smgr_rlocator.locator,
						   forknum,
						   blocknum + i,
						   lsn,
						   buffers[i]);

		air_relmeta_extend(reln->smgr_rlocator.locator, forknum, blocknum + nblocks);
	}
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
	bool		local_found = false;
	bool		local_exists = false;
	BlockNumber local_nblocks = 0;
	bool		local_exact = false;

	local_found = air_relmeta_lookup(reln->smgr_rlocator.locator, forknum,
									 &local_exists, &local_nblocks, &local_exact);
	if (local_found)
	{
		if (!local_exists)
			return 0;
		if (local_exact)
			return local_nblocks;
	}

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

	if (local_found && local_exists && nblocks < local_nblocks)
		nblocks = local_nblocks;

	return nblocks;
}

static void
air_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old_blocks, BlockNumber nblocks)
{
	if (air_smgr_read_only)
		air_reject_readonly_write("truncate");

	(void) old_blocks;
	air_lfc_invalidate_rel(reln->smgr_rlocator.locator, forknum, nblocks);
	air_relmeta_truncate(reln->smgr_rlocator.locator, forknum, nblocks);
}

static void
air_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	(void) reln;
	(void) forknum;
	if (air_smgr_read_only)
		return;
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

static int64
air_dbsize_hook(Oid spcOid, Oid dbOid)
{
	uint64_t	db_bytes = 0;
	char		errbuf[256];

	if (!air_smgr_enabled)
		return -1;

	if (!air_ps_get_db_size(air_pageserver_socket,
							air_tenant_id,
							spcOid,
							dbOid,
							current_read_lsn(),
							&db_bytes,
							errbuf,
							sizeof(errbuf)))
	{
		ereport(WARNING,
				(errmsg("air_smgr dbsize failed: %s", errbuf)));
		return -1;
	}
	return (int64) db_bytes;
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

	DefineCustomBoolVariable("air_smgr.read_only",
							 "Enforce read-only mode in air_smgr, rejecting persistent writes.",
							 NULL,
							 &air_smgr_read_only,
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
	smgr_dbsize_hook = air_dbsize_hook;

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = air_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = air_shmem_startup;
}
