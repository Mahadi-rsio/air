#ifndef AIR_SMGR_COMMUNICATOR_H
#define AIR_SMGR_COMMUNICATOR_H

#include "postgres.h"
#include "access/xlogdefs.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "common/relpath.h"

extern void air_communicator_close(void);

extern bool air_ps_get_pages(const char *socket_path,
							 const char *tenant_id,
							 RelFileLocator rlocator,
							 ForkNumber forknum,
							 BlockNumber blocknum,
							 BlockNumber nblocks,
							 XLogRecPtr lsn,
							 void **buffers,
							 char *errbuf,
							 size_t errbuf_len);

extern bool air_ps_put_pages(const char *socket_path,
							 const char *tenant_id,
							 RelFileLocator rlocator,
							 ForkNumber forknum,
							 BlockNumber blocknum,
							 BlockNumber nblocks,
							 XLogRecPtr lsn,
							 const void **buffers,
							 char *errbuf,
							 size_t errbuf_len);

extern bool air_ps_get_rel_size(const char *socket_path,
								const char *tenant_id,
								RelFileLocator rlocator,
								ForkNumber forknum,
								XLogRecPtr lsn,
								BlockNumber *nblocks_out,
								char *errbuf,
								size_t errbuf_len);

extern bool air_ps_get_rel_exists(const char *socket_path,
								  const char *tenant_id,
								  RelFileLocator rlocator,
								  ForkNumber forknum,
								  XLogRecPtr lsn,
								  bool *exists_out,
								  char *errbuf,
								  size_t errbuf_len);

extern bool air_ps_get_db_size(const char *socket_path,
							   const char *tenant_id,
							   Oid spc_oid,
							   Oid db_oid,
							   XLogRecPtr lsn,
							   uint64_t *bytes_out,
							   char *errbuf,
							   size_t errbuf_len);

extern bool air_ps_unlink_rel(const char *socket_path,
							  const char *tenant_id,
							  RelFileLocator rlocator,
							  ForkNumber forknum,
							  XLogRecPtr lsn,
							  char *errbuf,
							  size_t errbuf_len);

#endif							/* AIR_SMGR_COMMUNICATOR_H */
