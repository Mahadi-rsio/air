#ifndef AIR_SMGR_LFC_H
#define AIR_SMGR_LFC_H

#include "postgres.h"
#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"

extern void air_lfc_shmem_request(int lfc_max_mb);
extern void air_lfc_shmem_startup(int lfc_max_mb);

extern bool air_lfc_lookup(const char *lfc_dir,
						   const char *tenant_id,
						   RelFileLocator rlocator,
						   ForkNumber forknum,
						   BlockNumber blocknum,
						   XLogRecPtr min_lsn,
						   void *buffer);

extern void air_lfc_insert(const char *lfc_dir,
						   const char *tenant_id,
						   RelFileLocator rlocator,
						   ForkNumber forknum,
						   BlockNumber blocknum,
						   XLogRecPtr lsn,
						   const void *buffer);

extern void air_lfc_invalidate_rel(RelFileLocator rlocator,
								   ForkNumber forknum,
								   BlockNumber from_block);

extern void air_lfc_close(void);

#endif							/* AIR_SMGR_LFC_H */
