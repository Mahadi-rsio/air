#include "lfc.h"

#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct AirLfcSlot
{
	Oid			spcOid;
	Oid			dbOid;
	RelFileNumber relNumber;
	ForkNumber	forkNum;
	BlockNumber blockNum;
	XLogRecPtr	lsn;
	int32		next_slot;
	bool		valid;
	bool		refbit;
} AirLfcSlot;

typedef struct AirLfcShmemState
{
	LWLock	   *lock;
	uint32		max_slots;
	uint32		num_buckets;
	uint32		clock_hand;
	int32	   *buckets;
	AirLfcSlot *slots;
} AirLfcShmemState;

static AirLfcShmemState *lfc_state = NULL;
static int	lfc_fd = -1;
static char lfc_opened_path[512] = {0};

static uint32
compute_max_slots(int lfc_max_mb)
{
	if (lfc_max_mb <= 0)
		return 0;
	/* BLCKSZ is 8192 bytes -> 128 blocks per MB */
	return (uint32) lfc_max_mb * 128u;
}

static uint32
next_pow2(uint32 v)
{
	uint32		p = 1024;

	while (p < v && p < (1u << 24))
		p <<= 1;
	return p;
}

static Size
lfc_shmem_size(uint32 max_slots, uint32 num_buckets)
{
	Size		sz = MAXALIGN(sizeof(AirLfcShmemState));

	sz = add_size(sz, MAXALIGN(mul_size(num_buckets, sizeof(int32))));
	sz = add_size(sz, MAXALIGN(mul_size(max_slots, sizeof(AirLfcSlot))));
	return sz;
}

static uint32
hash_key(RelFileLocator rlocator, ForkNumber forknum, BlockNumber blocknum, uint32 num_buckets)
{
	uint64		h = ((uint64) rlocator.spcOid * 0x9e3779b97f4a7c15ULL) ^
	((uint64) rlocator.dbOid * 0xbf58476d1ce4e5b9ULL) ^
	((uint64) rlocator.relNumber * 0x94d049bb133111ebULL) ^
	((uint64) forknum << 32) ^
	(uint64) blocknum;

	h ^= h >> 30;
	h *= 0xbf58476d1ce4e5b9ULL;
	h ^= h >> 27;
	return (uint32) (h & (num_buckets - 1));
}

static bool
slot_matches(const AirLfcSlot *s, RelFileLocator rlocator, ForkNumber forknum, BlockNumber blocknum)
{
	return s->valid &&
		s->spcOid == rlocator.spcOid &&
		s->dbOid == rlocator.dbOid &&
		s->relNumber == rlocator.relNumber &&
		s->forkNum == forknum &&
		s->blockNum == blocknum;
}

void
air_lfc_shmem_request(int lfc_max_mb)
{
	uint32		max_slots = compute_max_slots(lfc_max_mb);

	if (max_slots == 0)
		return;

	RequestAddinShmemSpace(lfc_shmem_size(max_slots, next_pow2(max_slots)));
	RequestNamedLWLockTranche("air_smgr_lfc", 1);
}

void
air_lfc_shmem_startup(int lfc_max_mb)
{
	uint32		max_slots = compute_max_slots(lfc_max_mb);
	uint32		num_buckets;
	bool		found = false;

	if (max_slots == 0)
	{
		lfc_state = NULL;
		return;
	}

	num_buckets = next_pow2(max_slots);

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	lfc_state = ShmemInitStruct("air_smgr_lfc",
								lfc_shmem_size(max_slots, num_buckets),
								&found);
	if (!found)
	{
		char	   *base = (char *) lfc_state;
		Size		hdr_sz = MAXALIGN(sizeof(AirLfcShmemState));
		Size		bkt_sz = MAXALIGN(mul_size(num_buckets, sizeof(int32)));

		lfc_state->lock = &(GetNamedLWLockTranche("air_smgr_lfc"))->lock;
		lfc_state->max_slots = max_slots;
		lfc_state->num_buckets = num_buckets;
		lfc_state->clock_hand = 0;
		lfc_state->buckets = (int32 *) (base + hdr_sz);
		lfc_state->slots = (AirLfcSlot *) (base + hdr_sz + bkt_sz);

		for (uint32 i = 0; i < num_buckets; i++)
			lfc_state->buckets[i] = -1;
		for (uint32 i = 0; i < max_slots; i++)
		{
			lfc_state->slots[i].valid = false;
			lfc_state->slots[i].next_slot = -1;
		}
	}
	LWLockRelease(AddinShmemInitLock);
}

void
air_lfc_close(void)
{
	if (lfc_fd >= 0)
	{
		close(lfc_fd);
		lfc_fd = -1;
	}
	lfc_opened_path[0] = '\0';
}

static int
ensure_lfc_fd(const char *lfc_dir, const char *tenant_id)
{
	char		path[512];
	const char *tid = (tenant_id && tenant_id[0] != '\0') ? tenant_id : "default";
	const char *dir = (lfc_dir && lfc_dir[0] != '\0') ? lfc_dir : "/var/lib/air/lfc";

	snprintf(path, sizeof(path), "%s/%s.lfc", dir, tid);
	if (lfc_fd >= 0 && strcmp(lfc_opened_path, path) == 0)
		return lfc_fd;

	air_lfc_close();

	if (mkdir(dir, 0700) < 0 && errno != EEXIST)
		return -1;

	lfc_fd = open(path, O_RDWR | O_CREAT, 0600);
	if (lfc_fd < 0)
		return -1;

	snprintf(lfc_opened_path, sizeof(lfc_opened_path), "%s", path);
	return lfc_fd;
}

static void
unlink_slot_from_bucket(int32 target_slot)
{
	AirLfcSlot *s = &lfc_state->slots[target_slot];
	uint32		bkt;
	int32	   *prev_ptr;

	if (!s->valid)
		return;

	bkt = hash_key((RelFileLocator) {s->spcOid, s->dbOid, s->relNumber},
				   s->forkNum, s->blockNum, lfc_state->num_buckets);
	prev_ptr = &lfc_state->buckets[bkt];

	while (*prev_ptr >= 0)
	{
		if (*prev_ptr == target_slot)
		{
			*prev_ptr = s->next_slot;
			s->next_slot = -1;
			s->valid = false;
			return;
		}
		prev_ptr = &lfc_state->slots[*prev_ptr].next_slot;
	}
	s->valid = false;
	s->next_slot = -1;
}

static int32
find_slot_locked(RelFileLocator rlocator, ForkNumber forknum, BlockNumber blocknum)
{
	uint32		bkt = hash_key(rlocator, forknum, blocknum, lfc_state->num_buckets);
	int32		cur = lfc_state->buckets[bkt];

	while (cur >= 0)
	{
		if (slot_matches(&lfc_state->slots[cur], rlocator, forknum, blocknum))
			return cur;
		cur = lfc_state->slots[cur].next_slot;
	}
	return -1;
}

static int32
alloc_slot_locked(void)
{
	uint32		n = lfc_state->max_slots;

	for (uint32 scanned = 0; scanned < n * 2; scanned++)
	{
		uint32		idx = lfc_state->clock_hand;
		AirLfcSlot *s = &lfc_state->slots[idx];

		lfc_state->clock_hand = (idx + 1) % n;

		if (!s->valid)
			return (int32) idx;

		if (s->refbit)
		{
			s->refbit = false;
			continue;
		}

		unlink_slot_from_bucket((int32) idx);
		return (int32) idx;
	}

	/* Fallback: evict current hand */
	{
		uint32		idx = lfc_state->clock_hand;

		lfc_state->clock_hand = (idx + 1) % n;
		unlink_slot_from_bucket((int32) idx);
		return (int32) idx;
	}
}

bool
air_lfc_lookup(const char *lfc_dir,
			   const char *tenant_id,
			   RelFileLocator rlocator,
			   ForkNumber forknum,
			   BlockNumber blocknum,
			   XLogRecPtr min_lsn,
			   void *buffer)
{
	int			fd;
	int32		slot;
	bool		hit = false;

	if (!lfc_state)
		return false;

	fd = ensure_lfc_fd(lfc_dir, tenant_id);
	if (fd < 0)
		return false;

	LWLockAcquire(lfc_state->lock, LW_SHARED);
	slot = find_slot_locked(rlocator, forknum, blocknum);
	if (slot >= 0 && lfc_state->slots[slot].lsn >= min_lsn)
	{
		ssize_t		n = pread(fd, buffer, BLCKSZ, (off_t) slot * BLCKSZ);

		if (n == BLCKSZ)
		{
			lfc_state->slots[slot].refbit = true;
			hit = true;
		}
	}
	LWLockRelease(lfc_state->lock);
	return hit;
}

void
air_lfc_insert(const char *lfc_dir,
			   const char *tenant_id,
			   RelFileLocator rlocator,
			   ForkNumber forknum,
			   BlockNumber blocknum,
			   XLogRecPtr lsn,
			   const void *buffer)
{
	int			fd;
	int32		slot;

	if (!lfc_state)
		return;

	fd = ensure_lfc_fd(lfc_dir, tenant_id);
	if (fd < 0)
		return;

	LWLockAcquire(lfc_state->lock, LW_EXCLUSIVE);
	slot = find_slot_locked(rlocator, forknum, blocknum);
	if (slot < 0)
	{
		uint32		bkt = hash_key(rlocator, forknum, blocknum, lfc_state->num_buckets);

		slot = alloc_slot_locked();
		lfc_state->slots[slot].spcOid = rlocator.spcOid;
		lfc_state->slots[slot].dbOid = rlocator.dbOid;
		lfc_state->slots[slot].relNumber = rlocator.relNumber;
		lfc_state->slots[slot].forkNum = forknum;
		lfc_state->slots[slot].blockNum = blocknum;
		lfc_state->slots[slot].next_slot = lfc_state->buckets[bkt];
		lfc_state->buckets[bkt] = slot;
	}

	if (pwrite(fd, buffer, BLCKSZ, (off_t) slot * BLCKSZ) == BLCKSZ)
	{
		lfc_state->slots[slot].lsn = lsn;
		lfc_state->slots[slot].valid = true;
		lfc_state->slots[slot].refbit = true;
	}
	else
	{
		unlink_slot_from_bucket(slot);
	}
	LWLockRelease(lfc_state->lock);
}

void
air_lfc_invalidate_rel(RelFileLocator rlocator,
					   ForkNumber forknum,
					   BlockNumber from_block)
{
	if (!lfc_state)
		return;

	LWLockAcquire(lfc_state->lock, LW_EXCLUSIVE);
	for (uint32 i = 0; i < lfc_state->max_slots; i++)
	{
		AirLfcSlot *s = &lfc_state->slots[i];

		if (!s->valid)
			continue;
		if (s->spcOid == rlocator.spcOid &&
			s->dbOid == rlocator.dbOid &&
			s->relNumber == rlocator.relNumber &&
			(forknum == InvalidForkNumber || s->forkNum == forknum) &&
			s->blockNum >= from_block)
		{
			unlink_slot_from_bucket((int32) i);
		}
	}
	LWLockRelease(lfc_state->lock);
}
