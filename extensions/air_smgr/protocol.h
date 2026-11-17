#ifndef AIR_SMGR_PROTOCOL_H
#define AIR_SMGR_PROTOCOL_H

#include <stdint.h>

#define AIR_PS_MAGIC 0x41495250u /* "AIRP" */
#define AIR_PS_VERSION 1u
#define AIR_PS_TENANT_MAX 64
#define AIR_PAGE_SIZE 8192u

typedef enum AirPsOp
{
	AIR_PS_OP_GET_PAGE_AT_LSN = 1,
	AIR_PS_OP_GET_REL_SIZE = 2,
	AIR_PS_OP_GET_REL_EXISTS = 3,
	AIR_PS_OP_GET_DB_SIZE = 4,
	AIR_PS_OP_GET_PAGE_BATCH = 5,
	AIR_PS_OP_PUT_PAGE_BATCH = 6,
	AIR_PS_OP_UNLINK_REL = 7
} AirPsOp;

typedef enum AirPsStatus
{
	AIR_PS_STATUS_OK = 0,
	AIR_PS_STATUS_NOT_FOUND = 1,
	AIR_PS_STATUS_ERROR = 2
} AirPsStatus;

typedef struct __attribute__((packed)) AirPsReqHeader
{
	uint32_t	magic;
	uint16_t	version;
	uint16_t	op;
	char		tenant_id[AIR_PS_TENANT_MAX];
	uint32_t	spc_oid;
	uint32_t	db_oid;
	uint32_t	rel_number;
	uint8_t		fork_num;
	uint8_t		reserved[3];
	uint32_t	block_num;
	uint32_t	nblocks;
	uint64_t	lsn;
	uint64_t	not_modified_since;
} AirPsReqHeader;

typedef struct __attribute__((packed)) AirPsRespHeader
{
	uint32_t	magic;
	uint16_t	version;
	uint16_t	status;
	uint32_t	payload_len;
	uint32_t	nblocks_or_val;
	uint64_t	u64_val;
} AirPsRespHeader;

#endif							/* AIR_SMGR_PROTOCOL_H */
