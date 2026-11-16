#include "communicator.h"
#include "protocol.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

static int	ps_sock_fd = -1;
static char ps_connected_path[108] = {0};

void
air_communicator_close(void)
{
	if (ps_sock_fd >= 0)
	{
		close(ps_sock_fd);
		ps_sock_fd = -1;
	}
	ps_connected_path[0] = '\0';
}

static int
ensure_connected(const char *socket_path, char *errbuf, size_t errbuf_len)
{
	struct sockaddr_un addr;

	if (!socket_path || socket_path[0] == '\0')
	{
		snprintf(errbuf, errbuf_len, "air_smgr.pageserver_socket is not set");
		return -1;
	}

	if (ps_sock_fd >= 0 && strcmp(ps_connected_path, socket_path) == 0)
		return ps_sock_fd;

	air_communicator_close();

	if (strlen(socket_path) >= sizeof(addr.sun_path))
	{
		snprintf(errbuf, errbuf_len, "pageserver socket path too long: %s", socket_path);
		return -1;
	}

	ps_sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (ps_sock_fd < 0)
	{
		snprintf(errbuf, errbuf_len, "socket(AF_UNIX) failed: %s", strerror(errno));
		return -1;
	}

	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", socket_path);

	if (connect(ps_sock_fd, (struct sockaddr *) &addr, sizeof(addr)) < 0)
	{
		snprintf(errbuf, errbuf_len, "connect(%s) failed: %s", socket_path, strerror(errno));
		air_communicator_close();
		return -1;
	}

	snprintf(ps_connected_path, sizeof(ps_connected_path), "%s", socket_path);
	return ps_sock_fd;
}

static bool
write_full(int fd, const void *buf, size_t len)
{
	const uint8_t *p = (const uint8_t *) buf;
	size_t		off = 0;

	while (off < len)
	{
		ssize_t		n = write(fd, p + off, len - off);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			return false;
		}
		if (n == 0)
			return false;
		off += (size_t) n;
	}
	return true;
}

static bool
read_full(int fd, void *buf, size_t len)
{
	uint8_t    *p = (uint8_t *) buf;
	size_t		off = 0;

	while (off < len)
	{
		ssize_t		n = read(fd, p + off, len - off);

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			return false;
		}
		if (n == 0)
			return false;
		off += (size_t) n;
	}
	return true;
}

static void
fill_req_header(AirPsReqHeader *req,
				AirPsOp op,
				const char *tenant_id,
				RelFileLocator rlocator,
				ForkNumber forknum,
				BlockNumber blocknum,
				BlockNumber nblocks,
				XLogRecPtr lsn)
{
	memset(req, 0, sizeof(*req));
	req->magic = AIR_PS_MAGIC;
	req->version = AIR_PS_VERSION;
	req->op = (uint16_t) op;
	if (tenant_id && tenant_id[0] != '\0')
		strncpy(req->tenant_id, tenant_id, sizeof(req->tenant_id) - 1);
	req->spc_oid = (uint32_t) rlocator.spcOid;
	req->db_oid = (uint32_t) rlocator.dbOid;
	req->rel_number = (uint32_t) rlocator.relNumber;
	req->fork_num = (uint8_t) forknum;
	req->block_num = (uint32_t) blocknum;
	req->nblocks = (uint32_t) nblocks;
	req->lsn = (uint64_t) lsn;
}

static bool
do_rpc(const char *socket_path,
	   const AirPsReqHeader *req,
	   AirPsRespHeader *resp,
	   void **page_buffers,
	   BlockNumber expected_pages,
	   char *errbuf,
	   size_t errbuf_len)
{
	for (int attempt = 0; attempt < 2; attempt++)
	{
		int			fd = ensure_connected(socket_path, errbuf, errbuf_len);

		if (fd < 0)
			return false;

		if (!write_full(fd, req, sizeof(*req)) || !read_full(fd, resp, sizeof(*resp)))
		{
			int			saved = errno;

			air_communicator_close();
			if (attempt == 0)
				continue;
			snprintf(errbuf, errbuf_len, "pageserver RPC I/O failed: %s", strerror(saved));
			return false;
		}

		if (resp->magic != AIR_PS_MAGIC || resp->version != AIR_PS_VERSION)
		{
			air_communicator_close();
			snprintf(errbuf, errbuf_len, "invalid pageserver response header (magic=0x%x ver=%u)",
					 resp->magic, resp->version);
			return false;
		}

		if (resp->status == AIR_PS_STATUS_ERROR)
		{
			char		msg[256];
			uint32_t	to_read = resp->payload_len < sizeof(msg) - 1 ? resp->payload_len : (uint32_t) (sizeof(msg) - 1);

			if (to_read > 0 && read_full(fd, msg, to_read))
			{
				msg[to_read] = '\0';
				snprintf(errbuf, errbuf_len, "pageserver error: %s", msg);
			}
			else
				snprintf(errbuf, errbuf_len, "pageserver returned error status");

			if (resp->payload_len > to_read)
				air_communicator_close();
			return false;
		}

		if (page_buffers != NULL && expected_pages > 0 && resp->status == AIR_PS_STATUS_OK)
		{
			uint32_t	expected_len = (uint32_t) expected_pages * AIR_PAGE_SIZE;

			if (resp->payload_len != expected_len)
			{
				air_communicator_close();
				snprintf(errbuf, errbuf_len, "unexpected payload length %u (expected %u)",
						 resp->payload_len, expected_len);
				return false;
			}
			for (BlockNumber i = 0; i < expected_pages; i++)
			{
				if (!read_full(fd, page_buffers[i], AIR_PAGE_SIZE))
				{
					air_communicator_close();
					snprintf(errbuf, errbuf_len, "short read on pageserver page %u/%u", i + 1, expected_pages);
					return false;
				}
			}
		}
		else if (resp->payload_len > 0)
		{
			/* Drain unexpected trailing payload cleanly */
			char		discard[256];
			uint32_t	rem = resp->payload_len;

			while (rem > 0)
			{
				uint32_t	chunk = rem > sizeof(discard) ? (uint32_t) sizeof(discard) : rem;

				if (!read_full(fd, discard, chunk))
				{
					air_communicator_close();
					break;
				}
				rem -= chunk;
			}
		}

		return true;
	}
	return false;
}

bool
air_ps_get_pages(const char *socket_path,
				 const char *tenant_id,
				 RelFileLocator rlocator,
				 ForkNumber forknum,
				 BlockNumber blocknum,
				 BlockNumber nblocks,
				 XLogRecPtr lsn,
				 void **buffers,
				 char *errbuf,
				 size_t errbuf_len)
{
	AirPsReqHeader req;
	AirPsRespHeader resp;
	AirPsOp		op = (nblocks == 1) ? AIR_PS_OP_GET_PAGE_AT_LSN : AIR_PS_OP_GET_PAGE_BATCH;

	fill_req_header(&req, op, tenant_id, rlocator, forknum, blocknum, nblocks, lsn);
	if (!do_rpc(socket_path, &req, &resp, buffers, nblocks, errbuf, errbuf_len))
		return false;

	if (resp.status == AIR_PS_STATUS_NOT_FOUND)
	{
		for (BlockNumber i = 0; i < nblocks; i++)
			memset(buffers[i], 0, AIR_PAGE_SIZE);
	}
	return true;
}

bool
air_ps_get_rel_size(const char *socket_path,
					const char *tenant_id,
					RelFileLocator rlocator,
					ForkNumber forknum,
					XLogRecPtr lsn,
					BlockNumber *nblocks_out,
					char *errbuf,
					size_t errbuf_len)
{
	AirPsReqHeader req;
	AirPsRespHeader resp;

	fill_req_header(&req, AIR_PS_OP_GET_REL_SIZE, tenant_id, rlocator, forknum, 0, 0, lsn);
	if (!do_rpc(socket_path, &req, &resp, NULL, 0, errbuf, errbuf_len))
		return false;

	*nblocks_out = (resp.status == AIR_PS_STATUS_OK) ? (BlockNumber) resp.nblocks_or_val : 0;
	return true;
}

bool
air_ps_get_rel_exists(const char *socket_path,
					  const char *tenant_id,
					  RelFileLocator rlocator,
					  ForkNumber forknum,
					  XLogRecPtr lsn,
					  bool *exists_out,
					  char *errbuf,
					  size_t errbuf_len)
{
	AirPsReqHeader req;
	AirPsRespHeader resp;

	fill_req_header(&req, AIR_PS_OP_GET_REL_EXISTS, tenant_id, rlocator, forknum, 0, 0, lsn);
	if (!do_rpc(socket_path, &req, &resp, NULL, 0, errbuf, errbuf_len))
		return false;

	*exists_out = (resp.status == AIR_PS_STATUS_OK && resp.nblocks_or_val != 0);
	return true;
}
