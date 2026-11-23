#include "communicator.h"
#include "protocol.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
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

static bool
iov_full(int fd, struct iovec *iov, int iovcnt, bool is_write)
{
	while (iovcnt > 0)
	{
		ssize_t		n = is_write ? writev(fd, iov, iovcnt) : readv(fd, iov, iovcnt);
		size_t		rem;

		if (n < 0)
		{
			if (errno == EINTR)
				continue;
			return false;
		}
		if (n == 0)
			return false;

		rem = (size_t) n;

		while (iovcnt > 0 && rem >= iov[0].iov_len)
		{
			rem -= iov[0].iov_len;
			iov++;
			iovcnt--;
		}
		if (iovcnt > 0 && rem > 0)
		{
			iov[0].iov_base = (uint8_t *) iov[0].iov_base + rem;
			iov[0].iov_len -= rem;
		}
	}
	return true;
}

static bool
writev_full(int fd, struct iovec *iov, int iovcnt)
{
	return iov_full(fd, iov, iovcnt, true);
}

static bool
readv_full(int fd, struct iovec *iov, int iovcnt)
{
	return iov_full(fd, iov, iovcnt, false);
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
			if (expected_pages == 1)
			{
				if (!read_full(fd, page_buffers[0], AIR_PAGE_SIZE))
				{
					air_communicator_close();
					snprintf(errbuf, errbuf_len, "short read on pageserver page 1/1");
					return false;
				}
			}
			else
			{
				BlockNumber done = 0;

				while (done < expected_pages)
				{
					struct iovec iov[128];
					BlockNumber batch = expected_pages - done;

					if (batch > 128)
						batch = 128;
					for (BlockNumber i = 0; i < batch; i++)
					{
						iov[i].iov_base = page_buffers[done + i];
						iov[i].iov_len = AIR_PAGE_SIZE;
					}
					if (!readv_full(fd, iov, (int) batch))
					{
						air_communicator_close();
						snprintf(errbuf, errbuf_len, "short read on pageserver batch (%u pages)", expected_pages);
						return false;
					}
					done += batch;
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
		snprintf(errbuf, errbuf_len,
				 "page not found in pageserver (tenant=%s rel=%u/%u/%u fork=%u blk=%u nblocks=%u lsn=%llu)",
				 tenant_id ? tenant_id : "",
				 (unsigned) rlocator.spcOid,
				 (unsigned) rlocator.dbOid,
				 (unsigned) rlocator.relNumber,
				 (unsigned) forknum,
				 (unsigned) blocknum,
				 (unsigned) nblocks,
				 (unsigned long long) lsn);
		return false;
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

bool
air_ps_get_db_size(const char *socket_path,
				   const char *tenant_id,
				   Oid spc_oid,
				   Oid db_oid,
				   XLogRecPtr lsn,
				   uint64_t *bytes_out,
				   char *errbuf,
				   size_t errbuf_len)
{
	AirPsReqHeader req;
	AirPsRespHeader resp;
	RelFileLocator rlocator;

	rlocator.spcOid = spc_oid;
	rlocator.dbOid = db_oid;
	rlocator.relNumber = 0;

	fill_req_header(&req, AIR_PS_OP_GET_DB_SIZE, tenant_id, rlocator, 0, 0, 0, lsn);
	if (!do_rpc(socket_path, &req, &resp, NULL, 0, errbuf, errbuf_len))
		return false;

	*bytes_out = (resp.status == AIR_PS_STATUS_OK) ? resp.u64_val : 0;
	return true;
}

bool
air_ps_unlink_rel(const char *socket_path,
				  const char *tenant_id,
				  RelFileLocator rlocator,
				  ForkNumber forknum,
				  XLogRecPtr lsn,
				  char *errbuf,
				  size_t errbuf_len)
{
	AirPsReqHeader req;
	AirPsRespHeader resp;

	fill_req_header(&req, AIR_PS_OP_UNLINK_REL, tenant_id, rlocator, forknum, 0, 0, lsn);
	if (!do_rpc(socket_path, &req, &resp, NULL, 0, errbuf, errbuf_len))
		return false;

	return resp.status == AIR_PS_STATUS_OK || resp.status == AIR_PS_STATUS_NOT_FOUND;
}

bool
air_ps_put_pages(const char *socket_path,
				 const char *tenant_id,
				 RelFileLocator rlocator,
				 ForkNumber forknum,
				 BlockNumber blocknum,
				 BlockNumber nblocks,
				 XLogRecPtr lsn,
				 const void **buffers,
				 char *errbuf,
				 size_t errbuf_len)
{
	BlockNumber sent = 0;

	while (sent < nblocks)
	{
		BlockNumber chunk = nblocks - sent;

		if (chunk > 128)
			chunk = 128;

		AirPsReqHeader req;
		AirPsRespHeader resp;

		fill_req_header(&req, AIR_PS_OP_PUT_PAGE_BATCH, tenant_id, rlocator,
						forknum, blocknum + sent, chunk, lsn);

		bool		ok = false;

		for (int attempt = 0; attempt < 2; attempt++)
		{
			struct iovec iov[1 + 128];
			int			fd = ensure_connected(socket_path, errbuf, errbuf_len);

			if (fd < 0)
				return false;

			iov[0].iov_base = &req;
			iov[0].iov_len = sizeof(req);
			for (BlockNumber i = 0; i < chunk; i++)
			{
				/* writev only reads from iov_base */
				iov[1 + i].iov_base = (void *) (uintptr_t) buffers[sent + i];
				iov[1 + i].iov_len = AIR_PAGE_SIZE;
			}

			if (!writev_full(fd, iov, 1 + (int) chunk) || !read_full(fd, &resp, sizeof(resp)))
			{
				int			saved = errno;

				air_communicator_close();
				if (attempt == 0)
					continue;
				snprintf(errbuf, errbuf_len, "pageserver put I/O failed: %s", strerror(saved));
				return false;
			}

			if (resp.magic != AIR_PS_MAGIC || resp.version != AIR_PS_VERSION || resp.status != AIR_PS_STATUS_OK)
			{
				air_communicator_close();
				snprintf(errbuf, errbuf_len, "pageserver put failed (status=%u)", resp.status);
				return false;
			}

			ok = true;
			break;
		}

		if (!ok)
			return false;
		sent += chunk;
	}
	return true;
}
