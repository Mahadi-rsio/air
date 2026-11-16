#include "communicator.h"
#include "protocol.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

static bool
srv_read_full(int fd, void *buf, size_t len)
{
	uint8_t    *p = (uint8_t *) buf;
	size_t		off = 0;

	while (off < len)
	{
		ssize_t		n = read(fd, p + off, len - off);

		if (n <= 0)
			return false;
		off += (size_t) n;
	}
	return true;
}

static bool
srv_write_full(int fd, const void *buf, size_t len)
{
	const uint8_t *p = (const uint8_t *) buf;
	size_t		off = 0;

	while (off < len)
	{
		ssize_t		n = write(fd, p + off, len - off);

		if (n <= 0)
			return false;
		off += (size_t) n;
	}
	return true;
}

static void
run_mock_pageserver(int listen_fd)
{
	bool		dropped_once = false;

	for (;;)
	{
		int			cli = accept(listen_fd, NULL, NULL);

		if (cli < 0)
			break;

		for (;;)
		{
			AirPsReqHeader req;
			AirPsRespHeader resp;

			if (!srv_read_full(cli, &req, sizeof(req)))
				break;

			assert(req.magic == AIR_PS_MAGIC);
			assert(req.version == AIR_PS_VERSION);
			assert(strcmp(req.tenant_id, "test_tenant") == 0);

			memset(&resp, 0, sizeof(resp));
			resp.magic = AIR_PS_MAGIC;
			resp.version = AIR_PS_VERSION;

			if (req.op == AIR_PS_OP_GET_REL_EXISTS)
			{
				resp.status = (req.rel_number == 16384) ? AIR_PS_STATUS_OK : AIR_PS_STATUS_NOT_FOUND;
				resp.nblocks_or_val = (req.rel_number == 16384) ? 1 : 0;
				srv_write_full(cli, &resp, sizeof(resp));
			}
			else if (req.op == AIR_PS_OP_GET_REL_SIZE)
			{
				resp.status = AIR_PS_STATUS_OK;
				resp.nblocks_or_val = 42;
				srv_write_full(cli, &resp, sizeof(resp));
			}
			else if (req.op == AIR_PS_OP_GET_PAGE_AT_LSN || req.op == AIR_PS_OP_GET_PAGE_BATCH)
			{
				if (req.block_num == 500 && !dropped_once)
				{
					/* Simulate transient connection drop to test auto-reconnect */
					dropped_once = true;
					break;
				}
				if (req.block_num == 999)
				{
					resp.status = AIR_PS_STATUS_NOT_FOUND;
					srv_write_full(cli, &resp, sizeof(resp));
					continue;
				}
				if (req.block_num == 888)
				{
					const char *msg = "simulated error";

					resp.status = AIR_PS_STATUS_ERROR;
					resp.payload_len = (uint32_t) strlen(msg);
					srv_write_full(cli, &resp, sizeof(resp));
					srv_write_full(cli, msg, strlen(msg));
					continue;
				}

				resp.status = AIR_PS_STATUS_OK;
				resp.payload_len = req.nblocks * AIR_PAGE_SIZE;
				srv_write_full(cli, &resp, sizeof(resp));

				for (uint32_t i = 0; i < req.nblocks; i++)
				{
					uint8_t		page[AIR_PAGE_SIZE];
					uint32_t	blk = req.block_num + i;

					memset(page, 0xA5, sizeof(page));
					memcpy(page, &blk, sizeof(blk));
					memcpy(page + 4, &req.lsn, sizeof(req.lsn));
					srv_write_full(cli, page, sizeof(page));
				}
			}
		}
		close(cli);
	}
	close(listen_fd);
}

int
main(void)
{
	const char *sock_path = "/tmp/air_smgr_test.sock";
	struct sockaddr_un addr;
	int			listen_fd;
	pid_t		pid;
	char		errbuf[256];
	RelFileLocator rloc = {.spcOid = 1663,.dbOid = 5,.relNumber = 16384};
	RelFileLocator missing = {.spcOid = 1663,.dbOid = 5,.relNumber = 99999};

	unlink(sock_path);
	listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	assert(listen_fd >= 0);

	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", sock_path);
	assert(bind(listen_fd, (struct sockaddr *) &addr, sizeof(addr)) == 0);
	assert(listen(listen_fd, 4) == 0);

	pid = fork();
	assert(pid >= 0);
	if (pid == 0)
	{
		run_mock_pageserver(listen_fd);
		_exit(0);
	}
	close(listen_fd);

	/* 1. Exists = true / false */
	{
		bool		exists = false;

		assert(air_ps_get_rel_exists(sock_path, "test_tenant", rloc, 0, 0x12345, &exists, errbuf, sizeof(errbuf)));
		assert(exists == true);

		assert(air_ps_get_rel_exists(sock_path, "test_tenant", missing, 0, 0x12345, &exists, errbuf, sizeof(errbuf)));
		assert(exists == false);
		printf("PASS: get_rel_exists (present + missing)\n");
	}

	/* 2. Rel size */
	{
		BlockNumber nblocks = 0;

		assert(air_ps_get_rel_size(sock_path, "test_tenant", rloc, 0, 0x12345, &nblocks, errbuf, sizeof(errbuf)));
		assert(nblocks == 42);
		printf("PASS: get_rel_size (nblocks=42)\n");
	}

	/* 3. Single page read */
	{
		uint8_t		buf[AIR_PAGE_SIZE];
		void	   *ptrs[1] = {buf};
		uint32_t	blk = 0;
		uint64_t	lsn = 0;

		assert(air_ps_get_pages(sock_path, "test_tenant", rloc, 0, 7, 1, 0xABCDEF01ULL, ptrs, errbuf, sizeof(errbuf)));
		memcpy(&blk, buf, 4);
		memcpy(&lsn, buf + 4, 8);
		assert(blk == 7 && lsn == 0xABCDEF01ULL && buf[100] == 0xA5);
		printf("PASS: get_pages single block\n");
	}

	/* 4. Vectorized multi-block batch + transient drop auto-reconnect */
	{
		uint8_t		b0[AIR_PAGE_SIZE],
					b1[AIR_PAGE_SIZE],
					b2[AIR_PAGE_SIZE];
		void	   *ptrs[3] = {b0, b1, b2};

		assert(air_ps_get_pages(sock_path, "test_tenant", rloc, 0, 500, 3, 0x9999ULL, ptrs, errbuf, sizeof(errbuf)));
		for (uint32_t i = 0; i < 3; i++)
		{
			uint32_t	blk = 0;
			uint64_t	lsn = 0;

			memcpy(&blk, ptrs[i], 4);
			memcpy(&lsn, (uint8_t *) ptrs[i] + 4, 8);
			assert(blk == 500 + i && lsn == 0x9999ULL);
		}
		printf("PASS: get_pages batch + auto-reconnect\n");
	}

	/* 5. NOT_FOUND zero-fills page */
	{
		uint8_t		buf[AIR_PAGE_SIZE];
		void	   *ptrs[1] = {buf};

		memset(buf, 0xFF, sizeof(buf));
		assert(air_ps_get_pages(sock_path, "test_tenant", rloc, 0, 999, 1, 0x100, ptrs, errbuf, sizeof(errbuf)));
		for (size_t i = 0; i < sizeof(buf); i++)
			assert(buf[i] == 0);
		printf("PASS: get_pages NOT_FOUND zero-fill\n");
	}

	/* 6. Error propagation */
	{
		uint8_t		buf[AIR_PAGE_SIZE];
		void	   *ptrs[1] = {buf};

		assert(!air_ps_get_pages(sock_path, "test_tenant", rloc, 0, 888, 1, 0x100, ptrs, errbuf, sizeof(errbuf)));
		assert(strstr(errbuf, "simulated error") != NULL);
		printf("PASS: get_pages error propagation (%s)\n", errbuf);
	}

	air_communicator_close();
	kill(pid, SIGTERM);
	waitpid(pid, NULL, 0);
	unlink(sock_path);
	printf("ALL air_smgr communicator tests passed!\n");
	return 0;
}
