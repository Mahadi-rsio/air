# Beginner's Guide: Quickstart & Database Operations

Welcome to **AIR**! This step-by-step guide is designed to take you from zero to running your first multi-tenant PostgreSQL database in minutes, with no prior deep internals knowledge required.

---

## 1. What is AIR in Simple Terms?

Think of AIR as a self-hosted, cloud-native database platform (similar to Neon or Supabase) built with:
- **`air-gateway` (Port `5432` / `8081`)**: **The single Control Plane & front door.** Exposes the unified HTTP Control Plane (`:8081`) to manage all services, plus PostgreSQL wire port (`:5432`) for `psql`, Python, Node.js, Go, or any PG client.
- **`air-server` (gRPC `:9090`)**: Primary PostgreSQL 17 compute manager, KMS, and backup coordinator (internal gRPC only).
- **`air-read-engine` (Port `5433` / gRPC `:9092`)**: A fast SQLite-backed read engine that handles `SELECT` queries without waking full PostgreSQL compute (internal gRPC + PG wire only).
- **`air-pageserver` (gRPC `:9091`)**: Storage layer managing relation pages and Cloudflare R2 / S3 cold storage (internal gRPC only).
- **`air-meta-postgres` (internal port `5432`)**: External PostgreSQL database storing platform configuration and tenant catalogs.

---

## 2. 5-Minute Quickstart with Docker Compose

### Prerequisites
- [Docker Engine & Docker Compose v2](https://docs.docker.com/get-docker/) installed.
- PostgreSQL client (`psql`).

### Step 1: Start the AIR Stack
Set up the required secrets and certificates before starting Compose. The SQL gateway is the only published port, so the management examples run inside the gateway container. Export `AIR_INTERNAL_TOKEN` in the shell running the commands. [Compose setup](../setup.md#option-1-run-with-docker-compose-local-build)

```bash
# Start all 5 services in detached mode and wait for healthy status
docker compose up -d --wait
```

### Step 2: Check Service Health
Verify that `air-gateway` and all backend gRPC services are online:

```bash
# Gateway liveness
docker compose exec -T air-gateway curl -fsS http://127.0.0.1:8081/healthz
# Expected: {"status":"ok"}

# Aggregated 4-service cluster health (air-gateway, air-server, air-read-engine, air-pageserver)
docker compose exec -T air-gateway curl -fsS -H "X-Air-Internal-Token: ${AIR_INTERNAL_TOKEN}" http://127.0.0.1:8081/v1/cluster/health
```

If all services report `"status":"ok"`, your AIR platform is ready!

---

## 3. Creating Your First Database

In AIR, databases are multi-tenant isolated environments (also called **tenants** or **namespaces**).

To create a new database named `my_first_db` (via `air-gateway`, which orchestrates `air-server`, `air-read-engine`, and `air-pageserver` over gRPC):

```bash
docker compose exec -T air-gateway curl -s -X POST http://127.0.0.1:8081/v1/databases \
  -H "X-Air-Internal-Token: ${AIR_INTERNAL_TOKEN}" \
  -H "Idempotency-Key: first-run-001" \
  -H "Content-Type: application/json" \
  -d '{"tenant_id": "my_first_db", "account_id": "acct_default"}'
```

### Response Breakdown
The command returns a JSON response containing connection credentials:

```json
{
  "account_id": "acct_default",
  "tenant_id": "my_first_db",
  "database": "my_first_db",
  "username": "my_first_db",
  "password": "GENERATED_PASSWORD_HERE",
  "direct_port": 18021,
  "system_identifier": "7711011875213824053",
  "generation": "gen_380e33b412c1b70a",
  "fencing_epoch": 1,
  "state": "ACTIVE"
}
```

> [!IMPORTANT]
> Save the `password` from the JSON response! AIR creates an isolated non-superuser role matching your database name (`my_first_db`) for authentication and routing.

---

## 4. Connecting and Performing Database Operations

Always connect through the **AIR Gateway on port `5432`**. The gateway automatically routes writes to the PostgreSQL primary and reads to the read-engine.

### Connection String Format

Trust the gateway certificate through the client trust store or `PGSSLROOTCERT`. The examples use `127.0.0.1`, which requires that address as an IP SAN on the gateway public certificate.

```text
postgresql://<TENANT_ID>:<PASSWORD>@127.0.0.1:5432?sslmode=verify-full
```

### Using `psql` Command Line
Replace `<PASSWORD>` with the password received during database creation:

```bash
PGPASSWORD='<PASSWORD>' psql "postgresql://my_first_db@127.0.0.1:5432?sslmode=verify-full"
```

### Running CRUD Operations

Once connected to `psql`, try creating a table and inserting rows:

```sql
-- 1. Create a table
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Insert records (Routed to PostgreSQL Primary)
INSERT INTO products (name, price) VALUES
    ('Espresso Machine', 299.99),
    ('Coffee Grinder', 79.50),
    ('Ceramic Mug', 14.00);

-- 3. Query records (Routed transparently to Read-Engine or Primary)
SELECT id, name, price, created_at FROM products ORDER BY id;

-- 4. Update a record
UPDATE products SET price = 69.99 WHERE name = 'Coffee Grinder';

-- 5. Delete a record
DELETE FROM products WHERE name = 'Ceramic Mug';

-- 6. Verify final state
SELECT COUNT(*) AS total_items, AVG(price) AS average_price FROM products;
```

---

## 5. Code Examples in Popular Languages

### Node.js / TypeScript (`pg`)
```javascript
const { Client } = require('pg');

const client = new Client({
  host: '127.0.0.1',
  port: 5432,
  user: 'my_first_db',
  password: process.env.DB_PASSWORD,
  ssl: false,
});

async function main() {
  await client.connect();

  await client.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      email TEXT UNIQUE NOT NULL
    );
  `);

  await client.query('INSERT INTO users (email) VALUES ($1) ON CONFLICT DO NOTHING', ['alice@example.com']);
  const res = await client.query('SELECT * FROM users');
  console.log('Users in DB:', res.rows);

  await client.end();
}

main().catch(console.error);
```

### Python (`psycopg2` or `psycopg`)
```python
import psycopg

conn = psycopg.connect(
    "postgresql://my_first_db:YOUR_PASSWORD@127.0.0.1:5432?sslmode=verify-full"
)

with conn.cursor() as cur:
    cur.execute("CREATE TABLE IF NOT EXISTS notes (id SERIAL PRIMARY KEY, note TEXT);")
    cur.execute("INSERT INTO notes (note) VALUES (%s)", ("My first note",))
    conn.commit()

    cur.execute("SELECT * FROM notes;")
    print("Notes:", cur.fetchall())

conn.close()
```

### Go (`pgx`)
```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()
	dsn := "postgres://my_first_db:YOUR_PASSWORD@127.0.0.1:5432?sslmode=verify-full"
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("unable to connect: %v", err)
	}
	defer conn.Close(ctx)

	var greeting string
	err = conn.QueryRow(ctx, "SELECT 'Hello from AIR!'").Scan(&greeting)
	if err != nil {
		log.Fatalf("query failed: %v", err)
	}
	fmt.Println(greeting)
}
```

---

## 6. Managing Your Database Lifecycle

AIR lets you inspect, suspend, wake, backup, and delete databases via HTTP REST APIs.

### 1. Inspect Database Status
```bash
docker compose exec -T air-gateway curl -s http://127.0.0.1:8081/v1/databases/my_first_db \
  -H "X-Air-Internal-Token: ${AIR_INTERNAL_TOKEN}"
```

### 2. Trigger an Immediate Backup
```bash
docker compose exec -T air-gateway curl -s -X POST http://127.0.0.1:8081/v1/databases/my_first_db/backups \
  -H "X-Air-Internal-Token: ${AIR_INTERNAL_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"type": "FULL"}'
```

### 3. List Backups
```bash
docker compose exec -T air-gateway curl -s http://127.0.0.1:8081/v1/databases/my_first_db/backups \
  -H "X-Air-Internal-Token: ${AIR_INTERNAL_TOKEN}"
```

### 4. Suspend an Idle Database (Scale-to-Zero)
```bash
docker compose exec -T air-gateway curl -s -X POST http://127.0.0.1:8081/v1/databases/my_first_db/suspend \
  -H "X-Air-Internal-Token: ${AIR_INTERNAL_TOKEN}"
```

### 5. Wake a Suspended Database
You can wake it explicitly or simply send any query via the gateway on port `5432`:
```bash
docker compose exec -T air-gateway curl -s -X POST http://127.0.0.1:8081/v1/databases/my_first_db/start \
  -H "X-Air-Internal-Token: ${AIR_INTERNAL_TOKEN}"
```

### 6. Delete / Teardown Database
Delete the database when you are done with it. If cleanup fails and the tenant remains `DELETING`, resolve the reported error and retry the delete. Retained copies follow your storage retention policy.
```bash
docker compose exec -T air-gateway curl -s -X DELETE http://127.0.0.1:8081/v1/databases/my_first_db \
  -H "X-Air-Internal-Token: ${AIR_INTERNAL_TOKEN}"
```

---

## 7. Next Steps & Advanced Topics

Now that you've got your first database running:
- Read the [Setup Guide](../setup.md) for production deployments with Cloudflare R2 and external HA PostgreSQL.
- Check the [API Reference](../api.md) for full HTTP REST and gRPC endpoint specifications.
- Explore [Architecture Documentation](architecture/overview.md) to learn how pageserver disaggregation, query routing, and cold offload work under the hood.
