# Beginner's Guide: Quickstart & Database Operations

Welcome to **AIR**! This step-by-step guide is designed to take you from zero to running your first multi-tenant PostgreSQL database in minutes, with no prior deep internals knowledge required.

---

## 1. What is AIR in Simple Terms?

Think of AIR as a self-hosted, cloud-native database platform (similar to Neon or Supabase) built with:
- **`air-gateway` (Port `5432`)**: The front door for your applications. Connect with standard `psql`, Python, Node.js, Go, or any PostgreSQL client.
- **`air-server` (Port `8080` / `9090`)**: The control brain. It creates databases, issues credentials, handles backups, and manages instances.
- **`air-read-engine` (Port `5433` / `8082`)**: A blazing fast SQLite-backed read engine that handles `SELECT` queries without waking full PostgreSQL compute.
- **`air-pageserver` (Port `8083` / `9091`)**: Storage layer managing relation pages and Cloudflare R2 / S3 cold storage.
- **`air-meta-postgres` (Port `5435`)**: External PostgreSQL database storing platform configuration and tenant catalogs.

---

## 2. 5-Minute Quickstart with Docker Compose

### Prerequisites
- [Docker Engine & Docker Compose v2](https://docs.docker.com/get-docker/) installed.
- PostgreSQL client (`psql`) or `curl`.

### Step 1: Start the AIR Stack
From your terminal in the repository root:

```bash
# Start all 5 services in detached mode and wait for healthy status
docker compose up -d --wait
```

### Step 2: Check Service Health
Verify that all services are online:

```bash
# air-server (Control Plane)
curl -fsS http://127.0.0.1:8080/healthz
# Expected: {"status":"ok"}

# air-gateway (Client Gateway & Pooler)
curl -fsS http://127.0.0.1:8081/healthz
# Expected: {"status":"ok"}

# air-read-engine (Read Replica)
curl -fsS http://127.0.0.1:8082/healthz
# Expected: {"status":"ok"}

# air-pageserver (Storage Engine)
curl -fsS http://127.0.0.1:8083/healthz
# Expected: {"status":"ok"}
```

If all 4 endpoints return `{"status":"ok"}`, your AIR platform is ready!

---

## 3. Creating Your First Database

In AIR, databases are multi-tenant isolated environments (also called **tenants** or **namespaces**).

To create a new database named `my_first_db`:

```bash
curl -s -X POST http://127.0.0.1:8080/v1/databases \
  -H "Authorization: Bearer air_dev_bootstrap_key" \
  -H "Idempotency-Key: first-run-001" \
  -H "Content-Type: application/json" \
  -d '{"tenant_id": "my_first_db"}'
```

### Response Breakdown
The command returns a JSON response containing connection credentials:

```json
{
  "account_id": "acct_default",
  "tenant_id": "my_first_db",
  "database": "my_first_db",
  "username": "app_user",
  "password": "GENERATED_PASSWORD_HERE",
  "direct_port": 18021,
  "system_identifier": "7711011875213824053",
  "generation": "gen_380e33b412c1b70a",
  "fencing_epoch": 1,
  "state": "ACTIVE"
}
```

> [!IMPORTANT]
> Save the `password` from the JSON response! AIR creates an isolated non-superuser `app_user` for this database.

---

## 4. Connecting and Performing Database Operations

Always connect through the **AIR Gateway on port `5432`**. The gateway automatically routes writes to the PostgreSQL primary and reads to the read-engine.

### Connection String Format
```text
postgresql://app_user:<PASSWORD>@127.0.0.1:5432/<TENANT_ID>?sslmode=disable
```

### Using `psql` Command Line
Replace `<PASSWORD>` with the password received during database creation:

```bash
PGPASSWORD='<PASSWORD>' psql -h 127.0.0.1 -p 5432 -U app_user -d my_first_db
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
  user: 'app_user',
  password: process.env.DB_PASSWORD,
  database: 'my_first_db',
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
    "host=127.0.0.1 port=5432 user=app_user password=YOUR_PASSWORD dbname=my_first_db"
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
	dsn := "postgres://app_user:YOUR_PASSWORD@127.0.0.1:5432/my_first_db?sslmode=disable"
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
curl -s http://127.0.0.1:8080/v1/databases/my_first_db \
  -H "Authorization: Bearer air_dev_bootstrap_key"
```

### 2. Trigger an Immediate Backup
```bash
curl -s -X POST http://127.0.0.1:8080/v1/databases/my_first_db/backups \
  -H "Authorization: Bearer air_dev_bootstrap_key" \
  -H "Content-Type: application/json" \
  -d '{"type": "FULL"}'
```

### 3. List Backups
```bash
curl -s http://127.0.0.1:8080/v1/databases/my_first_db/backups \
  -H "Authorization: Bearer air_dev_bootstrap_key"
```

### 4. Suspend an Idle Database (Scale-to-Zero)
```bash
curl -s -X POST http://127.0.0.1:8081/v1/databases/my_first_db/suspend \
  -H "X-Air-Internal-Token: air-dev-internal-token"
```

### 5. Wake a Suspended Database
You can wake it explicitly or simply send any query via the gateway on port `5432`:
```bash
curl -s -X POST http://127.0.0.1:8081/v1/databases/my_first_db/start \
  -H "X-Air-Internal-Token: air-dev-internal-token"
```

### 6. Delete / Teardown Database
When you are done with the database:
```bash
curl -s -X DELETE http://127.0.0.1:8080/v1/databases/my_first_db \
  -H "Authorization: Bearer air_dev_bootstrap_key"
```

---

## 7. Next Steps & Advanced Topics

Now that you've got your first database running:
- Read the [Setup Guide](../setup.md) for production deployments with Cloudflare R2 and external HA PostgreSQL.
- Check the [API Reference](../api.md) for full HTTP REST and gRPC endpoint specifications.
- Explore [Architecture Documentation](architecture/overview.md) to learn how pageserver disaggregation, query routing, and cold offload work under the hood.
