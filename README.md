<div align="center">
  <img src="assets/img/banner.png" alt="Kodegen AI Banner" width="100%" />
</div>

# KODEGEN Database Tools

**Blazing-Fast MCP Database Tools for AI Agents**

Part of [KODEGEN.ᴀɪ](https://github.com/cyrup-ai/kodegen) - A Rust-native MCP server providing 7 production-ready database tools for autonomous SQL execution and schema exploration across PostgreSQL, MySQL, MariaDB, and SQLite.

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE.md)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE.md)

## Features 

- 🚀 **Multi-Database Support** - PostgreSQL, MySQL, MariaDB, SQLite with unified interface
- 🔒 **SSH Tunnel Support** - Secure connections through bastion hosts
- ⚡ **Connection Pooling** - Efficient connection reuse with configurable pool settings
- 🛡️ **Read-Only Mode** - Prevent accidental data modifications
- 🎯 **Smart Row Limiting** - Automatic result set limiting to prevent memory exhaustion
- 🔄 **Retry Logic** - Exponential backoff with configurable timeouts
- 📊 **Pool Health Monitoring** - Real-time connection pool statistics
- 🔍 **Schema Introspection** - Explore databases, tables, columns, indexes, and stored procedures

## Installation

### As Part of KODEGEN.ᴀɪ 

Install the complete KODEGEN.ᴀɪ toolkit (recommended):

```bash
curl -fsSL https://kodegen.ai/install | sh
```

The database tools are automatically included and configured.

### Standalone Installation

```bash
# Clone repository
git clone https://github.com/cyrup-ai/kodegen-tools-database
cd kodegen-tools-database

# Build and install
cargo install --path .
```

### Running the Server

The database server uses the `DATABASE_DSN` environment variable to connect to a database. If not provided, it defaults to an in-memory SQLite database (`sqlite::memory:`):

```bash
# Basic usage with PostgreSQL
DATABASE_DSN="postgres://user:pass@localhost:5432/mydb" kodegen-database

# Or use the default in-memory SQLite database (no external database required)
kodegen-database

# With SSH tunnel
SSH_HOST="bastion.example.com" \
SSH_PORT="22" \
SSH_USER="username" \
SSH_AUTH_TYPE="password" \
SSH_PASSWORD="secret" \
SSH_TARGET_HOST="internal.db.host" \
SSH_TARGET_PORT="5432" \
DATABASE_DSN="postgres://user:pass@internal.db.host:5432/db" \
kodegen-database
```

## The 7 Database Tools

### 1. db_execute_sql

Execute SQL queries with transaction support, retry logic, and automatic row limiting.

**Features:**
- Multi-statement execution with transactions
- Read-only mode enforcement
- Automatic LIMIT clause injection
- Binary data handling (base64 encoding)
- Configurable timeouts and retries

**Example:**
```javascript
db_execute_sql({
  "sql": "SELECT * FROM employees WHERE department_id = 1",
  "readonly": true,
  "max_rows": 100
})
```

**Response:**
```json
{
  "rows": [
    {"id": 1, "name": "Alice", "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "email": "bob@example.com"}
  ],
  "row_count": 2
}
```

### 2. db_list_schemas

List all databases or schemas available on the server.

**Example:**
```javascript
db_list_schemas({})
```

**Response:**
```json
{
  "schemas": [
    {"name": "public", "type": "schema"},
    {"name": "information_schema", "type": "schema"}
  ]
}
```

### 3. db_list_tables

List all tables within a specific schema/database.

**Example:**
```javascript
db_list_tables({
  "schema": "public"
})
```

**Response:**
```json
{
  "tables": [
    {"name": "employees", "type": "table"},
    {"name": "departments", "type": "table"}
  ]
}
```

### 4. db_table_schema

Get detailed column information for a table.

**Example:**
```javascript
db_table_schema({
  "schema": "public",
  "table": "employees"
})
```

**Response:**
```json
{
  "columns": [
    {
      "name": "id",
      "data_type": "integer",
      "nullable": false,
      "default": "nextval('employees_id_seq'::regclass)"
    },
    {
      "name": "name",
      "data_type": "varchar(255)",
      "nullable": false,
      "default": null
    }
  ]
}
```

### 5. db_table_indexes

Get index information for a table.

**Example:**
```javascript
db_table_indexes({
  "schema": "public",
  "table": "employees"
})
```

**Response:**
```json
{
  "indexes": [
    {
      "name": "employees_pkey",
      "columns": ["id"],
      "unique": true,
      "primary": true
    },
    {
      "name": "idx_employee_department",
      "columns": ["department_id"],
      "unique": false,
      "primary": false
    }
  ]
}
```

### 6. db_stored_procedures

List stored procedures (PostgreSQL and MySQL only).

**Example:**
```javascript
db_stored_procedures({
  "schema": "public"
})
```

**Response:**
```json
{
  "procedures": [
    {
      "name": "get_department_employee_count",
      "schema": "public",
      "return_type": "integer",
      "language": "plpgsql"
    }
  ]
}
```

### 7. db_pool_stats

Monitor connection pool health and performance.

**Example:**
```javascript
db_pool_stats({})
```

**Response:**
```json
{
  "connections": 5,
  "idle_connections": 3,
  "max_connections": 10,
  "min_connections": 2,
  "wait_queue_size": 0
}
```

## Configuration

Control database tool behavior through ConfigManager settings:

### Connection Pool Settings

```json
{
  "db_min_connections": 2,
  "db_max_connections": 10,
  "db_acquire_timeout_secs": 30,
  "db_idle_timeout_secs": 600,
  "db_max_lifetime_secs": 1800
}
```

### Retry Configuration

```json
{
  "db_max_retries": 2,
  "db_retry_backoff_ms": 500,
  "db_max_backoff_ms": 5000
}
```

- **`db_max_retries`** (default: 2) - Maximum retry attempts
- **`db_retry_backoff_ms`** (default: 500) - Base backoff duration
- **`db_max_backoff_ms`** (default: 5000) - Maximum backoff cap

Backoff progression: 500ms → 1000ms → 2000ms → 4000ms (capped at 5000ms)

### Timeout Configuration

```json
{
  "db_query_timeout_secs": 60
}
```

- **`db_query_timeout_secs`** (default: 60) - Per-query timeout in seconds

## SSH Tunnel Support

Secure database connections through SSH bastion hosts using environment variables:

### Password Authentication

```bash
SSH_HOST="bastion.example.com"
SSH_PORT="22"
SSH_USER="username"
SSH_AUTH_TYPE="password"
SSH_PASSWORD="your-password"
SSH_TARGET_HOST="internal.database.host"
SSH_TARGET_PORT="5432"
```

### SSH Key Authentication

```bash
SSH_HOST="bastion.example.com"
SSH_PORT="22"
SSH_USER="username"
SSH_AUTH_TYPE="key"
SSH_KEY_PATH="/path/to/private/key"
SSH_KEY_PASSPHRASE="optional-passphrase"  # Optional
SSH_TARGET_HOST="internal.database.host"
SSH_TARGET_PORT="5432"
```

The tunnel automatically:
- Establishes SSH connection on server startup
- Creates local port forwarding
- Rewrites DSN to use tunnel endpoint
- Handles graceful shutdown and cleanup

## Development & Testing

### Docker-Based Testing

Test all 7 tools across 4 database types with Docker:

```bash
# Start test databases
docker-compose up -d

# Wait for health checks (20-30 seconds)
docker-compose ps

# Run example
cargo run --example database_demo

# Stop containers
docker-compose down
```

### Test Database Schema

The Docker setup provides a universal schema with 5 tables:

**departments** (5 records)
- id, name, budget, created_at

**employees** (15 records)
- id, name, email, department_id, salary, hire_date, active
- Indexes on department_id and name

**projects** (8 records)
- id, name, department_id, start_date, end_date, status
- Indexes on department_id and (status, start_date, end_date)

**employee_projects** (20 records - junction table)
- employee_id, project_id, role, assigned_at
- Composite primary key

**audit_log** (10 records)
- id, table_name, record_id, action, changed_at, changed_by
- Indexes on (table_name, record_id) and changed_at

**Stored Procedure** (PostgreSQL/MySQL/MariaDB):
- `get_department_employee_count(dept_id)` - Returns employee count

### Connection Strings

```bash
# PostgreSQL
postgres://testuser:testpass@localhost:5432/testdb

# MySQL
mysql://testuser:testpass@localhost:3306/testdb

# MariaDB (port 3307 to avoid conflict)
mysql://testuser:testpass@localhost:3307/testdb

# SQLite
sqlite:///tmp/kodegen_test.db
```

### Build Commands

```bash
# Build
cargo build

# Build release
cargo build --release

# Run tests
cargo test

# Run clippy
cargo clippy

# Format code
cargo fmt
```

## Troubleshooting

### Containers won't start

```bash
# Check port conflicts
lsof -i :5432
lsof -i :3306
lsof -i :3307

# View logs
docker-compose logs postgres
docker-compose logs mysql
docker-compose logs mariadb
```

### Schema not loading

```bash
# Recreate with fresh data
docker-compose down -v
docker-compose up -d
```

### Connection failures

```bash
# Verify health
docker-compose ps

# Test manually
docker exec -it kodegen-test-postgres psql -U testuser -d testdb -c "SELECT COUNT(*) FROM employees;"
```

## Architecture

### Core Components

- **src/connection.rs** - Connection pool setup with SSH tunnel support
- **src/ssh_tunnel.rs** - SSH port forwarding implementation
- **src/dsn.rs** - Secure DSN parsing with SecretString
- **src/tools/** - 7 tool implementations
- **src/schema_queries.rs** - Database-specific introspection queries
- **src/sql_parser.rs** - SQL statement parsing and splitting
- **src/sql_limiter.rs** - Automatic row limiting
- **src/readonly.rs** - Read-only SQL validation

### Tool Pattern

All tools follow a consistent pattern:
1. Implement `Tool` trait from `kodegen_mcp_tool`
2. Accept `Arc<AnyPool>` for connection pooling
3. Use `execute_with_timeout()` for retry logic
4. Return structured JSON responses
5. Handle database-specific variations

## Example Workflows

### Schema Exploration

```javascript
// 1. List all schemas
db_list_schemas({})

// 2. List tables in a schema
db_list_tables({"schema": "public"})

// 3. Get table structure
db_table_schema({"schema": "public", "table": "employees"})

// 4. Get indexes
db_table_indexes({"schema": "public", "table": "employees"})
```

### Data Analysis

```javascript
// Execute analytical query
db_execute_sql({
  "sql": `
    SELECT 
      d.name as department,
      COUNT(e.id) as employee_count,
      AVG(e.salary) as avg_salary
    FROM departments d
    LEFT JOIN employees e ON d.id = e.department_id
    WHERE e.active = true
    GROUP BY d.name
    ORDER BY employee_count DESC
  `,
  "readonly": true,
  "max_rows": 50
})
```

### Multi-Statement Transaction

```javascript
db_execute_sql({
  "sql": `
    INSERT INTO departments (name, budget) VALUES ('Engineering', 500000);
    INSERT INTO employees (name, email, department_id, salary, hire_date, active)
    VALUES ('Charlie', 'charlie@example.com', 1, 85000, '2025-01-15', true);
  `,
  "readonly": false
})
```

## Performance

- **Startup Time:** ~25ms (with warmup)
- **Query Latency:** <10ms (simple queries, local DB)
- **Memory Usage:** ~8MB (idle with pool)
- **Connection Pool:** Configurable 2-10 connections
- **Concurrent Queries:** Supports high concurrency via connection pool

## Security

- **SecretString** - Passwords never logged or displayed
- **Read-Only Mode** - Prevent accidental modifications
- **SSH Tunneling** - Secure connections through bastions
- **SQL Validation** - Reject unsafe patterns in read-only mode
- **Row Limiting** - Prevent memory exhaustion attacks

## Contributing

See main [KODEGEN.ᴀɪ repository](https://github.com/cyrup-ai/kodegen) for contribution guidelines.

## License

Dual-licensed under Apache-2.0 and MIT. See [LICENSE.md](LICENSE.md) for details.

## Related Repositories

- [kodegen](https://github.com/cyrup-ai/kodegen) - Main MCP server
- [kodegen-tools-filesystem](https://github.com/cyrup-ai/kodegen-tools-filesystem) - Filesystem tools
- [kodegen-tools-terminal](https://github.com/cyrup-ai/kodegen-tools-terminal) - Terminal tools
- [kodegen-tools-github](https://github.com/cyrup-ai/kodegen-tools-github) - GitHub integration

---

**Part of KODEGEN.ᴀɪ** - The ultimate MCP auto-coding toolset 🚀
