//! ExecuteSQL tool - Primary interface for SQL query execution
//!
//! Integrates read-only mode enforcement, row limiting, multi-statement support,
//! and transaction wrapping for consistent database operations.

mod executor;
mod helpers;
mod row_converter;

pub use executor::ExecuteSQLTool;
use helpers::should_use_transaction;

use crate::{
    apply_row_limit, split_sql_statements, validate_readonly_sql,
};
use kodegen_mcp_tool::{Tool, ToolExecutionContext, error::McpError};
use kodegen_mcp_schema::database::{ExecuteSQLArgs, ExecuteSQLPromptArgs};
use rmcp::model::{Content, PromptArgument, PromptMessage, PromptMessageContent, PromptMessageRole};


impl Tool for ExecuteSQLTool {
    type Args = ExecuteSQLArgs;
    type PromptArgs = ExecuteSQLPromptArgs;

    fn name() -> &'static str {
        kodegen_mcp_schema::database::DB_EXECUTE_SQL
    }

    fn description() -> &'static str {
        "Execute SQL query or multiple SQL statements (separated by semicolons). \
         \n\n\
         MULTI-STATEMENT BEHAVIOR:\n\
         - Write operations (INSERT/UPDATE/DELETE/CREATE/ALTER/DROP) use transactions\n\
         - Read operations (SELECT/EXPLAIN/SHOW) execute independently without transaction\n\
         - On transactional error: returns error details without data (all changes rolled back)\n\
         - On non-transactional error: returns committed data plus errors array\n\
         \n\
         Returns query results as JSON with:\n\
         - rows: array of result rows\n\
         - row_count: number of rows returned\n\
         - errors: array of errors (if any failures in non-transactional mode)\n\
         \n\
         Supports read-only mode enforcement and automatic row limiting."
    }

    fn read_only() -> bool {
        false // Can execute write operations (based on config)
    }

    fn destructive() -> bool {
        true // Can delete/modify data
    }

    fn idempotent() -> bool {
        false // Multiple executions have different effects
    }

    fn open_world() -> bool {
        true // Network database connection
    }

    async fn execute(&self, args: Self::Args, _ctx: ToolExecutionContext) -> Result<Vec<Content>, McpError> {
        let start_time = std::time::Instant::now();

        // 1. Get configuration
        let readonly = self
            .config
            .get_value("readonly")
            .and_then(|v| match v {
                kodegen_config_manager::ConfigValue::Boolean(b) => Some(b),
                _ => None,
            })
            .unwrap_or(false);

        let max_rows = self.config.get_value("max_rows").and_then(|v| match v {
            kodegen_config_manager::ConfigValue::Number(n) => Some(n as usize),
            _ => None,
        });

        // 2. Get database type
        let db_type = self.get_database_type()?;

        // 3. Validate read-only mode if enabled
        if readonly {
            validate_readonly_sql(&args.sql, db_type)
                .map_err(|e| anyhow::anyhow!("Read-only violation: {}", e))?;
        }

        // 4. Apply row limiting if configured
        let sql = if let Some(max_rows) = max_rows {
            apply_row_limit(&args.sql, max_rows, db_type)
                .map_err(|e| anyhow::anyhow!("Row limit failed: {}", e))?
        } else {
            args.sql.clone()
        };

        // 5. Split into statements
        let statements = split_sql_statements(&sql, db_type)
            .map_err(|e| anyhow::anyhow!("SQL parse error: {}", e))?;

        // 6. Execute single or multi-statement (get Value from internal methods)
        let result_value = if statements.len() == 1 {
            self.execute_single(&statements[0]).await?
        } else {
            // Route based on statement types
            if should_use_transaction(&statements, db_type) {
                self.execute_multi_transactional(&statements).await?
            } else {
                self.execute_multi_non_transactional(&statements).await?
            }
        };
        
        // 7. Convert Value to Vec<Content>
        let mut contents = Vec::new();
        
        // Extract values from result_value
        let row_count = result_value["row_count"].as_u64().unwrap_or(0);

        // Calculate execution time
        let elapsed_ms = start_time.elapsed().as_millis();

        // Human-readable summary with ANSI colors and Nerd Font icons
        let summary = format!(
            "\x1b[36m SQL Executed\x1b[0m\n\
             Rows: {} · Time: {}ms",
            row_count,
            elapsed_ms
        );
        contents.push(Content::text(summary));
        
        // JSON metadata
        let json_str = serde_json::to_string_pretty(&result_value)
            .unwrap_or_else(|_| "{}".to_string());
        contents.push(Content::text(json_str));
        
        Ok(contents)
    }

    fn prompt_arguments() -> Vec<PromptArgument> {
        vec![PromptArgument {
            name: "database_type".to_string(),
            title: None,
            description: Some(
                "Database type to show examples for (postgres, mysql, sqlite)".to_string(),
            ),
            required: Some(false),
        }]
    }

    async fn prompt(&self, _args: Self::PromptArgs) -> Result<Vec<PromptMessage>, McpError> {
        Ok(vec![
            PromptMessage {
                role: PromptMessageRole::User,
                content: PromptMessageContent::text(
                    "How do I use execute_sql to query and modify a database?",
                ),
            },
            PromptMessage {
                role: PromptMessageRole::Assistant,
                content: PromptMessageContent::text(
                    "The execute_sql tool executes SQL queries and returns results as JSON:\n\n\
                     BASIC USAGE:\n\
                     1. Single query:\n   \
                        execute_sql({\"sql\": \"SELECT * FROM users LIMIT 10\"})\n   \
                        Returns: {\"rows\": [{...}, {...}], \"row_count\": 10}\n\n\
                     2. Multi-statement (uses transaction):\n   \
                        execute_sql({\"sql\": \"BEGIN; INSERT INTO logs VALUES (1, 'test'); COMMIT;\"})\n   \
                        All statements execute atomically - rolls back on error\n\n\
                     3. Data modification:\n   \
                        execute_sql({\"sql\": \"UPDATE users SET status = 'active' WHERE id = 5\"})\n\n\
                     FEATURES:\n\
                     • Read-only mode: When enabled, only SELECT/SHOW/DESCRIBE/EXPLAIN allowed\n\
                     • Row limiting: Automatically applied if max_rows configured\n\
                     • Transactions: Multi-statement queries execute in transaction for consistency\n\
                     • NULL handling: NULL values returned as JSON null\n\n\
                     EXAMPLES BY DATABASE:\n\
                     • PostgreSQL: Supports CTEs, EXPLAIN ANALYZE, JSON types\n\
                     • MySQL: Use SHOW TABLES, DESCRIBE table_name for schema\n\
                     • SQLite: Use .schema or SELECT * FROM sqlite_master\n\n\
                     BEST PRACTICES:\n\
                     • Use LIMIT in SELECT queries to avoid large result sets\n\
                     • Wrap multiple statements in explicit transaction for clarity\n\
                     • Check row_count in response to verify operations\n\
                     • Use schema tools (get_tables, get_table_schema) before querying",
                ),
            },
        ])
    }
}
