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
use kodegen_mcp_schema::{Tool, ToolExecutionContext, ToolResponse, McpError};
use kodegen_mcp_schema::ToolArgs;
use kodegen_mcp_schema::database::{ExecuteSQLArgs, DbExecuteSqlPrompts};


impl Tool for ExecuteSQLTool {
    type Args = ExecuteSQLArgs;
    type Prompts = DbExecuteSqlPrompts;

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

    async fn execute(&self, args: Self::Args, _ctx: ToolExecutionContext) 
        -> Result<ToolResponse<<Self::Args as ToolArgs>::Output>, McpError> 
    {
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

        // 6. Execute single or multi-statement (returns typed ExecuteSQLOutput directly)
        let mut output = if statements.len() == 1 {
            self.execute_single(&statements[0]).await?
        } else {
            // Route based on statement types
            if should_use_transaction(&statements, db_type) {
                self.execute_multi_transactional(&statements).await?
            } else {
                self.execute_multi_non_transactional(&statements).await?
            }
        };

        // 7. Set execution time (executor methods set it to 0)
        let elapsed_ms = start_time.elapsed().as_millis() as u64;
        output.execution_time_ms = elapsed_ms;

        // Human-readable display
        let display = format!(
            "\x1b[36m SQL Executed\x1b[0m\n\
             Rows: {} · Time: {}ms",
            output.row_count,
            elapsed_ms
        );
        
        Ok(ToolResponse::new(display, output))
    }

}
