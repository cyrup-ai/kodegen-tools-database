//! ListTables tool for database table exploration

use kodegen_mcp_schema::{Tool, ToolExecutionContext, ToolResponse, McpError};
use kodegen_mcp_schema::ToolArgs;
use kodegen_mcp_schema::database::{ListTablesArgs, ListTablesOutput, TableInfo, ListTablesPrompts};
use kodegen_config_manager::ConfigManager;
use sqlx::{AnyPool, Row};
use std::sync::Arc;
use std::time::Duration;

use crate::tools::timeout::execute_with_timeout;
use crate::types::DatabaseType;

// =============================================================================
// Tool Struct
// =============================================================================

#[derive(Clone)]
pub struct ListTablesTool {
    pool: Arc<AnyPool>,
    db_type: DatabaseType,
    config: ConfigManager,
}

impl ListTablesTool {
    /// Create a new ListTables tool instance
    ///
    /// # Errors
    /// Returns error if connection_url cannot be parsed to determine database type
    pub fn new(
        pool: Arc<AnyPool>,
        connection_url: &str,
        config: ConfigManager,
    ) -> Result<Self, McpError> {
        let db_type = DatabaseType::from_url(connection_url)
            .map_err(|e| McpError::Other(anyhow::anyhow!("Invalid database URL: {}", e)))?;
        Ok(Self {
            pool,
            db_type,
            config,
        })
    }
}

// =============================================================================
// Tool Trait Implementation
// =============================================================================

impl Tool for ListTablesTool {
    type Args = ListTablesArgs;
    type Prompts = ListTablesPrompts;

    fn name() -> &'static str {
        kodegen_mcp_schema::database::DB_LIST_TABLES
    }

    fn description() -> &'static str {
        "List all tables in a schema. If schema not provided, uses default schema \
         (public for PostgreSQL, current database for MySQL, main for SQLite, dbo for SQL Server). \
         Returns JSON with tables array, schema name, and count."
    }

    fn read_only() -> bool {
        true
    }

    fn open_world() -> bool {
        false
    }

    async fn execute(&self, args: Self::Args, _ctx: ToolExecutionContext) 
        -> Result<ToolResponse<<Self::Args as ToolArgs>::Output>, McpError> 
    {
        // Use stored database type
        let db_type = self.db_type;

        // Get SQL query from centralized schema_queries module
        let (sql, params) =
            crate::schema_queries::get_tables_query(db_type, args.schema.as_deref());

        // Determine resolved schema for response
        let resolved_schema = args.schema.unwrap_or_else(|| {
            crate::schema_queries::get_default_schema(db_type)
                .unwrap_or("main")
                .to_string()
        });

        // Execute query with parameters and timeout
        let pool = self.pool.clone();
        let sql_owned = sql.to_string();
        let params_owned = params.clone();
        let rows = execute_with_timeout(
            &self.config,
            "db_metadata_query_timeout_secs",
            Duration::from_secs(10), // 10s default for metadata
            || {
                let pool = pool.clone();
                let sql = sql_owned.clone();
                let params = params_owned.clone();
                async move {
                    let mut query = sqlx::query(&sql);
                    for param in &params {
                        query = query.bind(param);
                    }
                    query.fetch_all(&*pool).await
                }
            },
            "Listing tables",
        )
        .await?;

        // Extract table names
        let tables: Vec<String> = rows
            .iter()
            .filter_map(|row| row.try_get("table_name").ok())
            .collect();

        // Human-readable display
        let display = format!(
            "\x1b[36mTables: {}\x1b[0m\n ℹ Total: {} · Schema: {}",
            resolved_schema,
            tables.len(),
            resolved_schema
        );
        
        // Convert Vec<String> to Vec<TableInfo>
        let table_info: Vec<TableInfo> = tables.iter()
            .map(|name| TableInfo {
                name: name.clone(),
                table_type: None,
            })
            .collect();
        
        // Create typed output
        let output = ListTablesOutput {
            schema: resolved_schema,
            tables: table_info,
            count: tables.len(),
        };
        
        Ok(ToolResponse::new(display, output))
    }
}
