//! ListSchemas tool for database schema exploration

use kodegen_mcp_schema::{Tool, ToolExecutionContext, ToolResponse, McpError};
use kodegen_mcp_schema::ToolArgs;
use kodegen_mcp_schema::database::{ListSchemasArgs, ListSchemasOutput, ListSchemasPrompts};
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
pub struct ListSchemasTool {
    pool: Arc<AnyPool>,
    db_type: DatabaseType,
    config: ConfigManager,
}

impl ListSchemasTool {
    /// Create a new ListSchemas tool instance
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

impl Tool for ListSchemasTool {
    type Args = ListSchemasArgs;
    type Prompts = ListSchemasPrompts;

    fn name() -> &'static str {
        kodegen_mcp_schema::database::DB_LIST_SCHEMAS
    }

    fn description() -> &'static str {
        "List all schemas (databases) in the current database connection. \
         For PostgreSQL, returns all user schemas (excludes pg_catalog, information_schema). \
         For MySQL/MariaDB, returns all databases you have access to. \
         For SQLite, returns ['main']. \
         Returns JSON with schemas array and count."
    }

    fn read_only() -> bool {
        true
    }

    fn open_world() -> bool {
        false
    }

    async fn execute(&self, _args: Self::Args, _ctx: ToolExecutionContext) 
        -> Result<ToolResponse<<Self::Args as ToolArgs>::Output>, McpError> 
    {
        // Use stored database type
        let db_type = self.db_type;

        // SQLite special case - no query needed
        if matches!(db_type, DatabaseType::SQLite) {
            let schemas = vec!["main".to_string()];
            let count = schemas.len();
            
            // Human-readable summary
            let display = format!(
                "🗄️  Available Schemas\n\n\
                 Found {} schema:\n\
                 {}",
                count,
                schemas.iter()
                    .map(|s| format!("  • {}", s))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
            
            let output = ListSchemasOutput { schemas, count };
            return Ok(ToolResponse::new(display, output));
        }

        // Get SQL query from centralized schema_queries module
        let sql = crate::schema_queries::get_schemas_query(db_type);

        // Execute query with timeout (metadata queries should be fast)
        let pool = self.pool.clone();
        let sql_owned = sql.to_string();
        let rows = execute_with_timeout(
            &self.config,
            "db_metadata_query_timeout_secs",
            Duration::from_secs(10), // 10s default for metadata
            || {
                let pool = pool.clone();
                let sql = sql_owned.clone();
                async move { sqlx::query(&sql).fetch_all(&*pool).await }
            },
            "Listing database schemas",
        )
        .await?;

        // Extract schema names
        let schemas: Vec<String> = rows
            .iter()
            .filter_map(|row| row.try_get("schema_name").ok())
            .collect();

        let count = schemas.len();
        
        // Human-readable summary
        let display = format!(
            "🗄️  Available Schemas\n\n\
             Found {} schemas:\n\
             {}",
            count,
            schemas.iter()
                .map(|s| format!("  • {}", s))
                .collect::<Vec<_>>()
                .join("\n")
        );
        
        let output = ListSchemasOutput { schemas, count };
        Ok(ToolResponse::new(display, output))
    }
}
