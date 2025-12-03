//! ListSchemas tool for database schema exploration

use kodegen_mcp_tool::{Tool, ToolExecutionContext, ToolResponse, error::McpError};
use kodegen_mcp_schema::ToolArgs;
use kodegen_mcp_schema::database::{ListSchemasArgs, ListSchemasPromptArgs, ListSchemasOutput};
use kodegen_config_manager::ConfigManager;
use rmcp::model::{PromptArgument, PromptMessage, PromptMessageContent, PromptMessageRole};
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
    type PromptArgs = ListSchemasPromptArgs;

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

    fn prompt_arguments() -> Vec<PromptArgument> {
        vec![
            PromptArgument {
                name: "db_type".to_string(),
                title: Some("Database Type".to_string()),
                description: Some(
                    "Optional: Focus examples on a specific database type (e.g., 'postgresql', 'mysql', 'sqlite'). \
                     Helps learn patterns relevant to your actual database system.".to_string(),
                ),
                required: Some(false),
            },
            PromptArgument {
                name: "include_workflow".to_string(),
                title: Some("Include Workflow".to_string()),
                description: Some(
                    "Optional: When true (default), shows the full schema discovery workflow \
                     (list_schemas → list_tables → describe_table → query). \
                     When false, focuses only on list_schemas capabilities.".to_string(),
                ),
                required: Some(false),
            },
        ]
    }

    async fn prompt(&self, args: Self::PromptArgs) -> Result<Vec<PromptMessage>, McpError> {
        // Build customized assistant response based on arguments
        let base_message = "Use the list_schemas tool to discover available schemas/databases:\n\n\
                            **Usage**: list_schemas({})\n\n";
        
        // Part 1: Database-specific examples (filtered by db_type if provided)
        let db_examples = if let Some(ref db_type) = args.db_type {
            match db_type.to_lowercase().as_str() {
                "postgresql" | "postgres" | "pg" => {
                    "**PostgreSQL Specific**:\n\
                     - Returns all user-created schemas (public, myapp, analytics, etc.)\n\
                     - Automatically excludes system schemas (pg_catalog, information_schema)\n\
                     - Useful for multi-tenant database architectures\n\n"
                },
                "mysql" | "mariadb" => {
                    "**MySQL/MariaDB Specific**:\n\
                     - Returns all databases you have GRANT permissions for\n\
                     - No concept of schemas within databases (schemas = databases)\n\
                     - List reflects your database user's privileges\n\n"
                },
                "sqlite" => {
                    "**SQLite Specific**:\n\
                     - Always returns ['main'] since SQLite has no schema concept\n\
                     - For multi-database SQLite setups, use ATTACH DATABASE instead\n\
                     - Schemas are effectively namespaced tables in a single file\n\n"
                },
                _ => {
                    "**What it returns per database type**:\n\
                     - **PostgreSQL**: User schemas like 'public', 'myapp', 'analytics' (excludes pg_catalog, information_schema)\n\
                     - **MySQL/MariaDB**: All databases you have access to\n\
                     - **SQLite**: Always returns ['main'] (SQLite has no schema concept)\n\n"
                }
            }
        } else {
            "**What it returns per database type**:\n\
             - **PostgreSQL**: User schemas like 'public', 'myapp', 'analytics' (excludes pg_catalog, information_schema)\n\
             - **MySQL/MariaDB**: All databases you have access to\n\
             - **SQLite**: Always returns ['main'] (SQLite has no schema concept)\n\n"
        };
        
        // Part 2: Example response (always included)
        let example_response = "**Example response**:\n\
                                ```json\n\
                                {\n\
                                  \"schemas\": [\"public\", \"analytics\", \"staging\"],\n\
                                  \"count\": 3\n\
                                }\n\
                                ```\n\n";
        
        // Part 3: Workflow or isolated usage (conditional based on include_workflow)
        let workflow_section = if args.include_workflow {
            "**Typical schema discovery workflow**:\n\
             1. list_schemas({}) - discover available schemas\n\
             2. list_tables({\"schema\": \"public\"}) - see tables in a schema\n\
             3. describe_table({\"schema\": \"public\", \"table\": \"users\"}) - explore table structure\n\
             4. Execute SQL queries on discovered tables"
        } else {
            "**Key behavior**:\n\
             - list_schemas() takes no arguments\n\
             - Filters results automatically based on your database user's permissions\n\
             - Safe read-only operation (never modifies data)\n\
             - Works with any database type (PostgreSQL, MySQL, SQLite, SQL Server, MariaDB)"
        };
        
        // Build final assistant message
        let assistant_content = format!(
            "{}{}{}{}",
            base_message,
            db_examples,
            example_response,
            workflow_section
        );
        
        Ok(vec![
            PromptMessage {
                role: PromptMessageRole::User,
                content: PromptMessageContent::text(
                    "How do I discover what databases/schemas are available to query?",
                ),
            },
            PromptMessage {
                role: PromptMessageRole::Assistant,
                content: PromptMessageContent::text(assistant_content),
            },
        ])
    }
}
