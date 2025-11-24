//! ListTables tool for database table exploration

use kodegen_mcp_tool::{Tool, ToolExecutionContext, error::McpError};
use kodegen_mcp_schema::database::{ListTablesArgs, ListTablesPromptArgs};
use kodegen_config_manager::ConfigManager;
use rmcp::model::{Content, PromptArgument, PromptMessage, PromptMessageContent, PromptMessageRole};
use serde_json::json;
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
    type PromptArgs = ListTablesPromptArgs;

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

    async fn execute(&self, args: Self::Args, _ctx: ToolExecutionContext) -> Result<Vec<Content>, McpError> {
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

        let mut contents = Vec::new();

        // Human-readable summary
        let summary = format!(
            "\x1b[36m󰓅 Tables: {}\x1b[0m\n 󰈙 Total: {} · Schema: {}",
            resolved_schema,
            tables.len(),
            resolved_schema
        );
        contents.push(Content::text(summary));
        
        // JSON metadata
        let metadata = json!({
            "tables": tables,
            "schema": resolved_schema,
            "count": tables.len()
        });
        let json_str = serde_json::to_string_pretty(&metadata)
            .unwrap_or_else(|_| "{}".to_string());
        contents.push(Content::text(json_str));
        
        Ok(contents)
    }

    fn prompt_arguments() -> Vec<PromptArgument> {
        vec![PromptArgument {
            name: "database_type".to_string(),
            title: None,
            description: Some(
                "Optional database system to focus examples on (e.g., 'PostgreSQL', 'MySQL', 'SQLite', 'SQL Server')"
                    .to_string(),
            ),
            required: Some(false),
        }]
    }

    async fn prompt(&self, _args: Self::PromptArgs) -> Result<Vec<PromptMessage>, McpError> {
        Ok(vec![
            PromptMessage {
                role: PromptMessageRole::User,
                content: PromptMessageContent::text(
                    "How do I discover and list tables in different database schemas?",
                ),
            },
            PromptMessage {
                role: PromptMessageRole::Assistant,
                content: PromptMessageContent::text(
                    "Use the list_tables tool to discover available tables in your database:\n\n\
                     **Basic usage**:\n\
                     ```json\n\
                     // List tables in default schema\n\
                     list_tables({})\n\
                     ```\n\n\
                     **Database-specific defaults**:\n\
                     - PostgreSQL: Uses 'public' schema\n\
                     - MySQL/MariaDB: Uses current DATABASE()\n\
                     - SQLite: Uses 'main' (only schema available)\n\
                     - SQL Server: Uses 'dbo' schema\n\n\
                     **Schema-specific queries**:\n\
                     ```json\n\
                     // PostgreSQL - query specific schema\n\
                     list_tables({\"schema\": \"public\"})\n\
                     list_tables({\"schema\": \"information_schema\"})\n\n\
                     // MySQL - query different database\n\
                     list_tables({\"schema\": \"mysql\"})\n\n\
                     // SQL Server - query specific schema\n\
                     list_tables({\"schema\": \"dbo\"})\n\
                     list_tables({\"schema\": \"sys\"})\n\
                     ```\n\n\
                     **Response format**:\n\
                     ```json\n\
                     {\n\
                       \"tables\": [\"users\", \"posts\", \"comments\"],\n\
                       \"schema\": \"public\",\n\
                       \"count\": 3\n\
                     }\n\
                     ```\n\n\
                     **Common workflow**:\n\
                     1. list_tables({}) - explore default schema tables\n\
                     2. describe_table({\"table\": \"users\"}) - inspect table structure\n\
                     3. execute_sql({\"sql\": \"SELECT ...\"}) - query the data\n\n\
                     **Important notes**:\n\
                     - Omitting schema parameter uses database's default (shown above)\n\
                     - Schema names are case-sensitive in PostgreSQL, case-insensitive in MySQL\n\
                     - System schemas (information_schema, sys) are queryable but contain metadata\n\
                     - Response includes count for quick validation",
                ),
            },
        ])
    }
}
