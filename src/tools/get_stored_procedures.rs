//! Get stored procedures tool

use crate::error::DatabaseError;
use crate::schema_queries::get_stored_procedures_query;
use crate::tools::helpers::resolve_schema_default;
use crate::tools::timeout::execute_with_timeout;
use crate::types::{DatabaseType, StoredProcedure};
use kodegen_mcp_tool::{Tool, ToolExecutionContext, ToolResponse, error::McpError};
use kodegen_mcp_schema::ToolArgs;
use kodegen_mcp_schema::database::{GetStoredProceduresArgs, GetStoredProceduresPromptArgs, GetStoredProceduresOutput, ProcedureInfo};
use kodegen_config_manager::ConfigManager;
use rmcp::model::{PromptArgument, PromptMessage, PromptMessageContent, PromptMessageRole};
use sqlx::{AnyPool, Row};
use std::sync::Arc;
use std::time::Duration;

/// Tool for listing stored procedures and functions
#[derive(Clone)]
pub struct GetStoredProceduresTool {
    pool: Arc<AnyPool>,
    db_type: DatabaseType,
    config: Arc<ConfigManager>,
}

impl GetStoredProceduresTool {
    /// Create a new GetStoredProceduresTool instance
    pub fn new(
        pool: Arc<AnyPool>,
        connection_url: &str,
        config: Arc<ConfigManager>,
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

impl Tool for GetStoredProceduresTool {
    type Args = GetStoredProceduresArgs;
    type PromptArgs = GetStoredProceduresPromptArgs;

    fn name() -> &'static str {
        kodegen_mcp_schema::database::DB_STORED_PROCEDURES
    }

    fn description() -> &'static str {
        "List stored procedures in a schema. Returns procedure names and optionally \
         detailed information including parameters and definitions. \
         Not supported for SQLite."
    }

    fn read_only() -> bool {
        true // Only reads metadata
    }

    fn open_world() -> bool {
        true // Queries external database
    }

    async fn execute(&self, args: Self::Args, _ctx: ToolExecutionContext) 
        -> Result<ToolResponse<<Self::Args as ToolArgs>::Output>, McpError> 
    {
        // Use stored database type
        let db_type = self.db_type;

        // SQLite doesn't support stored procedures
        if matches!(db_type, DatabaseType::SQLite) {
            return Err(DatabaseError::FeatureNotSupported(
                "SQLite does not support stored procedures".to_string(),
            )
            .into());
        }

        // Resolve schema
        let schema = match args.schema {
            Some(s) => s,
            None => resolve_schema_default(db_type, &self.pool, &self.config).await?,
        };

        // Get query from helper (DBTOOL_5)
        let Some((query, params)) = get_stored_procedures_query(db_type, &schema) else {
            return Err(DatabaseError::FeatureNotSupported(format!(
                "{} does not support stored procedures",
                db_type
            ))
            .into());
        };

        // Execute with parameters and timeout
        let pool = self.pool.clone();
        let query_owned = query.clone();
        let params_owned = params.clone();
        let rows = execute_with_timeout(
            &self.config,
            "db_metadata_query_timeout_secs",
            Duration::from_secs(10), // 10s default for metadata
            || {
                let pool = pool.clone();
                let query = query_owned.clone();
                let params = params_owned.clone();
                async move {
                    let mut q = sqlx::query(&query);
                    for param in &params {
                        q = q.bind(param);
                    }
                    q.fetch_all(&*pool).await
                }
            },
            "Getting stored procedures",
        )
        .await?;

        // Parse into StoredProcedure structs
        let procedures: Vec<StoredProcedure> = rows
            .iter()
            .map(|row| {
                Ok(StoredProcedure {
                    procedure_name: row.try_get("procedure_name").unwrap_or_default(),
                    procedure_type: row.try_get("procedure_type").unwrap_or_default(),
                    language: row.try_get("language").ok(),
                    parameter_list: row.try_get("parameter_list").ok(),
                    return_type: row.try_get("return_type").ok(),
                    definition: if args.include_details {
                        row.try_get("definition").ok()
                    } else {
                        None
                    },
                })
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;

        // Human-readable display
        let display = format!(
            "\x1b[36m󰞔 Stored Procedures: {}\x1b[0m\n ℹ Total: {} · Schema: {}",
            schema,
            procedures.len(),
            schema
        );
        
        // Convert StoredProcedure to ProcedureInfo
        let procedure_info: Vec<ProcedureInfo> = procedures.iter()
            .map(|proc| ProcedureInfo {
                name: proc.procedure_name.clone(),
                procedure_type: proc.procedure_type.clone(),
                language: proc.language.clone(),
                parameters: proc.parameter_list.clone(),
                return_type: proc.return_type.clone(),
                definition: proc.definition.clone(),
            })
            .collect();
        
        // Create typed output
        let output = GetStoredProceduresOutput {
            schema: schema.clone(),
            procedures: procedure_info,
            count: procedures.len(),
        };
        
        Ok(ToolResponse::new(display, output))
    }

    fn prompt_arguments() -> Vec<PromptArgument> {
        vec![
            PromptArgument {
                name: "database_type".to_string(),
                title: None,
                description: Some(
                    "Database type to focus on (postgres, mysql, sqlserver)".to_string(),
                ),
                required: Some(false),
            },
            PromptArgument {
                name: "detail_level".to_string(),
                title: None,
                description: Some(
                    "Detail level for examples (basic or advanced)".to_string(),
                ),
                required: Some(false),
            },
        ]
    }

    async fn prompt(&self, _args: Self::PromptArgs) -> Result<Vec<PromptMessage>, McpError> {
        Ok(vec![
            PromptMessage {
                role: PromptMessageRole::User,
                content: PromptMessageContent::Text {
                    text: "How do I use get_stored_procedures to discover and inspect database procedures?".to_string(),
                },
            },
            PromptMessage {
                role: PromptMessageRole::Assistant,
                content: PromptMessageContent::Text {
                    text: "The get_stored_procedures tool lists stored procedures and functions in a database schema.\n\n\
                           SUPPORTED DATABASES:\n\
                           • PostgreSQL: Full support for functions and procedures\n\
                           • MySQL/MariaDB: Full support for procedures and functions\n\
                           • SQL Server: Full support for stored procedures\n\
                           • SQLite: NOT supported (returns error)\n\n\
                           BASIC USAGE:\n\
                           1. List procedure names only (fast):\n   \
                              get_stored_procedures({\"schema\": \"public\", \"include_details\": false})\n\
                           2. Include full details (slower, more data):\n   \
                              get_stored_procedures({\"schema\": \"public\", \"include_details\": true})\n\n\
                           PARAMETERS:\n\
                           • schema: Schema name (optional, uses database default if omitted)\n\
                           • include_details: Boolean flag\n\
                             - true: Returns parameter lists, return types, and full definitions\n\
                             - false: Returns only procedure/function names and types (recommended for large schemas)\n\n\
                           RETURN FORMAT:\n\
                           JSON object containing:\n{\n  \"schema\": \"public\",\n  \"procedures\": [\n    {\n      \"procedure_name\": \"calculate_total\",\n      \"procedure_type\": \"FUNCTION\",\n      \"language\": \"plpgsql\",\n      \"parameter_list\": \"amount numeric\",\n      \"return_type\": \"numeric\",\n      \"definition\": \"...\"\n    }\n  ],\n  \"count\": 42\n}\n\
                           WORKFLOW INTEGRATION:\n\
                           1. list_schemas() -> discover available schemas\n\
                           2. list_tables(schema='public') -> find tables\n\
                           3. get_stored_procedures(schema='public') -> find related procedures\n\
                           4. get_table_schema(table='...') -> understand table structure\n\
                           5. execute_sql('CALL procedure_name(...)') -> invoke procedure\n\n\
                           KEY PATTERNS:\n\
                           • Use include_details=false for quick discovery\n\
                           • Use include_details=true to examine procedure signatures and implementation\n\
                           • Schema parameter defaults vary by database (public/current/main/dbo)\n\
                           • Procedure names and types help identify callable routines vs. triggers\n\n\
                           BEST PRACTICES:\n\
                           • Check procedure_type to distinguish PROCEDURE from FUNCTION (return values differ)\n\
                           • Review parameter_list before calling to understand input requirements\n\
                           • Use language field to understand implementation (plpgsql, mysql, tsql, etc.)\n\
                           • Call list_schemas() first if you're unsure about schema names"
                        .to_string(),
                },
            },
        ])
    }
}
