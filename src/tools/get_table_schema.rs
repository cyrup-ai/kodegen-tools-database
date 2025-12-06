//! Get table schema (column information) tool

use crate::error::DatabaseError;
use crate::schema_queries::get_table_schema_query;
use crate::tools::helpers::resolve_schema_default;
use crate::tools::timeout::execute_with_timeout;
use crate::types::{DatabaseType, TableColumn};
use kodegen_mcp_schema::{Tool, ToolExecutionContext, ToolResponse, McpError};
use kodegen_mcp_schema::ToolArgs;
use kodegen_mcp_schema::database::{GetTableSchemaArgs, GetTableSchemaOutput, ColumnInfo, TableSchemaPrompts};
use kodegen_config_manager::ConfigManager;

use sqlx::{AnyPool, Row};
use std::sync::Arc;
use std::time::Duration;

/// Tool for retrieving table column information
#[derive(Clone)]
pub struct GetTableSchemaTool {
    pool: Arc<AnyPool>,
    db_type: DatabaseType,
    config: Arc<ConfigManager>,
}

impl GetTableSchemaTool {
    /// Create a new GetTableSchemaTool instance
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

impl Tool for GetTableSchemaTool {
    type Args = GetTableSchemaArgs;
    type Prompts = TableSchemaPrompts;

    fn name() -> &'static str {
        kodegen_mcp_schema::database::DB_TABLE_SCHEMA
    }

    fn description() -> &'static str {
        "Get column information for a table including column names, data types, \
         nullability, and default values. Use this before writing queries to \
         understand the table structure. Returns array of columns with metadata."
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

        // Resolve schema (use provided or default)
        let schema = match args.schema {
            Some(s) => s,
            None => resolve_schema_default(db_type, &self.pool, &self.config).await?,
        };

        // Get query from helper (DBTOOL_5) - validation enforced for SQLite
        let (query, params) = get_table_schema_query(db_type, &schema, &args.table)?;

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
            "Getting table schema",
        )
        .await?;

        // Parse into TableColumn structs
        let columns: Vec<TableColumn> = rows
            .iter()
            .map(|row| {
                Ok(TableColumn {
                    column_name: row
                        .try_get("column_name")
                        .or_else(|_| row.try_get("name"))
                        .unwrap_or_default(),
                    data_type: row
                        .try_get("data_type")
                        .or_else(|_| row.try_get("type"))
                        .unwrap_or_default(),
                    is_nullable: row
                        .try_get("is_nullable")
                        .or_else(|_| {
                            // SQLite: notnull field (0 = nullable, 1 = not null)
                            row.try_get::<i32, _>("notnull")
                                .map(|v| if v == 0 { "YES" } else { "NO" }.to_string())
                        })
                        .unwrap_or_else(|_| "YES".to_string()),
                    column_default: row
                        .try_get("column_default")
                        .or_else(|_| row.try_get("dflt_value"))
                        .ok(),
                })
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;

        // Human-readable display
        let display = format!(
            "📋 Table Schema: {}.{}\n\n\
             Columns: {}\n\
             {}",
            schema,
            args.table,
            columns.len(),
            columns.iter()
                .take(5)
                .map(|c| format!("  • {} ({}{})", 
                    c.column_name, 
                    c.data_type,
                    if c.is_nullable == "NO" { ", NOT NULL" } else { "" }
                ))
                .collect::<Vec<_>>()
                .join("\n")
        );
        
        // Convert TableColumn to ColumnInfo
        let column_info: Vec<ColumnInfo> = columns.iter()
            .map(|c| ColumnInfo {
                name: c.column_name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.is_nullable != "NO",
                default_value: c.column_default.clone(),
                is_primary_key: false, // TableColumn doesn't track this
            })
            .collect();
        
        // Create typed output
        let output = GetTableSchemaOutput {
            schema: schema.clone(),
            table: args.table.clone(),
            columns: column_info,
            column_count: columns.len(),
        };
        
        Ok(ToolResponse::new(display, output))
    }
}
