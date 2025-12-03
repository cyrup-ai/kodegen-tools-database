//! Get table schema (column information) tool

use crate::error::DatabaseError;
use crate::schema_queries::get_table_schema_query;
use crate::tools::helpers::resolve_schema_default;
use crate::tools::timeout::execute_with_timeout;
use crate::types::{DatabaseType, TableColumn};
use kodegen_mcp_tool::{Tool, ToolExecutionContext, ToolResponse, error::McpError};
use kodegen_mcp_schema::ToolArgs;
use kodegen_mcp_schema::database::{GetTableSchemaArgs, GetTableSchemaPromptArgs, GetTableSchemaOutput, ColumnInfo};
use kodegen_config_manager::ConfigManager;
use rmcp::model::{PromptArgument, PromptMessage, PromptMessageContent, PromptMessageRole};

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
    type PromptArgs = GetTableSchemaPromptArgs;

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

    fn prompt_arguments() -> Vec<PromptArgument> {
        vec![
            PromptArgument {
                name: "database_type".to_string(),
                title: Some("Database Type".to_string()),
                description: Some(
                    "Focus teaching examples on a specific database system: \
                    'postgres' (PostgreSQL), 'mysql' (MySQL/MariaDB), 'sqlite' (SQLite), \
                    or 'sql_server' (SQL Server). Helps see relevant schema terminology and syntax."
                        .to_string(),
                ),
                required: Some(false),
            },
            PromptArgument {
                name: "focus_area".to_string(),
                title: Some("Focus Area".to_string()),
                description: Some(
                    "Customize teaching to emphasize specific aspects: 'constraints' (PK, FK, CHECK), \
                    'indexes' (performance optimization), 'data_types' (type systems), \
                    'defaults' (default values), 'nullability' (NULL handling), \
                    or 'workflow' (step-by-step process)."
                        .to_string(),
                ),
                required: Some(false),
            },
        ]
    }

    async fn prompt(&self, args: Self::PromptArgs) -> Result<Vec<PromptMessage>, McpError> {
        let db_type = args.database_type.as_deref().unwrap_or("generic").to_lowercase();
        let focus = args.focus_area.as_deref().unwrap_or("workflow").to_lowercase();

        // Build base conversation
        let mut messages = vec![
            PromptMessage {
                role: PromptMessageRole::User,
                content: PromptMessageContent::Text {
                    text: "When should I use get_table_schema?".to_string(),
                },
            },
        ];

        // Build focused assistant response based on arguments
        let assistant_text = build_adaptive_response(&db_type, &focus);
        messages.push(PromptMessage {
            role: PromptMessageRole::Assistant,
            content: PromptMessageContent::Text {
                text: assistant_text,
            },
        });

        // Add database-specific example interaction if requested
        if db_type != "generic" {
            messages.push(PromptMessage {
                role: PromptMessageRole::User,
                content: PromptMessageContent::Text {
                    text: format!("Show me a {} example", db_type),
                },
            });
            messages.push(PromptMessage {
                role: PromptMessageRole::Assistant,
                content: PromptMessageContent::Text {
                    text: build_database_example(&db_type, &focus),
                },
            });
        }

        // Add focus-area-specific deep dive if not workflow
        if focus != "workflow" {
            messages.push(PromptMessage {
                role: PromptMessageRole::User,
                content: PromptMessageContent::Text {
                    text: format!("What should I look for in {} when inspecting a table?", focus),
                },
            });
            messages.push(PromptMessage {
                role: PromptMessageRole::Assistant,
                content: PromptMessageContent::Text {
                    text: build_focus_area_guidance(&focus),
                },
            });
        }

        Ok(messages)
    }
}

/// Build adaptive core response based on database type and focus area
fn build_adaptive_response(db_type: &str, _focus: &str) -> String {
    let base = "Use get_table_schema when you need to understand a table's structure \
                before writing queries. It returns column names, data types, nullability, \
                and default values.";

    let db_specific = match db_type {
        "postgres" | "postgresql" => {
            "In PostgreSQL, use get_table_schema to inspect:\n\
             • Column constraints (PRIMARY KEY, UNIQUE, NOT NULL)\n\
             • User-defined types (ENUM, DOMAIN, composite types)\n\
             • Serial/identity columns\n\
             • Check constraints and domain rules\n\
             Default schema is 'public' unless specified."
        }
        "mysql" | "mariadb" => {
            "In MySQL/MariaDB, use get_table_schema to inspect:\n\
             • Key information (PRIMARY, UNIQUE, INDEX)\n\
             • Column collation and character set\n\
             • Auto-increment fields\n\
             • Zerofill and unsigned attributes\n\
             Default schema is the current DATABASE() context."
        }
        "sqlite" => {
            "In SQLite, use get_table_schema to inspect:\n\
             • ROWID and INTEGER PRIMARY KEY behavior\n\
             • Table constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK)\n\
             • Default values and generated columns\n\
             • Type affinity (TEXT, NUMERIC, INTEGER, REAL, BLOB)\n\
             Default schema is 'main'. Note: SQLite has limited type system."
        }
        "sql_server" | "sqlserver" | "mssql" => {
            "In SQL Server, use get_table_schema to inspect:\n\
             • Identity columns and seed/increment values\n\
             • Computed columns and persisted flags\n\
             • Column collation settings\n\
             • Default constraints and check constraints\n\
             Default schema is 'dbo' unless specified."
        }
        _ => {
            "Workflow: list_schemas() → list_tables(schema) → get_table_schema(table, schema) → execute_sql()\n\
             Schema parameter is optional and defaults to:\n\
             • PostgreSQL: 'public'\n\
             • MySQL/MariaDB: current DATABASE()\n\
             • SQLite: 'main'\n\
             • SQL Server: 'dbo'"
        }
    };

    format!("{}.\n\n{}", base, db_specific)
}

/// Build database-specific example with relevant schema concepts
fn build_database_example(db_type: &str, _focus: &str) -> String {
    match db_type {
        "postgres" | "postgresql" => {
            "Example: Inspecting a PostgreSQL 'users' table:\n\n\
             get_table_schema(table='users', schema='public')\n\n\
             Result shows:\n\
             • id: bigserial (auto-incrementing PRIMARY KEY)\n\
             • email: character varying(255) with UNIQUE constraint\n\
             • created_at: timestamp with default CURRENT_TIMESTAMP\n\
             • role: user_defined ENUM type (admin, user, guest)\n\
             • is_active: boolean NOT NULL DEFAULT true\n\n\
             Use this to understand sequence dependencies and type constraints before JOIN operations."
                .to_string()
        }
        "mysql" | "mariadb" => {
            "Example: Inspecting a MySQL 'users' table:\n\n\
             get_table_schema(table='users')\n\n\
             Result shows:\n\
             • id: bigint UNSIGNED AUTO_INCREMENT PRIMARY KEY\n\
             • email: varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci UNIQUE\n\
             • created_at: timestamp DEFAULT CURRENT_TIMESTAMP\n\
             • flags: tinyint(1) unsigned (0=false, 1=true pattern)\n\n\
             Use this to understand collation impact on string comparisons and AUTO_INCREMENT behavior."
                .to_string()
        }
        "sqlite" => {
            "Example: Inspecting a SQLite 'users' table:\n\n\
             get_table_schema(table='users')\n\n\
             Result shows:\n\
             • id: INTEGER PRIMARY KEY (maps to internal ROWID, single per table)\n\
             • email: TEXT NOT NULL\n\
             • age: INTEGER (SQLite doesn't enforce range; use CHECK constraints)\n\
             • created_at: TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n\
             • metadata: BLOB (untyped storage)\n\n\
             SQLite has flexible typing; actual value type depends on what was inserted."
                .to_string()
        }
        "sql_server" | "sqlserver" | "mssql" => {
            "Example: Inspecting a SQL Server 'users' table:\n\n\
             get_table_schema(table='users', schema='dbo')\n\n\
             Result shows:\n\
             • id: bigint IDENTITY(1,1) PRIMARY KEY (similar to AUTO_INCREMENT)\n\
             • email: nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS UNIQUE\n\
             • created_at: datetime DEFAULT GETDATE()\n\
             • is_active: bit (SQL Server's boolean type, 0=false, 1=true)\n\n\
             Use this to understand IDENTITY ranges and collation-sensitive string operations."
                .to_string()
        }
        _ => "Use: get_table_schema(table='your_table', schema='optional_schema')\n\
               The tool adapts its output based on your database type and returns \
               column names, types, nullability, and defaults."
            .to_string(),
    }
}

/// Build guidance specific to a focus area
fn build_focus_area_guidance(focus: &str) -> String {
    match focus {
        "constraints" => {
            "When inspecting constraints, look for:\n\
             • PRIMARY KEY: Unique identifier for rows; only one per table\n\
             • UNIQUE: Ensures distinct values; multiple per table allowed\n\
             • FOREIGN KEY: References another table; ensures referential integrity\n\
             • CHECK: Validates data meets conditions (e.g., age > 0)\n\
             • NOT NULL: Column must have a value\n\
             • DEFAULT: Value used if none provided\n\n\
             Constraints impact: UPDATE/DELETE performance, data consistency, query writing patterns."
                .to_string()
        }
        "indexes" => {
            "When inspecting indexes, look for:\n\
             • PRIMARY KEY indexes (always created, fastest lookups)\n\
             • UNIQUE indexes (enforce distinctness, used for lookups)\n\
             • Composite indexes (multiple columns; order matters)\n\
             • Index type (B-tree, Hash, GIST, etc. - database dependent)\n\
             • Covering indexes (include non-key columns for efficiency)\n\n\
             Indexes impact: Query planning, WHERE clause performance, JOIN speed, INSERT/UPDATE cost."
                .to_string()
        }
        "data_types" => {
            "When inspecting data types, understand:\n\
             • Numeric: INTEGER (exact), DECIMAL (precision), FLOAT (approximate)\n\
             • String: VARCHAR (variable), CHAR (fixed), TEXT (large, unindexed)\n\
             • Date/Time: DATE, TIME, TIMESTAMP, INTERVAL\n\
             • Boolean: Different representations per database (BIT, BOOLEAN, etc.)\n\
             • Special: JSON, UUID, ENUM, ARRAY (database-specific)\n\n\
             Data type impact: Storage size, calculation precision, comparison semantics, indexability."
                .to_string()
        }
        "defaults" => {
            "When inspecting defaults, consider:\n\
             • Static defaults: Constant values (e.g., 0, 'ACTIVE')\n\
             • Function defaults: Database functions (e.g., CURRENT_TIMESTAMP, NEXT_VAL)\n\
             • Absence of defaults: Column requires explicit value on INSERT\n\
             • NULL defaults: Implicit when not specified for nullable columns\n\n\
             Defaults impact: INSERT statement requirements, data consistency, schema migrations."
                .to_string()
        }
        "nullability" => {
            "When inspecting nullability, understand:\n\
             • NOT NULL columns: Must provide value on every INSERT\n\
             • NULL columns: Can omit or explicitly set NULL (different semantics)\n\
             • NULL in JOINs: Three-valued logic (true, false, NULL)\n\
             • NULL in aggregates: COUNT(*) vs COUNT(column) - handles NULL differently\n\
             • NULL comparisons: Use IS NULL, not = NULL\n\n\
             Nullability impact: Query correctness, index usage, performance, data validation."
                .to_string()
        }
        _ => {
            "Standard workflow for using get_table_schema:\n\n\
             1. Start with: list_schemas()\n\
                To discover available schemas ('public', current database, 'dbo', etc.)\n\n\
             2. Find tables: list_tables(schema='chosen_schema')\n\
                To find specific tables you want to query\n\n\
             3. Inspect structure: get_table_schema(table='users', schema='public')\n\
                To understand columns before writing queries\n\n\
             4. Write queries: execute_sql('SELECT id, email FROM users WHERE is_active')\n\
                Armed with schema knowledge, write accurate, efficient queries\n\n\
             5. Iterate: Re-inspect if you encounter unexpected column names, types, or constraints"
                .to_string()
        }
    }
}
