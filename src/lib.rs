//! Database query tools for kodegen MCP server
//!
//! Provides tools for executing SQL queries and exploring database schemas
//! across PostgreSQL, MySQL, MariaDB, SQLite, and SQL Server.

pub mod error;
pub mod types;

// Utilities (implemented in later tasks)
pub mod dsn;
pub mod readonly;
pub mod schema_queries;
pub mod sql_limiter;
pub mod sql_parser;
pub mod ssh_tunnel;
pub mod validate;
pub mod connection;

// Tools (implemented in later tasks)
pub mod tools;

// Re-export secrecy types for consumers
pub use secrecy::{ExposeSecret, SecretString};

// Re-exports
pub use dsn::{
    DSNInfo, detect_database_type, extract_database, extract_host, extract_port, parse_dsn,
    rewrite_dsn_for_tunnel, validate_dsn,
};
pub use error::DatabaseError;
pub use readonly::validate_readonly_sql;
pub use schema_queries::{
    get_default_schema, get_indexes_query, get_schemas_query, get_stored_procedures_query,
    get_table_schema_query, get_tables_query,
};
pub use sql_limiter::apply_row_limit;
pub use sql_parser::{extract_first_keyword, split_sql_statements, strip_comments};
pub use ssh_tunnel::{SSHAuth, SSHConfig, SSHTunnel, TunnelConfig, establish_tunnel};
pub use connection::{DatabaseConnection, setup_database_pool, warmup_pool};
pub use tools::ExecuteSQLTool;
pub use types::{
    DatabaseType, ExecuteOptions, SQLResult, StoredProcedure, TableColumn, TableIndex,
};
pub use validate::validate_sqlite_identifier;

/// Start the HTTP server programmatically
///
/// Returns a ServerHandle for graceful shutdown control.
/// This function is non-blocking - the server runs in background tasks.
///
/// # Arguments
/// * `addr` - Socket address to bind to (e.g., "127.0.0.1:30446")
/// * `tls_cert` - Optional path to TLS certificate file
/// * `tls_key` - Optional path to TLS private key file
///
/// # Returns
/// ServerHandle for graceful shutdown, or error if startup fails
///
/// # Environment Variables
/// * `DATABASE_DSN` - Database connection string (defaults to `sqlite::memory:` if not set)
/// * `SSH_HOST`, `SSH_PORT`, `SSH_USER`, `SSH_AUTH_TYPE` - Optional SSH tunnel config
pub async fn start_server(
    addr: std::net::SocketAddr,
    tls_cert: Option<std::path::PathBuf>,
    tls_key: Option<std::path::PathBuf>,
) -> anyhow::Result<kodegen_server_http::ServerHandle> {
    use kodegen_server_http::{create_http_server, Managers, RouterSet, ShutdownHook, register_tool};
    use kodegen_config_manager::ConfigManager;
    use kodegen_utils::usage_tracker::UsageTracker;
    use rmcp::handler::server::router::{prompt::PromptRouter, tool::ToolRouter};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    let tls_config = match (tls_cert, tls_key) {
        (Some(cert), Some(key)) => Some((cert, key)),
        _ => None,
    };

    let shutdown_timeout = Duration::from_secs(30);
    let session_keep_alive = Duration::ZERO;

    create_http_server("database", addr, tls_config, shutdown_timeout, session_keep_alive, |config: &ConfigManager, _tracker: &UsageTracker| {
        let config = config.clone();
        Box::pin(async move {
            let mut tool_router = ToolRouter::new();
            let mut prompt_router = PromptRouter::new();
            let managers = Managers::new();

            // Get DATABASE_DSN from environment (defaults to in-memory SQLite)
            let dsn = std::env::var("DATABASE_DSN")
                .unwrap_or_else(|_| {
                    log::info!("DATABASE_DSN not set, defaulting to sqlite::memory:");
                    "sqlite::memory:".to_string()
                });

            // Parse optional SSH tunnel configuration
            let ssh_config = parse_ssh_config_from_env()?;

            // Setup database connection pool (with optional SSH tunnel)
            let db_connection = crate::setup_database_pool(&config, &dsn, ssh_config).await?;

            // Register SSH tunnel for graceful shutdown if present
            if db_connection.tunnel.is_some() {
                let tunnel_guard = Arc::new(Mutex::new(db_connection.tunnel));

                // Implement shutdown hook
                struct TunnelGuard(Arc<Mutex<Option<crate::SSHTunnel>>>);
                impl ShutdownHook for TunnelGuard {
                    fn shutdown(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send + '_>> {
                        let guard = self.0.clone();
                        Box::pin(async move {
                            let mut tunnel_opt = guard.lock().await;
                            if let Some(tunnel) = tunnel_opt.take() {
                                log::info!("Closing SSH tunnel for database connection");
                                tunnel.close().await;
                            }
                            Ok(())
                        })
                    }
                }

                managers.register(TunnelGuard(tunnel_guard)).await;
            }

            // Register all 7 database tools
            use crate::tools::*;

            let pool = db_connection.pool;
            let connection_url = &db_connection.connection_url;

            (tool_router, prompt_router) = register_tool(
                tool_router,
                prompt_router,
                ExecuteSQLTool::new(pool.clone(), config.clone(), connection_url)?,
            );

            (tool_router, prompt_router) = register_tool(
                tool_router,
                prompt_router,
                ListSchemasTool::new(pool.clone(), connection_url, config.clone())?,
            );

            (tool_router, prompt_router) = register_tool(
                tool_router,
                prompt_router,
                ListTablesTool::new(pool.clone(), connection_url, config.clone())?,
            );

            (tool_router, prompt_router) = register_tool(
                tool_router,
                prompt_router,
                GetTableSchemaTool::new(pool.clone(), connection_url, Arc::new(config.clone()))?,
            );

            (tool_router, prompt_router) = register_tool(
                tool_router,
                prompt_router,
                GetTableIndexesTool::new(pool.clone(), connection_url, Arc::new(config.clone()))?,
            );

            (tool_router, prompt_router) = register_tool(
                tool_router,
                prompt_router,
                GetStoredProceduresTool::new(pool.clone(), connection_url, Arc::new(config.clone()))?,
            );

            (tool_router, prompt_router) = register_tool(
                tool_router,
                prompt_router,
                GetPoolStatsTool::new(pool.clone(), connection_url)?,
            );

            Ok(RouterSet::new(tool_router, prompt_router, managers))
        })
    }).await
}

/// Parse SSH configuration from environment variables
///
/// Required variables (all must be set):
/// - SSH_HOST: SSH server hostname
/// - SSH_PORT: SSH server port
/// - SSH_USER: SSH username
/// - SSH_AUTH_TYPE: "password" or "key"
///
/// For password auth:
/// - SSH_PASSWORD: Password
///
/// For key auth:
/// - SSH_KEY_PATH: Path to private key
/// - SSH_KEY_PASSPHRASE: Optional key passphrase
///
/// Target configuration:
/// - SSH_TARGET_HOST: Database host from SSH perspective
/// - SSH_TARGET_PORT: Database port
fn parse_ssh_config_from_env() -> anyhow::Result<Option<(
    crate::SSHConfig,
    crate::TunnelConfig,
)>> {
    use anyhow::Context;
    use std::path::PathBuf;

    // Check if SSH is configured
    let ssh_host = match std::env::var("SSH_HOST") {
        Ok(h) => h,
        Err(_) => return Ok(None), // No SSH configured
    };

    // All other SSH vars are required if SSH_HOST is set
    let ssh_port: u16 = std::env::var("SSH_PORT")
        .context("SSH_PORT required when SSH_HOST is set")?
        .parse()
        .context("SSH_PORT must be valid port number")?;

    let ssh_user = std::env::var("SSH_USER")
        .context("SSH_USER required when SSH_HOST is set")?;

    let auth_type = std::env::var("SSH_AUTH_TYPE")
        .context("SSH_AUTH_TYPE required (must be 'password' or 'key')")?;

    let auth = match auth_type.as_str() {
        "password" => {
            let password = std::env::var("SSH_PASSWORD")
                .context("SSH_PASSWORD required when SSH_AUTH_TYPE=password")?;
            crate::SSHAuth::Password(password)
        }
        "key" => {
            let key_path = std::env::var("SSH_KEY_PATH")
                .context("SSH_KEY_PATH required when SSH_AUTH_TYPE=key")?;
            let passphrase = std::env::var("SSH_KEY_PASSPHRASE").ok();
            crate::SSHAuth::Key {
                path: PathBuf::from(key_path),
                passphrase,
            }
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Invalid SSH_AUTH_TYPE '{}': must be 'password' or 'key'",
                auth_type
            ));
        }
    };

    let target_host = std::env::var("SSH_TARGET_HOST")
        .context("SSH_TARGET_HOST required for tunnel target")?;
    let target_port: u16 = std::env::var("SSH_TARGET_PORT")
        .context("SSH_TARGET_PORT required for tunnel target")?
        .parse()
        .context("SSH_TARGET_PORT must be valid port number")?;

    let ssh_config = crate::SSHConfig {
        host: ssh_host,
        port: ssh_port,
        username: ssh_user,
        auth,
    };

    let tunnel_config = crate::TunnelConfig {
        target_host,
        target_port,
    };

    Ok(Some((ssh_config, tunnel_config)))
}
