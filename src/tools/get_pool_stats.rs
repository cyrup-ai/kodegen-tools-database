//! GetPoolStats tool - Exposes connection pool health metrics

use crate::DatabaseType;
use kodegen_mcp_tool::{Tool, ToolExecutionContext, error::McpError};
use kodegen_mcp_schema::database::{GetPoolStatsArgs, GetPoolStatsPromptArgs};
use rmcp::model::{Content, PromptArgument, PromptMessage, PromptMessageContent, PromptMessageRole};
use serde_json::json;
use sqlx::AnyPool;
use std::sync::Arc;

#[derive(Clone)]
pub struct GetPoolStatsTool {
    pool: Arc<AnyPool>,
    db_type: DatabaseType,
}

impl GetPoolStatsTool {
    pub fn new(pool: Arc<AnyPool>, connection_url: &str) -> Result<Self, McpError> {
        let db_type = DatabaseType::from_url(connection_url)
            .map_err(|e| anyhow::anyhow!("Failed to determine database type: {}", e))?;
        Ok(Self { pool, db_type })
    }
}

impl Tool for GetPoolStatsTool {
    type Args = GetPoolStatsArgs;
    type PromptArgs = GetPoolStatsPromptArgs;

    fn name() -> &'static str {
        kodegen_mcp_schema::database::DB_POOL_STATS
    }

    fn description() -> &'static str {
        "Get connection pool health metrics including active connections, \
         idle connections, and pool configuration. Use this to diagnose \
         connection pool exhaustion or monitor database connection health."
    }

    fn read_only() -> bool {
        true // Read-only operation
    }

    async fn execute(&self, _args: Self::Args, _ctx: ToolExecutionContext) -> Result<Vec<Content>, McpError> {
        // Get pool metrics
        let size = self.pool.size();
        let num_idle = self.pool.num_idle();
        let num_active = size.saturating_sub(num_idle as u32);

        // Get pool options
        let options = self.pool.options();
        
        // Calculate health metrics
        let max_connections = options.get_max_connections();
        let health_status = if num_active == max_connections {
            "EXHAUSTED"
        } else if num_idle == 0 {
            "BUSY"
        } else {
            "HEALTHY"
        };
        let utilization_pct = (num_active as f64 / max_connections as f64 * 100.0).round() as u32;

        let mut contents = Vec::new();
        
        // Human-readable summary
        let summary = format!(
            "🔌 Connection Pool Health\n\n\
             Status: {}\n\
             Utilization: {}%\n\
             Active: {}/{}\n\
             Idle: {}",
            health_status,
            utilization_pct,
            num_active,
            max_connections,
            num_idle
        );
        contents.push(Content::text(summary));
        
        // JSON metadata
        let metadata = json!({
            "database_type": format!("{:?}", self.db_type),
            "connections": {
                "total": size,
                "active": num_active,
                "idle": num_idle,
            },
            "configuration": {
                "max_connections": max_connections,
                "min_connections": options.get_min_connections(),
                "acquire_timeout_secs": options.get_acquire_timeout().as_secs(),
                "idle_timeout_secs": options.get_idle_timeout().map(|d| d.as_secs()),
                "max_lifetime_secs": options.get_max_lifetime().map(|d| d.as_secs()),
                "test_before_acquire": options.get_test_before_acquire(),
            },
            "health": {
                "status": health_status,
                "utilization_pct": utilization_pct,
            }
        });
        let json_str = serde_json::to_string_pretty(&metadata)
            .unwrap_or_else(|_| "{}".to_string());
        contents.push(Content::text(json_str));
        
        Ok(contents)
    }

    fn prompt_arguments() -> Vec<PromptArgument> {
        vec![PromptArgument {
            name: "focus_area".to_string(),
            title: None,
            description: Some(
                "Optional focus area for connection pool monitoring scenarios: \
                 'exhausted_pools' (diagnosing connection exhaustion), \
                 'health_monitoring' (proactive monitoring strategies), or \
                 'configuration_tuning' (adjusting pool parameters)".to_string()
            ),
            required: Some(false),
        }]
    }

    async fn prompt(&self, _args: Self::PromptArgs) -> Result<Vec<PromptMessage>, McpError> {
        Ok(vec![
            PromptMessage {
                role: PromptMessageRole::User,
                content: PromptMessageContent::text(
                    "How do I effectively monitor and diagnose database connection pool health? \
                     When should I use get_pool_stats, and what do the metrics tell me?",
                ),
            },
            PromptMessage {
                role: PromptMessageRole::Assistant,
                content: PromptMessageContent::text(
                    "# Database Connection Pool Health Monitoring with get_pool_stats\n\n\
                     ## When to Use This Tool\n\n\
                     Use get_pool_stats to proactively monitor connection pool health:\n\
                     • **Early Detection**: Identify connection pool issues before they cause failures\n\
                     • **Production Monitoring**: Track database health in production environments\n\
                     • **Performance Debugging**: Investigate slow query scenarios and connection bottlenecks\n\
                     • **Capacity Planning**: Understand connection usage patterns over time\n\
                     • **Post-Deployment**: Verify connection pool configuration after changes\n\n\n\
                     ## Practical Usage\n\n\
                     Basic usage (no arguments required):\n\
                     ```\n\
                     get_pool_stats({})\n\
                     ```\n\n\
                     Example JSON output structure:\n\
                     ```json\n\
                     {\n\
                       \"database_type\": \"PostgreSQL\",\n\
                       \"connections\": {\n\
                         \"total\": 8,\n\
                         \"active\": 5,\n\
                         \"idle\": 3\n\
                       },\n\
                       \"configuration\": {\n\
                         \"max_connections\": 10,\n\
                         \"min_connections\": 2,\n\
                         \"acquire_timeout_secs\": 30,\n\
                         \"idle_timeout_secs\": 600,\n\
                         \"max_lifetime_secs\": 1800,\n\
                         \"test_before_acquire\": false\n\
                       },\n\
                       \"health\": {\n\
                         \"status\": \"HEALTHY\",\n\
                         \"utilization_pct\": 50\n\
                       }\n\
                     }\n\
                     ```\n\n\n\
                     ## Health Status Interpretation\n\n\
                     The tool reports three distinct health states:\n\n\
                     **HEALTHY**: Optimal state with available capacity\n\
                     • Utilization < max_connections\n\
                     • Idle connections available for immediate reuse\n\
                     • New queries execute without waiting\n\
                     • Action: Continue normal monitoring\n\n\
                     **BUSY**: High utilization but not yet critical\n\
                     • No idle connections currently available\n\
                     • Pool not yet at max_connections limit\n\
                     • New queries may wait briefly for connection release\n\
                     • Action: Monitor closely; consider increasing max_connections if sustained\n\n\
                     **EXHAUSTED**: Critical state requiring immediate attention\n\
                     • All connections actively in use (active == max_connections)\n\
                     • New queries will wait up to acquire_timeout_secs or fail\n\
                     • Risk of connection timeout errors and query failures\n\
                     • Action: Increase max_connections, investigate connection leaks, or scale database\n\n\n\
                     ## Key Metrics Explained\n\n\
                     **connections.total**: Current number of connections in the pool\n\
                     • Fluctuates between min_connections and max_connections\n\
                     • Pool grows on demand up to maximum limit\n\n\
                     **connections.active**: Connections currently executing queries\n\
                     • High active count indicates heavy database load\n\
                     • Compare against max_connections to gauge headroom\n\n\
                     **connections.idle**: Connections available for immediate reuse\n\
                     • Should be > 0 for healthy pool (except under peak load)\n\
                     • Zero idle suggests pool is at capacity or near it\n\n\
                     **health.utilization_pct**: Percentage of pool capacity in use\n\
                     • Formula: (active / max_connections) × 100\n\
                     • < 70%: Healthy headroom\n\
                     • 70-90%: Monitor closely\n\
                     • > 90%: Consider scaling\n\n\
                     **configuration.max_connections**: Upper limit of pool size\n\
                     • Hard cap on concurrent connections\n\
                     • Must be ≤ database server's max_connections setting\n\n\
                     **configuration.acquire_timeout_secs**: Wait time before query fails\n\
                     • How long queries wait for available connection\n\
                     • Timeout errors occur if exceeded during EXHAUSTED state\n\n\
                     **configuration.idle_timeout_secs**: Time before idle connections close\n\
                     • Conserves database resources during low traffic\n\
                     • May cause brief latency spikes if all connections expire\n\n\
                     **configuration.max_lifetime_secs**: Maximum connection age\n\
                     • Connections are recycled after this duration\n\
                     • Prevents stale connection issues\n\n\n\
                     ## Common Scenarios & Solutions\n\n\
                     **Scenario A: Frequent EXHAUSTED Status**\n\
                     • Symptom: status: \"EXHAUSTED\" appears regularly\n\
                     • Root Cause: max_connections too low for workload\n\
                     • Solution: Increase max_connections (ensure database server supports it)\n\
                     • Alternative: Implement connection pooling at application layer, scale database\n\n\
                     **Scenario B: Low Idle Count, High Active Count**\n\
                     • Symptom: idle near 0, active consistently high\n\
                     • Root Cause: Possible connection leak (connections not released)\n\
                     • Solution: Audit application code for unclosed connections/transactions\n\
                     • Investigation: Check for long-running queries, deadlocks\n\n\
                     **Scenario C: Intermittent BUSY Status**\n\
                     • Symptom: status alternates between HEALTHY and BUSY\n\
                     • Root Cause: Traffic spikes or uneven load distribution\n\
                     • Solution: Implement load balancing, optimize query performance\n\
                     • Consider: Read replicas for read-heavy workloads\n\n\
                     **Scenario D: Utilization Trending Upward**\n\
                     • Symptom: utilization_pct increases over days/weeks\n\
                     • Root Cause: Growing user base or data volume\n\
                     • Solution: Trend analysis for capacity planning, proactive scaling\n\n\n\
                     ## Database-Specific Considerations\n\n\
                     **PostgreSQL**:\n\
                     • max_connections is a system-wide server limit (default: 100)\n\
                     • Each connection consumes ~10MB RAM\n\
                     • Pool's max_connections must be ≤ server's max_connections\n\
                     • Consider pgBouncer for connection pooling at server level\n\n\
                     **MySQL/MariaDB**:\n\
                     • max_connections varies by version (default: 151 in MySQL 8.0)\n\
                     • Each connection uses ~256KB-1MB RAM\n\
                     • InnoDB has separate thread pool configuration\n\
                     • Consider ProxySQL for enterprise connection pooling\n\n\
                     **SQLite**:\n\
                     • Connections are lightweight (single-file database)\n\
                     • Serialization lock means only one write at a time\n\
                     • Connection pool size less critical than other databases\n\
                     • Focus on write concurrency rather than connection count\n\n\
                     **SQL Server**:\n\
                     • Connection pooling at driver level (ADO.NET, ODBC)\n\
                     • Server has sp_configure 'user connections' setting\n\
                     • Dynamic allocation by default\n\
                     • Monitor with sys.dm_exec_sessions DMV\n\n\n\
                     ## Best Practices\n\n\
                     1. **Monitor Periodically**: Track pool stats over time, establish baselines\n\
                     2. **Set Up Alerts**: Configure alerts for EXHAUSTED state or high utilization (>80%)\n\
                     3. **Match Workload**: Choose connection pooling library suited to your access patterns\n\
                     4. **Tune Based on Load**: Set max_connections based on 95th percentile load, not peak\n\
                     5. **Separate Pools**: Consider separate pools for read vs write operations\n\
                     6. **Implement Timeouts**: Always handle connection timeout errors gracefully in code\n\
                     7. **Test Configuration**: Load test after changing pool parameters\n\
                     8. **Document Baselines**: Record normal operating ranges for each metric\n\n\n\
                     ## Common Gotchas\n\n\
                     • **Configuration vs Server Limits**: max_connections setting only controls the pool, \
                       not the database server's maximum. Ensure pool limit ≤ server limit.\n\n\
                     • **Idle Timeout Side Effects**: Aggressive idle_timeout can close all connections \
                       during low traffic, causing latency spikes when traffic resumes.\n\n\
                     • **Test Before Acquire Overhead**: Enabling test_before_acquire adds latency \
                       (validation query per connection). Only enable if detecting stale connections.\n\n\
                     • **Pool Statistics Reset**: Pool stats reset on application/server restart. \
                       Historical trends require external monitoring/logging.\n\n\
                     • **Connection Leak Masking**: Large max_connections can mask connection leaks. \
                       Monitor active count trends rather than just increasing limits.\n\n\
                     • **Thundering Herd**: If all connections hit max_lifetime simultaneously, \
                       sudden reconnection storm can occur. Use jittered lifetimes if supported.\n\n\
                     • **Read Replica Confusion**: Pool stats show only the specific connection pool \
                       being queried, not overall database load across replicas.",
                ),
            },
        ])
    }
}
