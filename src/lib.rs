//! S3 object storage plugin for VoidB

mod agent_session;
mod capabilities;
mod cli_plugin;
pub mod config;
pub mod s3_ops;
pub mod service;
pub mod sync_ops;
mod transfer_contract;
mod tui;
pub mod types;

pub use agent_session::S3AgentSessionFactory;
pub use capabilities::{invoke_s3_capability, s3_capabilities};
pub use cli_plugin::create_s3_cli_plugin;
pub use transfer_contract::s3_transfer_contract;

/// Test an S3 connection from a ConnectionConfig
pub async fn test_connection(
    conn: &voidb_core::connection::ConnectionConfig,
) -> anyhow::Result<String> {
    let config: config::S3Config = conn
        .plugin_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing plugin_config"))
        .and_then(|v| serde_json::from_value(v.clone()).map_err(Into::into))?;

    let buckets = s3_ops::list_buckets(&config).await?;

    Ok(format!(
        "S3 connection successful: {} ({} buckets accessible)",
        config.provider.label(),
        buckets.len()
    ))
}
