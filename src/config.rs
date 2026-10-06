//! S3 connection configuration

use serde::{Deserialize, Serialize};

fn default_timeout() -> u64 {
    30
}

fn default_true() -> bool {
    true
}

/// S3 connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 service provider
    pub provider: S3Provider,

    /// Bucket name (optional, can be selected after connecting)
    pub bucket: Option<String>,

    /// Authentication method
    pub auth: S3Auth,

    /// Connection timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

/// S3 service provider
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum S3Provider {
    /// AWS S3
    Aws { region: String },
    /// MinIO
    Minio { endpoint: String },
    /// Cloudflare R2
    R2 { account_id: String },
    /// Custom S3-compatible service
    Custom {
        endpoint: String,
        region: String,
        #[serde(default = "default_true")]
        path_style: bool,
    },
}

/// S3 authentication method
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum S3Auth {
    /// Access Key + Secret Key
    AccessKey {
        access_key: String,
        secret_key: String,
    },
    /// Auto-detect from environment / AWS config
    Auto,
    /// Anonymous access (public buckets)
    Anonymous,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            provider: S3Provider::Aws {
                region: "us-east-1".to_string(),
            },
            bucket: None,
            auth: S3Auth::Auto,
            timeout: 30,
        }
    }
}

impl S3Provider {
    /// Human-readable label for display
    pub fn label(&self) -> &'static str {
        match self {
            S3Provider::Aws { .. } => "AWS S3",
            S3Provider::Minio { .. } => "MinIO",
            S3Provider::R2 { .. } => "Cloudflare R2",
            S3Provider::Custom { .. } => "Custom S3-Compatible",
        }
    }
}

impl S3Auth {
    /// Human-readable label for display
    pub fn label(&self) -> &'static str {
        match self {
            S3Auth::AccessKey { .. } => "Access Key",
            S3Auth::Auto => "Auto (env/config)",
            S3Auth::Anonymous => "Anonymous",
        }
    }
}
