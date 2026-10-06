//! S3 service commands.
//!
//! Commands sent from the UI layer to the S3 background service task.

use crate::types::SyncOptions;

/// Commands sent from the UI to the background worker
pub enum S3Command {
    /// Connect to S3 (initialize bucket handle)
    Connect {
        /// Bucket name to connect to (None = list buckets)
        bucket: Option<String>,
    },
    /// Disconnect from S3 (stop the worker loop)
    Disconnect,
    /// List objects at the given prefix
    ListObjects { prefix: String },
    /// List all buckets
    ListBuckets,
    /// Download an object to a local path
    DownloadObject { key: String, local: String },
    /// Upload a local file to a remote key
    UploadObject { local: String, key: String },
    /// Delete a remote object
    DeleteObject(String),
    /// Copy an object
    CopyObject { from: String, to: String },
    /// Move an object (copy + delete)
    MoveObject { from: String, to: String },
    /// Cancel all active transfers
    CancelTransfer,
    /// Compute or execute a sync plan
    SyncExecute {
        remote_prefix: String,
        local_path: String,
        options: SyncOptions,
    },
}
