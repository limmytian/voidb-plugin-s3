//! Shared types for S3 operations

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use chrono::{DateTime, Utc};
use voidb_core::AgentTransferEvent;

/// One bucket returned by provider-aware discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3BucketEntry {
    pub name: String,
    pub creation_date: Option<String>,
}

/// One native S3 ListObjects page.
#[derive(Debug, Clone)]
pub struct S3ListPage {
    pub entries: Vec<S3Entry>,
    pub continuation_token: Option<String>,
    pub next_continuation_token: Option<String>,
    pub truncated: bool,
}

/// A remote S3 entry (object or prefix)
#[derive(Debug, Clone)]
pub struct S3Entry {
    /// Full object key
    pub key: String,
    /// Display name (last component of key)
    pub display_name: String,
    /// Whether this is an object or prefix (virtual directory)
    pub entry_type: S3EntryType,
    /// Object size in bytes (0 for prefixes)
    pub size: u64,
    /// Last modified time (ISO 8601 string)
    pub last_modified: Option<String>,
    /// Storage class (STANDARD, GLACIER, etc.)
    pub storage_class: Option<String>,
    /// ETag header value
    pub etag: Option<String>,
}

/// Type of S3 entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3EntryType {
    /// An actual S3 object
    Object,
    /// A common prefix (virtual directory)
    Prefix,
}

/// Events sent from the background worker back to the UI
pub enum S3Event {
    /// Shared redacted transfer lifecycle for CLI/TUI parity.
    TransferLifecycle(Box<AgentTransferEvent>),
    /// Object listing completed
    ObjectsListed {
        prefix: String,
        entries: Vec<S3Entry>,
    },
    /// Bucket listing completed
    BucketsListed(Vec<String>),
    /// File download progress
    DownloadProgress {
        key: String,
        transferred: u64,
        total: u64,
    },
    /// File download completed
    DownloadComplete { key: String, local: String },
    /// File upload progress
    UploadProgress {
        local: String,
        transferred: u64,
        total: u64,
    },
    /// File upload completed
    UploadComplete { local: String, key: String },
    /// A mutation operation completed (with message)
    OperationComplete(String),
    /// An error occurred
    Error(String),
    /// Transfer was cancelled
    TransferCancelled,
    /// Sync plan computed (dry-run result)
    SyncPlanReady(SyncPlan),
    /// Sync progress update
    SyncProgress(SyncProgress),
    /// Sync completed
    SyncComplete {
        downloaded: usize,
        uploaded: usize,
        deleted: usize,
    },
}

impl S3Event {
    /// Wrap a shared transfer event without inflating every service event.
    pub fn transfer_lifecycle(event: AgentTransferEvent) -> Self {
        Self::TransferLifecycle(Box::new(event))
    }
}

/// Tracks progress of an active file transfer
pub struct TransferProgress {
    /// Display filename
    pub filename: String,
    /// Bytes transferred so far
    pub transferred: u64,
    /// Total file size
    pub total: u64,
    /// When the transfer started
    pub started_at: Instant,
    /// Whether this is a download (true) or upload (false)
    pub is_download: bool,
}

impl TransferProgress {
    /// Calculate transfer speed in bytes per second
    pub fn speed_bps(&self) -> f64 {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.transferred as f64 / elapsed
        } else {
            0.0
        }
    }

    /// Calculate estimated time remaining in seconds
    pub fn eta_secs(&self) -> Option<f64> {
        let speed = self.speed_bps();
        if speed > 0.0 && self.total > self.transferred {
            Some((self.total - self.transferred) as f64 / speed)
        } else {
            None
        }
    }

    /// Progress as a fraction 0.0..1.0
    pub fn fraction(&self) -> f64 {
        if self.total > 0 {
            (self.transferred as f64 / self.total as f64).min(1.0)
        } else {
            0.0
        }
    }
}

// --- Sync types ---

/// Sync direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Remote -> Local
    Pull,
    /// Local -> Remote
    Push,
    /// Bidirectional (newer wins)
    Sync,
}

impl SyncMode {
    pub fn label(&self) -> &'static str {
        match self {
            SyncMode::Pull => "Pull (Remote \u{2192} Local)",
            SyncMode::Push => "Push (Local \u{2192} Remote)",
            SyncMode::Sync => "Sync (Bidirectional)",
        }
    }

    pub fn next(&self) -> SyncMode {
        match self {
            SyncMode::Pull => SyncMode::Push,
            SyncMode::Push => SyncMode::Sync,
            SyncMode::Sync => SyncMode::Pull,
        }
    }
}

/// Options controlling sync behavior
#[derive(Debug, Clone)]
pub struct SyncOptions {
    pub mode: SyncMode,
    /// Remove files not present in source
    pub delete_extra: bool,
    /// Preview only, no changes
    pub dry_run: bool,
    /// Glob patterns to exclude
    pub exclude: Vec<String>,
}

/// A unified entry for diff comparison (remote or local)
#[derive(Debug, Clone)]
pub struct SyncEntry {
    /// Relative path from sync root (forward slashes)
    pub rel_path: String,
    pub is_dir: bool,
    pub size: u64,
    pub mtime: Option<DateTime<Utc>>,
}

/// What kind of change is needed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncAction {
    Download,
    Upload,
    DeleteLocal,
    DeleteRemote,
    Conflict,
}

impl SyncAction {
    pub fn icon(&self) -> &'static str {
        match self {
            SyncAction::Download => "\u{2193}",
            SyncAction::Upload => "\u{2191}",
            SyncAction::DeleteLocal => "\u{2715}",
            SyncAction::DeleteRemote => "\u{2715}",
            SyncAction::Conflict => "!",
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            SyncAction::Download => "NEW",
            SyncAction::Upload => "NEW",
            SyncAction::DeleteLocal => "DEL",
            SyncAction::DeleteRemote => "DEL",
            SyncAction::Conflict => "CONFLICT",
        }
    }
}

/// A single change in the sync plan
#[derive(Debug, Clone)]
pub struct SyncChange {
    pub rel_path: String,
    pub action: SyncAction,
    /// Bytes to transfer (0 for deletes)
    pub size: u64,
    pub is_dir: bool,
}

/// The complete diff result
pub struct SyncPlan {
    pub changes: Vec<SyncChange>,
    pub total_transfer_bytes: u64,
}

/// Counts returned after sync execution
pub struct SyncResult {
    pub downloaded: usize,
    pub uploaded: usize,
    pub deleted: usize,
}

/// Progress report during sync execution
pub struct SyncProgress {
    pub total_changes: usize,
    pub completed: usize,
    pub current_file: String,
    pub bytes_transferred: u64,
    pub total_bytes: u64,
}

/// Context needed by the sync worker to send progress and check cancellation
pub struct SyncContext {
    pub event_tx: tokio::sync::mpsc::UnboundedSender<S3Event>,
    pub cancel_flag: Arc<AtomicBool>,
}
