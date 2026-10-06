//! S3 service layer.
//!
//! This module provides `S3Service`, the service facade for the S3 plugin.
//! It follows the RedisService / MySqlService convention:
//!
//! - Background tokio task processes commands asynchronously
//! - `send()` dispatches commands via unbounded mpsc channel (non-blocking)
//! - `poll_event()` drains events via `try_recv()` (non-blocking)
//! - Render notifications fire after every event emission
//!
//! # Connection Lifecycle
//!
//! The service starts by optionally connecting to a specific bucket. A
//! `Connect` command creates a bucket handle. If no bucket is specified,
//! it lists available buckets. On `Disconnect` (or sender drop), the
//! background loop exits.
//!
//! # Direct Mode
//!
//! For non-TUI consumers (CLI, MCP, tests), `S3Service::new_direct()` creates
//! the service without spawning a background task. Callers use the async
//! methods directly (e.g. `list_buckets()`, `list_objects()`, `download()`,
//! etc.) which await the result inline.

pub mod commands;
pub mod events;

pub use commands::S3Command;
pub use events::S3Event;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use tokio::sync::mpsc;

use crate::config::S3Config;
use crate::s3_ops;
use crate::sync_ops;
use crate::types::{
    S3BucketEntry, S3Entry, S3ListPage, SyncContext, SyncEntry, SyncOptions, SyncPlan, SyncResult,
};
use chrono::Utc;
use voidb_core::{AgentTransferEvent, AgentTransferOperation, AgentTransferPhase, TabManager};

// ---------------------------------------------------------------------------
// ServiceMode
// ---------------------------------------------------------------------------

/// Internal execution strategy for the service.
///
/// `Channel` is the TUI mode: a background task processes commands
/// asynchronously. `Direct` is the CLI/test mode: no background task is
/// spawned; callers await async methods directly.
enum ServiceMode {
    /// TUI mode — background task with mpsc channels.
    Channel {
        cmd_tx: mpsc::UnboundedSender<S3Command>,
        event_rx: mpsc::UnboundedReceiver<S3Event>,
        _task: tokio::task::JoinHandle<()>,
    },
    /// Direct mode — holds the S3 config; no background task.
    Direct {
        config: S3Config,
        cancel_flag: Arc<AtomicBool>,
    },
}

// ---------------------------------------------------------------------------
// S3Service
// ---------------------------------------------------------------------------

/// S3 service facade.
///
/// Owns the command sender and event receiver channels in TUI mode, or the
/// raw connection config in direct (CLI) mode. The background task runs on
/// the shared tokio runtime via `runtime.spawn()`.
///
/// # Send + Sync
///
/// `S3Service` is `Send` but NOT `Sync` (because `UnboundedReceiver`
/// is `!Sync`). Plugin structs must wrap it in `std::sync::Mutex` to
/// satisfy `Plugin: Send + Sync`. Since `Plugin::update(&mut self)` has
/// exclusive access, the Mutex is never contended.
pub struct S3Service {
    mode: ServiceMode,
    cancel_flag: Arc<AtomicBool>,
}

impl S3Service {
    /// Create a new S3Service with a background processing task (TUI mode).
    ///
    /// The service immediately attempts to connect. If `initial_bucket` is
    /// `Some`, it creates a bucket handle and lists root objects. If `None`,
    /// it lists available buckets.
    pub fn new(
        config: S3Config,
        initial_bucket: Option<String>,
        cancel_flag: Arc<AtomicBool>,
        tabs: Arc<dyn TabManager>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<S3Command>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<S3Event>();

        let task_cancel_flag = Arc::clone(&cancel_flag);
        let task = runtime.spawn(Self::background_task(
            cmd_rx,
            event_tx,
            tabs,
            config,
            initial_bucket,
            task_cancel_flag,
        ));

        Self {
            mode: ServiceMode::Channel {
                cmd_tx,
                event_rx,
                _task: task,
            },
            cancel_flag,
        }
    }

    /// Create a new S3Service in direct (CLI / test) mode.
    ///
    /// No background task is spawned. Use the async direct methods
    /// (`list_buckets`, `list_objects`, `download`, etc.) to perform
    /// operations inline inside an async context.
    pub fn new_direct(config: S3Config) -> Self {
        let cancel_flag = Arc::new(AtomicBool::new(false));
        Self {
            mode: ServiceMode::Direct {
                config,
                cancel_flag: Arc::clone(&cancel_flag),
            },
            cancel_flag,
        }
    }

    /// Send a command to the background service task (TUI/Channel mode only).
    ///
    /// This is non-blocking — safe to call from the synchronous
    /// `Plugin::update()` context. Does nothing in Direct mode.
    pub fn send(&self, cmd: S3Command) {
        match &cmd {
            S3Command::CancelTransfer => self.cancel_flag.store(true, Ordering::Release),
            S3Command::DownloadObject { .. }
            | S3Command::UploadObject { .. }
            | S3Command::SyncExecute { .. } => {
                self.cancel_flag.store(false, Ordering::Release);
            }
            _ => {}
        }
        if let ServiceMode::Channel { cmd_tx, .. } = &self.mode {
            let _ = cmd_tx.send(cmd);
        }
    }

    /// Poll for the next event from the service (TUI/Channel mode only).
    ///
    /// Returns `Some(event)` if available, `None` otherwise.
    /// Non-blocking, suitable for calling from `Plugin::update()`.
    /// Always returns `None` in Direct mode.
    pub fn poll_event(&mut self) -> Option<S3Event> {
        if let ServiceMode::Channel { event_rx, .. } = &mut self.mode {
            event_rx.try_recv().ok()
        } else {
            None
        }
    }

    // -----------------------------------------------------------------------
    // Direct async methods (used by CLI plugin)
    // -----------------------------------------------------------------------

    /// List all buckets accessible with the configured credentials.
    ///
    /// Available in both Channel and Direct modes; in Channel mode, this
    /// performs the call inline (bypasses the background task).
    pub async fn list_buckets(&self) -> Result<Vec<String>> {
        let config = self.config_ref()?;
        s3_ops::list_buckets(config).await
    }

    /// Discover accessible buckets with provider-returned metadata.
    pub async fn discover_buckets(&self) -> Result<Vec<S3BucketEntry>> {
        let config = self.config_ref()?;
        s3_ops::discover_buckets(config).await
    }

    /// Create a new S3 bucket.
    pub async fn create_bucket_op(&self, name: &str) -> Result<()> {
        let config = self.config_ref()?;
        s3_ops::create_bucket_op(config, name).await
    }

    /// Delete an empty S3 bucket.
    pub async fn delete_bucket_op(&self, name: &str) -> Result<()> {
        let config = self.config_ref()?;
        s3_ops::delete_bucket_op(config, name).await
    }

    /// List objects at `prefix` inside `bucket_name`.
    ///
    /// Returns entries with a `"/"` delimiter (one-level listing).
    pub async fn list_objects(&self, bucket_name: &str, prefix: &str) -> Result<Vec<S3Entry>> {
        let config = self.config_ref()?;
        let bucket = s3_ops::create_bucket(config, bucket_name)?;
        s3_ops::list_objects(&bucket, prefix, Some("/")).await
    }

    /// Fetch one native, bounded S3 object page.
    pub async fn list_objects_page(
        &self,
        bucket_name: &str,
        prefix: &str,
        delimiter: Option<&str>,
        continuation_token: Option<String>,
        max_keys: usize,
    ) -> Result<S3ListPage> {
        let config = self.config_ref()?;
        let bucket = s3_ops::create_bucket(config, bucket_name)?;
        s3_ops::list_objects_page(&bucket, prefix, delimiter, continuation_token, max_keys).await
    }

    /// Download `key` from `bucket_name` and return the raw bytes.
    pub async fn download(&self, bucket_name: &str, key: &str) -> Result<Vec<u8>> {
        let config = self.config_ref()?;
        let bucket = s3_ops::create_bucket(config, bucket_name)?;
        s3_ops::download(&bucket, key).await
    }

    /// Upload `data` to `key` inside `bucket_name`.
    pub async fn upload(&self, bucket_name: &str, key: &str, data: &[u8]) -> Result<()> {
        let config = self.config_ref()?;
        let bucket = s3_ops::create_bucket(config, bucket_name)?;
        s3_ops::upload(&bucket, key, data, None).await
    }

    /// Check whether an object already exists without treating target errors as
    /// absence.
    pub async fn object_exists(&self, bucket_name: &str, key: &str) -> Result<bool> {
        let config = self.config_ref()?;
        let bucket = s3_ops::create_bucket(config, bucket_name)?;
        s3_ops::object_exists(&bucket, key).await
    }

    /// Delete `key` from `bucket_name`.
    pub async fn delete_object(&self, bucket_name: &str, key: &str) -> Result<()> {
        let config = self.config_ref()?;
        let bucket = s3_ops::create_bucket(config, bucket_name)?;
        s3_ops::delete(&bucket, key).await
    }

    /// Copy `src_key` to `dst_key` within `bucket_name`.
    pub async fn copy_object(&self, bucket_name: &str, src_key: &str, dst_key: &str) -> Result<()> {
        let config = self.config_ref()?;
        let bucket = s3_ops::create_bucket(config, bucket_name)?;
        s3_ops::copy_object(&bucket, src_key, dst_key).await
    }

    /// Copy between buckets and verify the destination before returning.
    pub async fn copy_object_verified(
        &self,
        source_bucket: &str,
        source_key: &str,
        destination_bucket: &str,
        destination_key: &str,
        replace: bool,
    ) -> Result<S3Entry> {
        let config = self.config_ref()?;
        s3_ops::copy_object_verified(
            config,
            source_bucket,
            source_key,
            destination_bucket,
            destination_key,
            replace,
        )
        .await
    }

    /// Move by verified copy and a source identity recheck before deletion.
    pub async fn move_object_verified(
        &self,
        source_bucket: &str,
        source_key: &str,
        destination_bucket: &str,
        destination_key: &str,
        replace: bool,
    ) -> Result<S3Entry> {
        let config = self.config_ref()?;
        s3_ops::move_object_verified(
            config,
            source_bucket,
            source_key,
            destination_bucket,
            destination_key,
            replace,
        )
        .await
    }

    /// Get object metadata for `key` inside `bucket_name`.
    pub async fn get_object_info(&self, bucket_name: &str, key: &str) -> Result<S3Entry> {
        let config = self.config_ref()?;
        let bucket = s3_ops::create_bucket(config, bucket_name)?;
        s3_ops::get_object_info(&bucket, key).await
    }

    /// Generate a presigned GET URL for `key` inside `bucket_name`.
    pub async fn presign_get(
        &self,
        bucket_name: &str,
        key: &str,
        expire_secs: u32,
    ) -> Result<String> {
        let config = self.config_ref()?;
        let bucket = s3_ops::create_bucket(config, bucket_name)?;
        s3_ops::presign_get(&bucket, key, expire_secs).await
    }

    /// Generate a presigned PUT URL for `key` inside `bucket_name`.
    pub async fn presign_put(
        &self,
        bucket_name: &str,
        key: &str,
        expire_secs: u32,
    ) -> Result<String> {
        let config = self.config_ref()?;
        let bucket = s3_ops::create_bucket(config, bucket_name)?;
        s3_ops::presign_put(&bucket, key, expire_secs).await
    }

    /// Inventory the remote side of a sync plan without touching local paths.
    pub async fn walk_remote_tree(
        &self,
        bucket_name: &str,
        remote_prefix: &str,
    ) -> Result<Vec<SyncEntry>> {
        let config = self.config_ref()?;
        let bucket = s3_ops::create_bucket(config, bucket_name)?;
        s3_ops::walk_remote_tree(&bucket, remote_prefix).await
    }

    /// Execute a sync operation between a remote S3 prefix and a local path.
    ///
    /// When `options.dry_run` is `true`, returns the plan without performing
    /// any I/O. When `false`, executes all transfers and returns the result.
    pub async fn sync(
        &self,
        bucket_name: &str,
        remote_prefix: &str,
        local_path: &str,
        options: &SyncOptions,
    ) -> Result<SyncOutcome> {
        let config = self.config_ref()?;
        let bucket = s3_ops::create_bucket(config, bucket_name)?;

        let remote_entries = s3_ops::walk_remote_tree(&bucket, remote_prefix).await?;
        let local_entries = sync_ops::walk_local_tree(local_path)?;
        let plan = sync_ops::compute_sync_plan(&remote_entries, &local_entries, options);

        if options.dry_run {
            return Ok(SyncOutcome::DryRun(plan));
        }

        if plan.changes.is_empty() {
            return Ok(SyncOutcome::Result(SyncResult {
                downloaded: 0,
                uploaded: 0,
                deleted: 0,
            }));
        }

        // Use the service's own cancel_flag when available
        let cancel_flag = match &self.mode {
            ServiceMode::Direct { cancel_flag, .. } => cancel_flag.clone(),
            ServiceMode::Channel { .. } => Arc::new(AtomicBool::new(false)),
        };

        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<S3Event>();
        // Drain events — sync_ops sends progress events that we discard in CLI mode.
        tokio::spawn(async move { while event_rx.recv().await.is_some() {} });

        let sync_ctx = SyncContext {
            event_tx,
            cancel_flag,
        };

        let result =
            sync_ops::execute_sync_plan(&bucket, &plan, remote_prefix, local_path, &sync_ctx)
                .await?;

        Ok(SyncOutcome::Result(result))
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Return a reference to the S3 config regardless of service mode.
    ///
    /// In Channel mode the config is owned by the background task; this method
    /// returns an error because direct calls are not supported in that mode
    /// (use `send()` + `poll_event()` instead). In practice, the CLI plugin
    /// always uses Direct mode, so this path is only hit on programming error.
    fn config_ref(&self) -> Result<&S3Config> {
        match &self.mode {
            ServiceMode::Direct { config, .. } => Ok(config),
            ServiceMode::Channel { .. } => {
                anyhow::bail!(
                    "S3Service direct methods are only available in Direct mode. \
                     Use send() + poll_event() for Channel mode."
                )
            }
        }
    }

    // -----------------------------------------------------------------------
    // Background task (Channel / TUI mode)
    // -----------------------------------------------------------------------

    /// Background task that processes commands.
    ///
    /// Absorbed from the former `start_worker()` method in `browser.rs`.
    async fn background_task(
        mut cmd_rx: mpsc::UnboundedReceiver<S3Command>,
        event_tx: mpsc::UnboundedSender<S3Event>,
        tabs: Arc<dyn TabManager>,
        config: S3Config,
        initial_bucket: Option<String>,
        cancel_flag: Arc<AtomicBool>,
    ) {
        // Current bucket handle (lazily created)
        let mut bucket: Option<Box<s3::Bucket>> = None;

        // If we already know the bucket, create a handle
        if let Some(ref bname) = initial_bucket {
            match s3_ops::create_bucket(&config, bname) {
                Ok(b) => bucket = Some(b),
                Err(e) => {
                    let _ = event_tx.send(S3Event::Error(format!(
                        "Failed to create bucket handle: {}",
                        e
                    )));
                }
            }
        }

        // If no bucket configured, list buckets first
        if initial_bucket.is_none() {
            match s3_ops::list_buckets(&config).await {
                Ok(buckets) => {
                    let _ = event_tx.send(S3Event::BucketsListed(buckets));
                }
                Err(e) => {
                    let _ = event_tx.send(S3Event::Error(format!("Failed to list buckets: {}", e)));
                }
            }
            let _ = tabs.request_render();
        } else if bucket.is_some() {
            // List root of the bucket
            let b = bucket.as_ref().unwrap();
            match s3_ops::list_objects(b, "", Some("/")).await {
                Ok(entries) => {
                    let _ = event_tx.send(S3Event::ObjectsListed {
                        prefix: String::new(),
                        entries,
                    });
                }
                Err(e) => {
                    let _ = event_tx.send(S3Event::Error(format!("Failed to list objects: {}", e)));
                }
            }
            let _ = tabs.request_render();
        }

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                S3Command::Connect { bucket: bname } => {
                    if let Some(ref name) = bname {
                        match s3_ops::create_bucket(&config, name) {
                            Ok(b) => {
                                bucket = Some(b);
                                // List root objects of the new bucket
                                let bref = bucket.as_ref().unwrap();
                                match s3_ops::list_objects(bref, "", Some("/")).await {
                                    Ok(entries) => {
                                        let _ = event_tx.send(S3Event::ObjectsListed {
                                            prefix: String::new(),
                                            entries,
                                        });
                                    }
                                    Err(e) => {
                                        let _ = event_tx.send(S3Event::Error(format!(
                                            "Failed to list objects: {}",
                                            e
                                        )));
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = event_tx.send(S3Event::Error(format!(
                                    "Failed to create bucket handle: {}",
                                    e
                                )));
                            }
                        }
                    } else {
                        match s3_ops::list_buckets(&config).await {
                            Ok(buckets) => {
                                let _ = event_tx.send(S3Event::BucketsListed(buckets));
                            }
                            Err(e) => {
                                let _ = event_tx
                                    .send(S3Event::Error(format!("Failed to list buckets: {}", e)));
                            }
                        }
                    }
                    let _ = tabs.request_render();
                }

                S3Command::Disconnect => {
                    drop(bucket.take());
                    break;
                }

                S3Command::ListBuckets => {
                    match s3_ops::list_buckets(&config).await {
                        Ok(buckets) => {
                            let _ = event_tx.send(S3Event::BucketsListed(buckets));
                        }
                        Err(e) => {
                            let _ = event_tx
                                .send(S3Event::Error(format!("Failed to list buckets: {}", e)));
                        }
                    }
                    let _ = tabs.request_render();
                }

                S3Command::ListObjects { prefix } => {
                    let b = match &bucket {
                        Some(b) => b,
                        None => {
                            let _ = event_tx.send(S3Event::Error("No bucket selected".to_string()));
                            let _ = tabs.request_render();
                            continue;
                        }
                    };
                    match s3_ops::list_objects(b, &prefix, Some("/")).await {
                        Ok(entries) => {
                            let _ = event_tx.send(S3Event::ObjectsListed { prefix, entries });
                        }
                        Err(e) => {
                            let _ = event_tx
                                .send(S3Event::Error(format!("Failed to list objects: {}", e)));
                        }
                    }
                    let _ = tabs.request_render();
                }

                S3Command::DownloadObject { key, local } => {
                    let b = match &bucket {
                        Some(b) => b,
                        None => {
                            let _ = event_tx.send(S3Event::Error("No bucket selected".to_string()));
                            continue;
                        }
                    };

                    // Get file size first
                    let total = match s3_ops::get_object_info(b, &key).await {
                        Ok(info) => info.size,
                        Err(_) => 0,
                    };
                    let transfer_id = tui_transfer_id("download");
                    let _ = event_tx.send(S3Event::transfer_lifecycle(
                        AgentTransferEvent::single_object_snapshot(
                            &transfer_id,
                            AgentTransferOperation::Download,
                            AgentTransferPhase::Transferring,
                            1,
                            0,
                            (total > 0).then_some(total),
                        ),
                    ));

                    match s3_ops::download(b, &key).await {
                        Ok(data) => {
                            if cancel_flag.load(Ordering::Acquire) {
                                let _ = event_tx.send(S3Event::transfer_lifecycle(
                                    AgentTransferEvent::single_object_snapshot(
                                        &transfer_id,
                                        AgentTransferOperation::Download,
                                        AgentTransferPhase::Cancelling,
                                        2,
                                        0,
                                        (total > 0).then_some(total),
                                    ),
                                ));
                                let _ = event_tx.send(S3Event::transfer_lifecycle(
                                    AgentTransferEvent::single_object_snapshot(
                                        transfer_id,
                                        AgentTransferOperation::Download,
                                        AgentTransferPhase::Cancelled,
                                        3,
                                        0,
                                        (total > 0).then_some(total),
                                    ),
                                ));
                                let _ = event_tx.send(S3Event::TransferCancelled);
                                let _ = tabs.request_render();
                                continue;
                            }
                            let _ = event_tx.send(S3Event::DownloadProgress {
                                key: key.clone(),
                                transferred: data.len() as u64,
                                total: total.max(data.len() as u64),
                            });
                            if let Err(e) = std::fs::write(&local, &data) {
                                let _ = event_tx.send(S3Event::transfer_lifecycle(
                                    AgentTransferEvent::single_object_snapshot(
                                        transfer_id,
                                        AgentTransferOperation::Download,
                                        AgentTransferPhase::Failed,
                                        2,
                                        data.len() as u64,
                                        Some(total.max(data.len() as u64)),
                                    ),
                                ));
                                let _ = event_tx.send(S3Event::Error(format!(
                                    "Failed to write local file: {}",
                                    e
                                )));
                            } else {
                                let _ = event_tx.send(S3Event::transfer_lifecycle(
                                    AgentTransferEvent::single_object_snapshot(
                                        transfer_id,
                                        AgentTransferOperation::Download,
                                        AgentTransferPhase::Completed,
                                        2,
                                        data.len() as u64,
                                        Some(data.len() as u64),
                                    ),
                                ));
                                let _ = event_tx.send(S3Event::DownloadComplete { key, local });
                            }
                        }
                        Err(e) => {
                            let _ = event_tx.send(S3Event::transfer_lifecycle(
                                AgentTransferEvent::single_object_snapshot(
                                    transfer_id,
                                    AgentTransferOperation::Download,
                                    AgentTransferPhase::Failed,
                                    2,
                                    0,
                                    (total > 0).then_some(total),
                                ),
                            ));
                            let _ =
                                event_tx.send(S3Event::Error(format!("Download failed: {}", e)));
                        }
                    }
                    let _ = tabs.request_render();
                }

                S3Command::UploadObject { local, key } => {
                    let b = match &bucket {
                        Some(b) => b,
                        None => {
                            let _ = event_tx.send(S3Event::Error("No bucket selected".to_string()));
                            continue;
                        }
                    };

                    let data = match std::fs::read(&local) {
                        Ok(d) => d,
                        Err(e) => {
                            let _ = event_tx
                                .send(S3Event::Error(format!("Failed to read local file: {}", e)));
                            continue;
                        }
                    };

                    let total = data.len() as u64;
                    let transfer_id = tui_transfer_id("upload");
                    let _ = event_tx.send(S3Event::transfer_lifecycle(
                        AgentTransferEvent::single_object_snapshot(
                            &transfer_id,
                            AgentTransferOperation::Upload,
                            AgentTransferPhase::Transferring,
                            1,
                            0,
                            Some(total),
                        ),
                    ));
                    let _ = event_tx.send(S3Event::UploadProgress {
                        local: local.clone(),
                        transferred: 0,
                        total,
                    });
                    if cancel_flag.load(Ordering::Acquire) {
                        let _ = event_tx.send(S3Event::transfer_lifecycle(
                            AgentTransferEvent::single_object_snapshot(
                                &transfer_id,
                                AgentTransferOperation::Upload,
                                AgentTransferPhase::Cancelling,
                                2,
                                0,
                                Some(total),
                            ),
                        ));
                        let _ = event_tx.send(S3Event::transfer_lifecycle(
                            AgentTransferEvent::single_object_snapshot(
                                transfer_id,
                                AgentTransferOperation::Upload,
                                AgentTransferPhase::Cancelled,
                                3,
                                0,
                                Some(total),
                            ),
                        ));
                        let _ = event_tx.send(S3Event::TransferCancelled);
                        let _ = tabs.request_render();
                        continue;
                    }

                    match s3_ops::upload(b, &key, &data, None).await {
                        Ok(()) => {
                            if cancel_flag.load(Ordering::Acquire) {
                                let _ = event_tx.send(S3Event::transfer_lifecycle(
                                    AgentTransferEvent::single_object_snapshot(
                                        transfer_id,
                                        AgentTransferOperation::Upload,
                                        AgentTransferPhase::Failed,
                                        2,
                                        0,
                                        Some(total),
                                    ),
                                ));
                                let _ = event_tx.send(S3Event::Error(
                                    "Upload cancellation arrived after the remote request; verify the destination before retrying.".to_string(),
                                ));
                                let _ = tabs.request_render();
                                continue;
                            }
                            let _ = event_tx.send(S3Event::UploadProgress {
                                local: local.clone(),
                                transferred: total,
                                total,
                            });
                            let _ = event_tx.send(S3Event::transfer_lifecycle(
                                AgentTransferEvent::single_object_snapshot(
                                    transfer_id,
                                    AgentTransferOperation::Upload,
                                    AgentTransferPhase::Completed,
                                    2,
                                    total,
                                    Some(total),
                                ),
                            ));
                            let _ = event_tx.send(S3Event::UploadComplete {
                                local,
                                key: key.clone(),
                            });
                            // Auto-refresh: get parent prefix
                            let parent = parent_prefix(&key);
                            if let Ok(entries) = s3_ops::list_objects(b, &parent, Some("/")).await {
                                let _ = event_tx.send(S3Event::ObjectsListed {
                                    prefix: parent,
                                    entries,
                                });
                            }
                        }
                        Err(e) => {
                            let _ = event_tx.send(S3Event::transfer_lifecycle(
                                AgentTransferEvent::single_object_snapshot(
                                    transfer_id,
                                    AgentTransferOperation::Upload,
                                    AgentTransferPhase::Failed,
                                    2,
                                    0,
                                    Some(total),
                                ),
                            ));
                            let _ = event_tx.send(S3Event::Error(format!("Upload failed: {}", e)));
                        }
                    }
                    let _ = tabs.request_render();
                }

                S3Command::DeleteObject(key) => {
                    let b = match &bucket {
                        Some(b) => b,
                        None => {
                            let _ = event_tx.send(S3Event::Error("No bucket selected".to_string()));
                            continue;
                        }
                    };

                    match s3_ops::delete(b, &key).await {
                        Ok(()) => {
                            let name = key.rsplit('/').next().unwrap_or(&key);
                            let _ = event_tx
                                .send(S3Event::OperationComplete(format!("Deleted '{}'", name)));
                            // Auto-refresh parent
                            let parent = parent_prefix(&key);
                            if let Ok(entries) = s3_ops::list_objects(b, &parent, Some("/")).await {
                                let _ = event_tx.send(S3Event::ObjectsListed {
                                    prefix: parent,
                                    entries,
                                });
                            }
                        }
                        Err(e) => {
                            let _ = event_tx.send(S3Event::Error(format!("Delete failed: {}", e)));
                        }
                    }
                    let _ = tabs.request_render();
                }

                S3Command::CopyObject { from, to } => {
                    let b = match &bucket {
                        Some(b) => b,
                        None => {
                            let _ = event_tx.send(S3Event::Error("No bucket selected".to_string()));
                            continue;
                        }
                    };

                    match s3_ops::copy_object(b, &from, &to).await {
                        Ok(()) => {
                            let _ = event_tx.send(S3Event::OperationComplete(format!(
                                "Copied '{}' -> '{}'",
                                from, to
                            )));
                            let parent = parent_prefix(&to);
                            if let Ok(entries) = s3_ops::list_objects(b, &parent, Some("/")).await {
                                let _ = event_tx.send(S3Event::ObjectsListed {
                                    prefix: parent,
                                    entries,
                                });
                            }
                        }
                        Err(e) => {
                            let _ = event_tx.send(S3Event::Error(format!("Copy failed: {}", e)));
                        }
                    }
                    let _ = tabs.request_render();
                }

                S3Command::MoveObject { from, to } => {
                    let b = match &bucket {
                        Some(b) => b,
                        None => {
                            let _ = event_tx.send(S3Event::Error("No bucket selected".to_string()));
                            continue;
                        }
                    };

                    // Move = copy + delete
                    match s3_ops::copy_object(b, &from, &to).await {
                        Ok(()) => {
                            if let Err(e) = s3_ops::delete(b, &from).await {
                                let _ = event_tx.send(S3Event::Error(format!(
                                    "Move: copied but failed to delete source: {}",
                                    e
                                )));
                            } else {
                                let _ = event_tx.send(S3Event::OperationComplete(format!(
                                    "Moved '{}' -> '{}'",
                                    from, to
                                )));
                            }
                            let parent = parent_prefix(&to);
                            if let Ok(entries) = s3_ops::list_objects(b, &parent, Some("/")).await {
                                let _ = event_tx.send(S3Event::ObjectsListed {
                                    prefix: parent,
                                    entries,
                                });
                            }
                        }
                        Err(e) => {
                            let _ = event_tx.send(S3Event::Error(format!("Move failed: {}", e)));
                        }
                    }
                    let _ = tabs.request_render();
                }

                S3Command::SyncExecute {
                    remote_prefix,
                    local_path,
                    options,
                } => {
                    let b = match &bucket {
                        Some(b) => b,
                        None => {
                            let _ = event_tx.send(S3Event::Error("No bucket selected".to_string()));
                            continue;
                        }
                    };

                    let dry_run = options.dry_run;
                    match s3_ops::walk_remote_tree(b, &remote_prefix).await {
                        Ok(remote_entries) => {
                            match sync_ops::walk_local_tree(&local_path) {
                                Ok(local_entries) => {
                                    let plan = sync_ops::compute_sync_plan(
                                        &remote_entries,
                                        &local_entries,
                                        &options,
                                    );
                                    if dry_run {
                                        let _ = event_tx.send(S3Event::SyncPlanReady(plan));
                                    } else {
                                        let sync_ctx = SyncContext {
                                            event_tx: event_tx.clone(),
                                            cancel_flag: cancel_flag.clone(),
                                        };
                                        match sync_ops::execute_sync_plan(
                                            b,
                                            &plan,
                                            &remote_prefix,
                                            &local_path,
                                            &sync_ctx,
                                        )
                                        .await
                                        {
                                            Ok(result) => {
                                                let _ = event_tx.send(S3Event::SyncComplete {
                                                    downloaded: result.downloaded,
                                                    uploaded: result.uploaded,
                                                    deleted: result.deleted,
                                                });
                                                // Auto-refresh remote
                                                if let Ok(entries) = s3_ops::list_objects(
                                                    b,
                                                    &remote_prefix,
                                                    Some("/"),
                                                )
                                                .await
                                                {
                                                    let _ = event_tx.send(S3Event::ObjectsListed {
                                                        prefix: remote_prefix,
                                                        entries,
                                                    });
                                                }
                                            }
                                            Err(e) => {
                                                let _ = event_tx.send(S3Event::Error(format!(
                                                    "Sync failed: {}",
                                                    e
                                                )));
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    let _ = event_tx
                                        .send(S3Event::Error(format!("Scan local failed: {}", e)));
                                }
                            }
                        }
                        Err(e) => {
                            let _ =
                                event_tx.send(S3Event::Error(format!("Scan remote failed: {}", e)));
                        }
                    }
                    let _ = tabs.request_render();
                }

                S3Command::CancelTransfer => {
                    cancel_flag.store(true, Ordering::Relaxed);
                    let _ = event_tx.send(S3Event::TransferCancelled);
                    let _ = tabs.request_render();
                }
            }
        }

        // Loop exits on sender drop or Disconnect
        drop(bucket);
    }
}

// ---------------------------------------------------------------------------
// SyncOutcome
// ---------------------------------------------------------------------------

/// The result of a `sync()` call in Direct mode.
///
/// `DryRun` contains the computed plan (no I/O performed).
/// `Result` contains the transfer counts after execution.
pub enum SyncOutcome {
    /// Dry-run: plan was computed but no files were transferred.
    DryRun(SyncPlan),
    /// Execution complete: actual transfer counts.
    Result(SyncResult),
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn tui_transfer_id(operation: &str) -> String {
    format!("s3-tui-{operation}-{}", Utc::now().timestamp_micros())
}

/// Get the parent prefix for an S3 key.
/// e.g. "a/b/c.txt" -> "a/b/", "a/b/" -> "a/", "a.txt" -> ""
fn parent_prefix(key: &str) -> String {
    let trimmed = key.trim_end_matches('/');
    if let Some(pos) = trimmed.rfind('/') {
        format!("{}/", &trimmed[..pos])
    } else {
        String::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // === Send/Sync compile-time assertions ===

    fn assert_send<T: Send>() {}

    #[allow(dead_code)]
    fn assert_sync<T: Sync>() {}

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn service_is_send() {
        assert_send::<S3Service>();
    }

    #[test]
    fn command_is_send() {
        assert_send::<S3Command>();
    }

    #[test]
    fn event_is_send() {
        assert_send::<S3Event>();
    }

    #[test]
    fn mutex_service_is_send_sync() {
        assert_send_sync::<std::sync::Mutex<S3Service>>();
    }

    #[test]
    fn direct_mode_constructs() {
        let config = S3Config::default();
        let svc = S3Service::new_direct(config);
        // Direct mode: poll_event always returns None
        let mut svc = svc;
        assert!(svc.poll_event().is_none());
    }
}
