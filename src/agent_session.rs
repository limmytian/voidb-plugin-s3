//! Persistent, cancellable S3 transfer sessions for Agent workflows.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use s3::serde_types::Part;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use voidb_core::{
    AGENT_TRANSFER_PROTOCOL_VERSION, AgentSessionCallRequest, AgentSessionCallResult,
    AgentSessionConcurrency, AgentSessionOpenContext, AgentTransferChecksum,
    AgentTransferChecksumAlgorithm, AgentTransferChecksumScope, AgentTransferChunkState,
    AgentTransferCleanupReport, AgentTransferCleanupState, AgentTransferEvent,
    AgentTransferOperation, AgentTransferPhase, AgentTransferProgress,
    AgentTransferResumeCheckpoint, AgentTransferRetry, LocalPathScope, PluginAgentSession,
    PluginAgentSessionFactory, PluginSessionError, PluginSessionErrorCode, PluginSessionHealth,
    PluginSessionPurpose, RedactionStatus,
};

use crate::config::S3Config;
use crate::s3_ops;
use crate::transfer_contract::s3_transfer_contract;

const TRANSFER_CAPABILITY: &str = "s3.transfer";
const STATUS_CAPABILITY: &str = "s3.transfer_status";
const DEFAULT_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const MIN_MULTIPART_BYTES: usize = 5 * 1024 * 1024;
const MAX_CHUNK_BYTES: usize = 64 * 1024 * 1024;
const MAX_TRANSFER_BYTES: usize = 128 * 1024 * 1024;
const RESUME_TTL_SECONDS: i64 = 60 * 60;

pub struct S3AgentSessionFactory {
    config: S3Config,
}

impl S3AgentSessionFactory {
    pub fn new(config: S3Config) -> Self {
        Self { config }
    }
}

#[async_trait]
impl PluginAgentSessionFactory for S3AgentSessionFactory {
    fn plugin_id(&self) -> &str {
        "s3"
    }

    async fn open(
        &self,
        context: AgentSessionOpenContext,
    ) -> Result<Arc<dyn PluginAgentSession>, PluginSessionError> {
        validate_open(&context)?;
        s3_transfer_contract().validate().map_err(|_| {
            error(
                PluginSessionErrorCode::HealthFailed,
                "The S3 transfer contract is invalid.",
            )
        })?;
        let binding_scope = sha256_scope(&[
            &context.binding.grant_id,
            &context.binding.profile_id,
            &context.binding.plugin_id,
        ]);
        Ok(Arc::new(S3AgentSession {
            config: self.config.clone(),
            binding_scope,
            state: Mutex::new(S3TransferState::default()),
            cancel: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        }))
    }
}

struct S3AgentSession {
    config: S3Config,
    binding_scope: String,
    state: Mutex<S3TransferState>,
    cancel: AtomicBool,
    closed: AtomicBool,
}

#[derive(Default)]
struct S3TransferState {
    active: bool,
    event: Option<AgentTransferEvent>,
    resume: Option<S3ResumeState>,
}

enum S3ResumeState {
    Upload {
        token: String,
        scope: String,
        bucket: String,
        key: String,
        data: Vec<u8>,
        upload_id: String,
        parts: Vec<Part>,
        next_offset: usize,
        chunk_bytes: usize,
    },
    Download {
        token: String,
        scope: String,
        bucket: String,
        key: String,
        local_scope: LocalPathScope,
        local_path: String,
        data: Vec<u8>,
        total: u64,
        etag: Option<String>,
        chunk_bytes: usize,
    },
}

#[async_trait]
impl PluginAgentSession for S3AgentSession {
    fn concurrency(&self) -> AgentSessionConcurrency {
        AgentSessionConcurrency::Multiplexed
    }

    async fn call(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(error(
                PluginSessionErrorCode::OwnerUnavailable,
                "The S3 transfer session is closed.",
            ));
        }
        match request.capability.as_str() {
            STATUS_CAPABILITY | "transfer_status" => self.status(request).await,
            TRANSFER_CAPABILITY | "transfer" => self.transfer(request).await,
            _ => Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "The S3 FileTransfer session accepts transfer and transfer_status only.",
            )),
        }
    }

    async fn health(&self) -> Result<PluginSessionHealth, PluginSessionError> {
        if self.closed.load(Ordering::Acquire) {
            return Ok(PluginSessionHealth::Closed);
        }
        Ok(if self.state.lock().await.active {
            PluginSessionHealth::Busy
        } else {
            PluginSessionHealth::Ready
        })
    }

    async fn cancel(&self, _call_id: &str) -> Result<(), PluginSessionError> {
        self.cancel.store(true, Ordering::Release);
        Ok(())
    }

    async fn close(&self, _reason: String) -> Result<(), PluginSessionError> {
        self.closed.store(true, Ordering::Release);
        self.cancel.store(true, Ordering::Release);
        let resume = self.state.lock().await.resume.take();
        if let Some(S3ResumeState::Upload {
            bucket,
            key,
            upload_id,
            ..
        }) = resume
            && let Ok(bucket) = s3_ops::create_bucket(&self.config, &bucket)
        {
            let _ = bucket.abort_upload(&key, &upload_id).await;
        }
        Ok(())
    }
}

impl S3AgentSession {
    async fn status(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        let state = self.state.lock().await;
        AgentSessionCallResult::bounded(
            request.call_id,
            json!({ "active": state.active, "event": state.event }),
            request.output_limit_bytes,
        )
    }

    async fn transfer(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        if !request.destructive_acknowledged {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "S3 transfer requires destructive acknowledgement for local or remote side effects.",
            ));
        }
        let operation = required_string(&request.input, "operation")?;
        let operation_kind = match operation.as_str() {
            "upload" => AgentTransferOperation::Upload,
            "download" => AgentTransferOperation::Download,
            "copy" => AgentTransferOperation::Copy,
            "move" => AgentTransferOperation::Move,
            _ => {
                return Err(error(
                    PluginSessionErrorCode::PolicyDenied,
                    "S3 transfer operation must be upload, download, copy, or move.",
                ));
            }
        };
        if matches!(operation.as_str(), "copy" | "move")
            && request.input.get("expected_sha256").is_some()
        {
            return Err(error(
                PluginSessionErrorCode::Unsupported,
                "S3 expected_sha256 is supported only for upload and download.",
            ));
        }
        {
            let mut state = self.state.lock().await;
            if state.active {
                return Err(error(
                    PluginSessionErrorCode::PolicyDenied,
                    "Only one S3 transfer may be active in a session.",
                ));
            }
            state.active = true;
        }
        self.cancel.store(false, Ordering::Release);

        let result = match operation.as_str() {
            "upload" => self.upload(&request).await,
            "download" => self.download(&request).await,
            "copy" => self.copy_or_move(&request, false).await,
            "move" => self.copy_or_move(&request, true).await,
            _ => unreachable!("operation validated before session activation"),
        };
        self.state.lock().await.active = false;

        match result {
            Ok(event) => AgentSessionCallResult::bounded(
                request.call_id,
                json!({ "event": event }),
                request.output_limit_bytes,
            ),
            Err(failure) => {
                if self.cancel.load(Ordering::Acquire) {
                    Err(error(
                        PluginSessionErrorCode::Cancelled,
                        "The S3 transfer was cancelled; query transfer_status for cleanup and resume state.",
                    ))
                } else {
                    self.fail_event(&request.call_id, operation_kind).await;
                    Err(failure)
                }
            }
        }
    }

    async fn upload(
        &self,
        request: &AgentSessionCallRequest,
    ) -> Result<AgentTransferEvent, PluginSessionError> {
        let bucket_name = required_string(&request.input, "bucket")?;
        let key = required_string(&request.input, "key")?;
        let chunk_bytes = chunk_bytes(&request.input)?;
        let resume_token = optional_string(&request.input, "resume_token")?;
        let is_resume = resume_token.is_some();
        let expected_sha256 = expected_sha256(&request.input)?;

        let (data, upload_id, mut parts, mut offset, scope, token) = if let Some(token) =
            resume_token
        {
            let resume = { self.state.lock().await.resume.take() };
            match resume {
                Some(S3ResumeState::Upload {
                    token: expected,
                    scope,
                    bucket,
                    key: resume_key,
                    data,
                    upload_id,
                    parts,
                    next_offset,
                    chunk_bytes: resumed_chunk,
                }) if token == expected
                    && bucket == bucket_name
                    && resume_key == key
                    && resumed_chunk == chunk_bytes =>
                {
                    (data, upload_id, parts, next_offset, scope, expected)
                }
                other => {
                    self.state.lock().await.resume = other;
                    return Err(error(
                        PluginSessionErrorCode::BindingMismatch,
                        "The S3 upload resume token does not match this session and transfer scope.",
                    ));
                }
            }
        } else {
            let local_root = required_string(&request.input, "local_root")?;
            let local_path = required_string(&request.input, "local_path")?;
            let local_scope = LocalPathScope::new(&local_root).map_err(local_error)?;
            let data = local_scope.read_file(&local_path).map_err(local_error)?;
            enforce_transfer_bound(data.len())?;
            let scope = sha256_scope(&[
                &self.binding_scope,
                &bucket_name,
                &key,
                &local_root,
                &local_path,
            ]);
            let token = resume_token_for(&request.call_id, &scope);
            (data, String::new(), Vec::new(), 0, scope, token)
        };
        let total = data.len() as u64;
        let local_checksum = expected_sha256
            .as_deref()
            .map(|expected| verify_sha256(&data, expected))
            .transpose()?;
        let bucket = s3_ops::create_bucket(&self.config, &bucket_name).map_err(target_error)?;
        if !is_resume
            && !request
                .input
                .get("replace")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            && s3_ops::object_exists(&bucket, &key)
                .await
                .map_err(target_error)?
        {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "S3 upload destination exists; set replace only with explicit replacement authorization.",
            ));
        }
        self.set_event(progress_event(
            &request.call_id,
            AgentTransferOperation::Upload,
            AgentTransferPhase::Transferring,
            1,
            offset as u64,
            total,
            parts.len() as u32,
            chunk_count(data.len(), chunk_bytes),
            None,
            None,
            false,
        ))
        .await?;

        if data.len() < MIN_MULTIPART_BYTES {
            if self.cancel.load(Ordering::Acquire) {
                self.cancel_without_resume(&request.call_id, AgentTransferOperation::Upload, total)
                    .await?;
                return Err(cancelled());
            }
            s3_ops::upload(&bucket, &key, &data, None)
                .await
                .map_err(target_error)?;
        } else {
            let upload_id = if upload_id.is_empty() {
                bucket
                    .initiate_multipart_upload(&key, "application/octet-stream")
                    .await
                    .map_err(target_error)?
                    .upload_id
            } else {
                upload_id
            };
            while offset < data.len() {
                if self.cancel.load(Ordering::Acquire) {
                    let checkpoint = checkpoint(&token, &scope, offset as u64, parts.len() as u32);
                    self.state.lock().await.resume = Some(S3ResumeState::Upload {
                        token: token.clone(),
                        scope: scope.clone(),
                        bucket: bucket_name.clone(),
                        key: key.clone(),
                        data,
                        upload_id,
                        parts,
                        next_offset: offset,
                        chunk_bytes,
                    });
                    self.set_cancelled_event(cancelled_event(
                        &request.call_id,
                        AgentTransferOperation::Upload,
                        offset as u64,
                        total,
                        checkpoint,
                        AgentTransferCleanupState::RetainedForResume,
                    ))
                    .await?;
                    return Err(cancelled());
                }
                let chunk_start = offset;
                let end = offset.saturating_add(chunk_bytes).min(data.len());
                let part_number = parts.len() as u32 + 1;
                let part = match bucket
                    .put_multipart_chunk(
                        data[offset..end].to_vec(),
                        &key,
                        part_number,
                        &upload_id,
                        "application/octet-stream",
                    )
                    .await
                {
                    Ok(part) => part,
                    Err(_) => {
                        return Err(self
                            .retain_upload_failure(
                                &request.call_id,
                                token,
                                scope,
                                bucket_name,
                                key,
                                data,
                                upload_id,
                                parts,
                                offset,
                                chunk_bytes,
                            )
                            .await);
                    }
                };
                parts.push(part);
                offset = end;
                self.set_event(progress_event(
                    &request.call_id,
                    AgentTransferOperation::Upload,
                    AgentTransferPhase::Transferring,
                    u64::from(part_number) + 1,
                    offset as u64,
                    total,
                    part_number,
                    chunk_count(data.len(), chunk_bytes),
                    Some((part_number, chunk_start as u64, (end - chunk_start) as u64)),
                    None,
                    false,
                ))
                .await?;
            }
            let response = match bucket
                .complete_multipart_upload(&key, &upload_id, parts.clone())
                .await
            {
                Ok(response) => response,
                Err(_) => {
                    return Err(self
                        .retain_upload_failure(
                            &request.call_id,
                            token,
                            scope,
                            bucket_name,
                            key,
                            data,
                            upload_id,
                            parts,
                            offset,
                            chunk_bytes,
                        )
                        .await);
                }
            };
            if !(200..300).contains(&response.status_code()) {
                return Err(self
                    .retain_upload_failure(
                        &request.call_id,
                        token,
                        scope,
                        bucket_name,
                        key,
                        data,
                        upload_id,
                        parts,
                        offset,
                        chunk_bytes,
                    )
                    .await);
            }
        }

        let verified = s3_ops::get_object_info(&bucket, &key)
            .await
            .map_err(target_error)?;
        if verified.size != total {
            return Err(target_error_message(
                "S3 upload verification detected a size mismatch.",
            ));
        }
        if let Some(expected) = expected_sha256.as_deref() {
            let remote_data = s3_ops::download(&bucket, &key)
                .await
                .map_err(target_error)?;
            verify_sha256(&remote_data, expected)?;
        }
        self.state.lock().await.resume = None;
        let mut event = completed_event(
            &request.call_id,
            AgentTransferOperation::Upload,
            total,
            chunk_count(data.len(), chunk_bytes),
            verified.etag,
        );
        if local_checksum.is_some() {
            event.checksum = local_checksum;
        }
        self.set_event(event.clone()).await?;
        Ok(event)
    }

    async fn download(
        &self,
        request: &AgentSessionCallRequest,
    ) -> Result<AgentTransferEvent, PluginSessionError> {
        if request
            .input
            .get("replace")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Err(error(
                PluginSessionErrorCode::Unsupported,
                "S3 Agent downloads fail closed on an existing local destination; replacement is not supported.",
            ));
        }
        let bucket_name = required_string(&request.input, "bucket")?;
        let key = required_string(&request.input, "key")?;
        let chunk_bytes = chunk_bytes(&request.input)?;
        let resume_token = optional_string(&request.input, "resume_token")?;
        let expected_sha256 = expected_sha256(&request.input)?;
        let bucket = s3_ops::create_bucket(&self.config, &bucket_name).map_err(target_error)?;
        let remote = s3_ops::get_object_info(&bucket, &key)
            .await
            .map_err(target_error)?;
        enforce_transfer_bound(remote.size as usize)?;

        let (local_scope, local_path, mut data, scope, token) = if let Some(token) = resume_token {
            let resume = { self.state.lock().await.resume.take() };
            match resume {
                Some(S3ResumeState::Download {
                    token: expected,
                    scope,
                    bucket,
                    key: resume_key,
                    local_scope,
                    local_path,
                    data,
                    total,
                    etag,
                    chunk_bytes: resumed_chunk,
                }) if token == expected
                    && bucket == bucket_name
                    && resume_key == key
                    && total == remote.size
                    && etag == remote.etag
                    && resumed_chunk == chunk_bytes =>
                {
                    (local_scope, local_path, data, scope, expected)
                }
                other => {
                    self.state.lock().await.resume = other;
                    return Err(error(
                        PluginSessionErrorCode::BindingMismatch,
                        "The S3 download resume token or remote identity is stale.",
                    ));
                }
            }
        } else {
            let local_root = required_string(&request.input, "local_root")?;
            let local_path = required_string(&request.input, "local_path")?;
            let local_scope = LocalPathScope::new(&local_root).map_err(local_error)?;
            local_scope
                .validate_new_file(&local_path)
                .map_err(local_error)?;
            let scope = sha256_scope(&[
                &self.binding_scope,
                &bucket_name,
                &key,
                &local_root,
                &local_path,
            ]);
            let token = resume_token_for(&request.call_id, &scope);
            (local_scope, local_path, Vec::new(), scope, token)
        };
        let total = remote.size;
        self.set_event(progress_event(
            &request.call_id,
            AgentTransferOperation::Download,
            AgentTransferPhase::Transferring,
            u64::from(completed_chunks(data.len(), chunk_bytes)) + 1,
            data.len() as u64,
            total,
            completed_chunks(data.len(), chunk_bytes),
            chunk_count(total as usize, chunk_bytes),
            None,
            None,
            false,
        ))
        .await?;
        while (data.len() as u64) < total {
            if self.cancel.load(Ordering::Acquire) {
                let completed_chunks = completed_chunks(data.len(), chunk_bytes);
                let checkpoint = checkpoint(&token, &scope, data.len() as u64, completed_chunks);
                self.state.lock().await.resume = Some(S3ResumeState::Download {
                    token: token.clone(),
                    scope: scope.clone(),
                    bucket: bucket_name.clone(),
                    key: key.clone(),
                    local_scope,
                    local_path,
                    data,
                    total,
                    etag: remote.etag,
                    chunk_bytes,
                });
                self.set_cancelled_event(cancelled_event(
                    &request.call_id,
                    AgentTransferOperation::Download,
                    checkpoint.completed_bytes,
                    total,
                    checkpoint,
                    AgentTransferCleanupState::RetainedForResume,
                ))
                .await?;
                return Err(cancelled());
            }
            let start = data.len() as u64;
            let end = start
                .saturating_add(chunk_bytes as u64)
                .min(total)
                .saturating_sub(1);
            let response = match bucket.get_object_range(&key, start, Some(end)).await {
                Ok(response) => response,
                Err(_) => {
                    return Err(self
                        .retain_download_failure(
                            &request.call_id,
                            token,
                            scope,
                            bucket_name,
                            key,
                            local_scope,
                            local_path,
                            data,
                            total,
                            remote.etag,
                            chunk_bytes,
                        )
                        .await);
                }
            };
            let expected = end.saturating_sub(start).saturating_add(1);
            if response.as_slice().len() as u64 != expected {
                return Err(self
                    .retain_download_failure(
                        &request.call_id,
                        token,
                        scope,
                        bucket_name,
                        key,
                        local_scope,
                        local_path,
                        data,
                        total,
                        remote.etag,
                        chunk_bytes,
                    )
                    .await);
            }
            data.extend_from_slice(response.as_slice());
            let chunks = completed_chunks(data.len(), chunk_bytes);
            self.set_event(progress_event(
                &request.call_id,
                AgentTransferOperation::Download,
                AgentTransferPhase::Transferring,
                u64::from(chunks) + 1,
                data.len() as u64,
                total,
                chunks,
                chunk_count(total as usize, chunk_bytes),
                Some((chunks, start, end.saturating_sub(start).saturating_add(1))),
                None,
                false,
            ))
            .await?;
        }
        let verified_checksum = expected_sha256
            .as_deref()
            .map(|expected| verify_sha256(&data, expected))
            .transpose()?;
        local_scope
            .write_new_file(&local_path, &data)
            .map_err(local_error)?;
        self.state.lock().await.resume = None;
        let mut event = completed_event(
            &request.call_id,
            AgentTransferOperation::Download,
            total,
            chunk_count(total as usize, chunk_bytes),
            remote.etag,
        );
        if verified_checksum.is_some() {
            event.checksum = verified_checksum;
        }
        self.set_event(event.clone()).await?;
        Ok(event)
    }

    async fn copy_or_move(
        &self,
        request: &AgentSessionCallRequest,
        move_source: bool,
    ) -> Result<AgentTransferEvent, PluginSessionError> {
        let bucket = required_string(&request.input, "bucket")?;
        let key = required_string(&request.input, "key")?;
        let destination_bucket = required_string(&request.input, "destination_bucket")?;
        let destination_key = required_string(&request.input, "destination_key")?;
        let replace = request
            .input
            .get("replace")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let source_handle = s3_ops::create_bucket(&self.config, &bucket).map_err(target_error)?;
        let source_info = s3_ops::get_object_info(&source_handle, &key)
            .await
            .map_err(target_error)?;
        let operation = if move_source {
            AgentTransferOperation::Move
        } else {
            AgentTransferOperation::Copy
        };
        self.set_event(progress_event(
            &request.call_id,
            operation,
            AgentTransferPhase::Transferring,
            1,
            0,
            source_info.size,
            0,
            1,
            None,
            None,
            false,
        ))
        .await?;
        if self.cancel.load(Ordering::Acquire) {
            self.cancel_without_resume(&request.call_id, operation, source_info.size)
                .await?;
            return Err(cancelled());
        }
        let entry = if move_source {
            s3_ops::move_object_verified(
                &self.config,
                &bucket,
                &key,
                &destination_bucket,
                &destination_key,
                replace,
            )
            .await
        } else {
            s3_ops::copy_object_verified(
                &self.config,
                &bucket,
                &key,
                &destination_bucket,
                &destination_key,
                replace,
            )
            .await
        }
        .map_err(target_error)?;
        let event = completed_event(
            &request.call_id,
            if move_source {
                AgentTransferOperation::Move
            } else {
                AgentTransferOperation::Copy
            },
            entry.size,
            1,
            entry.etag,
        );
        self.set_event(event.clone()).await?;
        Ok(event)
    }

    async fn set_event(&self, event: AgentTransferEvent) -> Result<(), PluginSessionError> {
        let contract = s3_transfer_contract();
        let mut state = self.state.lock().await;
        let validation = match state
            .event
            .as_ref()
            .filter(|previous| previous.transfer_id == event.transfer_id)
        {
            Some(previous) => event.validate_transition(previous, &contract),
            None => event.validate(&contract),
        };
        validation.map_err(|_| {
            error(
                PluginSessionErrorCode::HealthFailed,
                "S3 produced an invalid transfer lifecycle event.",
            )
        })?;
        state.event = Some(event);
        Ok(())
    }

    async fn cancel_without_resume(
        &self,
        transfer_id: &str,
        operation: AgentTransferOperation,
        total: u64,
    ) -> Result<(), PluginSessionError> {
        let event = cancelled_event(
            transfer_id,
            operation,
            0,
            total,
            checkpoint(
                &resume_token_for(transfer_id, &self.binding_scope),
                &self.binding_scope,
                0,
                0,
            ),
            AgentTransferCleanupState::NotApplicable,
        );
        let mut event = event;
        event.checkpoint = None;
        self.set_cancelled_event(event).await
    }

    async fn set_cancelled_event(
        &self,
        mut event: AgentTransferEvent,
    ) -> Result<(), PluginSessionError> {
        let previous = self.state.lock().await.event.clone();
        if let Some(previous) = previous
            && previous.transfer_id == event.transfer_id
            && previous.phase != AgentTransferPhase::Cancelling
        {
            event.progress.bytes_total = previous.progress.bytes_total;
            event.progress.objects_total = previous.progress.objects_total;
            event.progress.chunks_total = previous.progress.chunks_total;
            let mut cancelling = event.clone();
            cancelling.sequence = previous.sequence.saturating_add(1);
            cancelling.observed_at = Utc::now();
            cancelling.phase = AgentTransferPhase::Cancelling;
            cancelling.terminal = false;
            cancelling.cleanup = None;
            self.set_event(cancelling).await?;
            event.sequence = previous.sequence.saturating_add(2);
            event.observed_at = Utc::now();
        }
        self.set_event(event).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn retain_upload_failure(
        &self,
        transfer_id: &str,
        token: String,
        scope: String,
        bucket: String,
        key: String,
        data: Vec<u8>,
        upload_id: String,
        parts: Vec<Part>,
        next_offset: usize,
        chunk_bytes: usize,
    ) -> PluginSessionError {
        let total = data.len() as u64;
        let chunks = parts.len() as u32;
        let checkpoint = checkpoint(&token, &scope, next_offset as u64, chunks);
        self.state.lock().await.resume = Some(S3ResumeState::Upload {
            token,
            scope,
            bucket,
            key,
            data,
            upload_id,
            parts,
            next_offset,
            chunk_bytes,
        });
        let mut event = progress_event(
            transfer_id,
            AgentTransferOperation::Upload,
            AgentTransferPhase::RetryWaiting,
            u64::from(chunks) + 2,
            next_offset as u64,
            total,
            chunks,
            chunk_count(total as usize, chunk_bytes),
            None,
            Some(checkpoint),
            false,
        );
        event.retry = Some(AgentTransferRetry {
            attempt: 1,
            backoff_ms: 250,
            reason_code: "target_unavailable".into(),
        });
        event.cleanup = Some(AgentTransferCleanupReport {
            local_staging: AgentTransferCleanupState::NotApplicable,
            remote_partial: AgentTransferCleanupState::RetainedForResume,
            remote_lock: AgentTransferCleanupState::NotApplicable,
        });
        match self.set_event(event).await {
            Ok(()) => target_error_message(
                "The S3 multipart request was interrupted; a scoped resume checkpoint was retained.",
            ),
            Err(failure) => failure,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn retain_download_failure(
        &self,
        transfer_id: &str,
        token: String,
        scope: String,
        bucket: String,
        key: String,
        local_scope: LocalPathScope,
        local_path: String,
        data: Vec<u8>,
        total: u64,
        etag: Option<String>,
        chunk_bytes: usize,
    ) -> PluginSessionError {
        let chunks = completed_chunks(data.len(), chunk_bytes);
        let completed = data.len() as u64;
        let checkpoint = checkpoint(&token, &scope, completed, chunks);
        self.state.lock().await.resume = Some(S3ResumeState::Download {
            token,
            scope,
            bucket,
            key,
            local_scope,
            local_path,
            data,
            total,
            etag,
            chunk_bytes,
        });
        let mut event = progress_event(
            transfer_id,
            AgentTransferOperation::Download,
            AgentTransferPhase::RetryWaiting,
            u64::from(chunks) + 2,
            completed,
            total,
            chunks,
            chunk_count(total as usize, chunk_bytes),
            None,
            Some(checkpoint),
            false,
        );
        event.retry = Some(AgentTransferRetry {
            attempt: 1,
            backoff_ms: 250,
            reason_code: "target_unavailable".into(),
        });
        event.cleanup = Some(AgentTransferCleanupReport {
            local_staging: AgentTransferCleanupState::RetainedForResume,
            remote_partial: AgentTransferCleanupState::NotApplicable,
            remote_lock: AgentTransferCleanupState::NotApplicable,
        });
        match self.set_event(event).await {
            Ok(()) => target_error_message(
                "The S3 range request was interrupted; a scoped resume checkpoint was retained.",
            ),
            Err(failure) => failure,
        }
    }

    async fn fail_event(&self, transfer_id: &str, requested_operation: AgentTransferOperation) {
        let previous = self.state.lock().await.event.clone();
        if previous.as_ref().is_some_and(|event| {
            event.operation == requested_operation
                && (event.terminal
                    || (event.phase == AgentTransferPhase::RetryWaiting
                        && event.checkpoint.is_some()))
        }) {
            return;
        }
        let (transfer_id, operation, progress) = previous
            .filter(|event| event.operation == requested_operation && !event.terminal)
            .map(|event| (event.transfer_id, event.operation, event.progress))
            .unwrap_or((
                transfer_id.to_string(),
                requested_operation,
                AgentTransferProgress::default(),
            ));
        let event = AgentTransferEvent {
            protocol_version: AGENT_TRANSFER_PROTOCOL_VERSION,
            transfer_id,
            sequence: 10_000,
            observed_at: Utc::now(),
            operation,
            phase: AgentTransferPhase::Failed,
            progress,
            current_chunk: None,
            checkpoint: None,
            checksum: None,
            retry: None,
            conflict: None,
            cleanup: Some(AgentTransferCleanupReport {
                local_staging: AgentTransferCleanupState::NotApplicable,
                remote_partial: AgentTransferCleanupState::FailedClosed,
                remote_lock: AgentTransferCleanupState::NotApplicable,
            }),
            terminal: true,
            redaction: RedactionStatus::Applied,
        };
        let _ = self.set_event(event).await;
    }
}

fn validate_open(context: &AgentSessionOpenContext) -> Result<(), PluginSessionError> {
    if context.binding.purpose != PluginSessionPurpose::FileTransfer {
        return Err(error(
            PluginSessionErrorCode::Unsupported,
            "S3 persistent sessions require the FileTransfer purpose.",
        ));
    }
    if context
        .binding
        .allowed_capabilities
        .iter()
        .any(|capability| {
            !matches!(
                capability.as_str(),
                TRANSFER_CAPABILITY | STATUS_CAPABILITY | "transfer" | "transfer_status"
            )
        })
    {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "S3 FileTransfer sessions accept one unmixed transfer capability family.",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn progress_event(
    transfer_id: &str,
    operation: AgentTransferOperation,
    phase: AgentTransferPhase,
    sequence: u64,
    bytes_completed: u64,
    bytes_total: u64,
    chunks_completed: u32,
    chunks_total: u32,
    chunk: Option<(u32, u64, u64)>,
    checkpoint: Option<AgentTransferResumeCheckpoint>,
    terminal: bool,
) -> AgentTransferEvent {
    AgentTransferEvent {
        protocol_version: AGENT_TRANSFER_PROTOCOL_VERSION,
        transfer_id: transfer_id.to_string(),
        sequence: sequence.max(1),
        observed_at: Utc::now(),
        operation,
        phase,
        progress: AgentTransferProgress {
            bytes_completed,
            bytes_total: Some(bytes_total),
            objects_completed: u64::from(phase == AgentTransferPhase::Completed),
            objects_total: Some(1),
            chunks_completed,
            chunks_total: Some(chunks_total.max(1)),
        },
        current_chunk: chunk.map(|(index, offset, length)| AgentTransferChunkState {
            index: index.max(1),
            offset,
            length: length.max(1),
            completed: true,
        }),
        checkpoint,
        checksum: None,
        retry: None,
        conflict: None,
        cleanup: terminal.then(clean_cleanup),
        terminal,
        redaction: RedactionStatus::Applied,
    }
}

fn completed_event(
    transfer_id: &str,
    operation: AgentTransferOperation,
    total: u64,
    chunks: u32,
    etag: Option<String>,
) -> AgentTransferEvent {
    let mut event = progress_event(
        transfer_id,
        operation,
        AgentTransferPhase::Completed,
        u64::from(chunks) + 2,
        total,
        total,
        chunks.max(1),
        chunks.max(1),
        None,
        None,
        true,
    );
    event.checksum = etag
        .filter(|value| !value.is_empty())
        .map(|value| AgentTransferChecksum {
            algorithm: AgentTransferChecksumAlgorithm::Etag,
            scope: AgentTransferChecksumScope::Object,
            value,
            verified: true,
        });
    event
}

fn cancelled_event(
    transfer_id: &str,
    operation: AgentTransferOperation,
    completed: u64,
    total: u64,
    checkpoint: AgentTransferResumeCheckpoint,
    remote_partial: AgentTransferCleanupState,
) -> AgentTransferEvent {
    let chunks = checkpoint.completed_chunks;
    let mut event = progress_event(
        transfer_id,
        operation,
        AgentTransferPhase::Cancelled,
        u64::from(chunks) + 2,
        completed,
        total,
        chunks,
        chunk_count(total as usize, DEFAULT_CHUNK_BYTES)
            .max(chunks)
            .max(1),
        None,
        Some(checkpoint),
        true,
    );
    event.cleanup = Some(AgentTransferCleanupReport {
        local_staging: AgentTransferCleanupState::NotApplicable,
        remote_partial,
        remote_lock: AgentTransferCleanupState::NotApplicable,
    });
    event
}

fn clean_cleanup() -> AgentTransferCleanupReport {
    AgentTransferCleanupReport {
        local_staging: AgentTransferCleanupState::NotApplicable,
        remote_partial: AgentTransferCleanupState::NotApplicable,
        remote_lock: AgentTransferCleanupState::NotApplicable,
    }
}

fn checkpoint(
    token: &str,
    scope: &str,
    completed_bytes: u64,
    completed_chunks: u32,
) -> AgentTransferResumeCheckpoint {
    AgentTransferResumeCheckpoint {
        token: token.to_string(),
        scope: scope.to_string(),
        completed_bytes,
        completed_chunks,
        expires_at: Utc::now() + Duration::seconds(RESUME_TTL_SECONDS),
    }
}

fn chunk_bytes(input: &Value) -> Result<usize, PluginSessionError> {
    let value = input
        .get("chunk_bytes")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_CHUNK_BYTES as u64);
    let value = usize::try_from(value).map_err(|_| {
        error(
            PluginSessionErrorCode::PolicyDenied,
            "S3 transfer chunk size is invalid.",
        )
    })?;
    if !(MIN_MULTIPART_BYTES..=MAX_CHUNK_BYTES).contains(&value) {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "S3 transfer chunk size is outside the bounded multipart range.",
        ));
    }
    Ok(value)
}

fn chunk_count(length: usize, chunk_bytes: usize) -> u32 {
    if length == 0 {
        1
    } else {
        u32::try_from(length.div_ceil(chunk_bytes)).unwrap_or(u32::MAX)
    }
}

fn completed_chunks(length: usize, chunk_bytes: usize) -> u32 {
    u32::try_from(length / chunk_bytes).unwrap_or(u32::MAX)
}

fn enforce_transfer_bound(length: usize) -> Result<(), PluginSessionError> {
    if length > MAX_TRANSFER_BYTES {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "Agent S3 transfers are bounded to 128 MiB per session operation.",
        ));
    }
    Ok(())
}

fn required_string(input: &Value, field: &str) -> Result<String, PluginSessionError> {
    input
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            error(
                PluginSessionErrorCode::PolicyDenied,
                format!("S3 transfer requires {field}."),
            )
        })
}

fn optional_string(input: &Value, field: &str) -> Result<Option<String>, PluginSessionError> {
    input
        .get(field)
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty() && !value.chars().any(char::is_control))
                .map(str::to_string)
                .ok_or_else(|| {
                    error(
                        PluginSessionErrorCode::PolicyDenied,
                        format!("S3 transfer field {field} is invalid."),
                    )
                })
        })
        .transpose()
}

fn expected_sha256(input: &Value) -> Result<Option<String>, PluginSessionError> {
    optional_string(input, "expected_sha256")?
        .map(|value| {
            if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                Ok(value.to_ascii_lowercase())
            } else {
                Err(error(
                    PluginSessionErrorCode::PolicyDenied,
                    "S3 expected_sha256 must contain exactly 64 hexadecimal characters.",
                ))
            }
        })
        .transpose()
}

fn verify_sha256(data: &[u8], expected: &str) -> Result<AgentTransferChecksum, PluginSessionError> {
    let actual = hex::encode(Sha256::digest(data));
    if actual != expected {
        return Err(error(
            PluginSessionErrorCode::HealthFailed,
            "S3 SHA-256 verification failed; the transfer was not reported as successful.",
        ));
    }
    Ok(AgentTransferChecksum {
        algorithm: AgentTransferChecksumAlgorithm::Sha256,
        scope: AgentTransferChecksumScope::WholeTransfer,
        value: actual,
        verified: true,
    })
}

fn sha256_scope(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn resume_token_for(transfer_id: &str, scope: &str) -> String {
    let fingerprint = sha256_scope(&[transfer_id, scope]);
    format!("s3-resume-{}", &fingerprint[7..39])
}

fn local_error(_error: voidb_core::LocalPathError) -> PluginSessionError {
    error(
        PluginSessionErrorCode::PolicyDenied,
        "The local path is outside the approved scope, changed, linked, or conflicts with an existing destination.",
    )
}

fn target_error(_error: impl std::fmt::Display) -> PluginSessionError {
    target_error_message("The S3 target operation failed; target details were withheld.")
}

fn target_error_message(message: &str) -> PluginSessionError {
    error(PluginSessionErrorCode::OwnerUnavailable, message)
}

fn cancelled() -> PluginSessionError {
    error(
        PluginSessionErrorCode::Cancelled,
        "The S3 transfer was cancelled.",
    )
}

fn error(code: PluginSessionErrorCode, message: impl Into<String>) -> PluginSessionError {
    PluginSessionError::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_tokens_are_opaque_and_scope_bound() {
        let left = resume_token_for(
            "call-1",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        let right = resume_token_for(
            "call-1",
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        );
        assert_ne!(left, right);
        assert!(!left.contains("sha256"));
    }

    #[test]
    fn chunk_count_is_bounded_and_never_zero() {
        assert_eq!(chunk_count(0, DEFAULT_CHUNK_BYTES), 1);
        assert_eq!(chunk_count(DEFAULT_CHUNK_BYTES + 1, DEFAULT_CHUNK_BYTES), 2);
    }

    #[test]
    fn expected_sha256_detects_mismatch_without_exposing_payload() {
        let expected = hex::encode(Sha256::digest(b"expected"));
        let failure = verify_sha256(b"changed", &expected).unwrap_err();
        assert_eq!(failure.code, PluginSessionErrorCode::HealthFailed);
        assert!(!failure.message.contains("expected"));
        assert!(!failure.message.contains("changed"));
    }

    #[tokio::test]
    async fn interrupted_multipart_retains_a_scoped_retry_checkpoint() {
        let session = S3AgentSession {
            config: S3Config::default(),
            binding_scope:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            state: Mutex::new(S3TransferState::default()),
            cancel: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        };
        let total = DEFAULT_CHUNK_BYTES * 2;
        session
            .set_event(progress_event(
                "transfer-interrupted",
                AgentTransferOperation::Upload,
                AgentTransferPhase::Transferring,
                1,
                0,
                total as u64,
                0,
                2,
                None,
                None,
                false,
            ))
            .await
            .unwrap();
        let failure = session
            .retain_upload_failure(
                "transfer-interrupted",
                "opaque-resume-token".into(),
                session.binding_scope.clone(),
                "fixture-bucket".into(),
                "fixture-key".into(),
                vec![0; total],
                "opaque-upload-id".into(),
                Vec::new(),
                0,
                DEFAULT_CHUNK_BYTES,
            )
            .await;
        assert_eq!(failure.code, PluginSessionErrorCode::OwnerUnavailable);

        let state = session.state.lock().await;
        assert!(matches!(state.resume, Some(S3ResumeState::Upload { .. })));
        let event = state.event.as_ref().unwrap();
        assert_eq!(event.phase, AgentTransferPhase::RetryWaiting);
        assert!(event.checkpoint.is_some());
        assert_eq!(
            event.retry.as_ref().map(|retry| retry.reason_code.as_str()),
            Some("target_unavailable")
        );
        assert_eq!(
            event.cleanup.as_ref().map(|cleanup| cleanup.remote_partial),
            Some(AgentTransferCleanupState::RetainedForResume)
        );
    }

    #[tokio::test]
    async fn session_lifecycle_settles_cancellation_and_rejects_terminal_escape() {
        let session = S3AgentSession {
            config: S3Config::default(),
            binding_scope:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            state: Mutex::new(S3TransferState::default()),
            cancel: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        };
        session
            .set_event(progress_event(
                "transfer-1",
                AgentTransferOperation::Download,
                AgentTransferPhase::Transferring,
                1,
                0,
                10,
                0,
                1,
                None,
                None,
                false,
            ))
            .await
            .expect("initial event");
        session
            .cancel_without_resume("transfer-1", AgentTransferOperation::Download, 10)
            .await
            .expect("settled cancellation");
        let terminal = session.state.lock().await.event.clone().unwrap();
        assert_eq!(terminal.phase, AgentTransferPhase::Cancelled);
        assert!(terminal.terminal);
        assert_eq!(terminal.progress.objects_completed, 0);

        let failure = session
            .set_event(completed_event(
                "transfer-1",
                AgentTransferOperation::Download,
                10,
                1,
                None,
            ))
            .await
            .unwrap_err();
        assert_eq!(failure.code, PluginSessionErrorCode::HealthFailed);
    }
}
