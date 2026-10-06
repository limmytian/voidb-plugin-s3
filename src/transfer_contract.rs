//! Driver-free mapping from S3 semantics to the shared transfer lifecycle.

use voidb_core::{
    AGENT_TRANSFER_PROTOCOL_VERSION, AgentTransferChecksumAlgorithm, AgentTransferChunkMode,
    AgentTransferChunkPolicy, AgentTransferCleanupAction, AgentTransferCleanupPolicy,
    AgentTransferConflictPolicy, AgentTransferContract, AgentTransferLocalAccess,
    AgentTransferLocalPathPolicy, AgentTransferOperation, AgentTransferPrecondition,
    AgentTransferResumeMode, AgentTransferResumePolicy, AgentTransferRetryPolicy,
    MAX_AGENT_TRANSFER_CHUNKS,
};

/// Required S3 transfer behavior shared by future Agent sessions, CLI streams,
/// and the plugin-owned TUI.
///
/// Returning this descriptor does not advertise an executable capability. The
/// capability expansion slice must attach it only after the S3 service owns
/// multipart state, cancellation, resume binding, and bounded cleanup.
pub fn s3_transfer_contract() -> AgentTransferContract {
    AgentTransferContract {
        protocol_version: AGENT_TRANSFER_PROTOCOL_VERSION,
        target_kind: "s3_object".into(),
        operations: vec![
            AgentTransferOperation::Upload,
            AgentTransferOperation::Download,
            AgentTransferOperation::Copy,
            AgentTransferOperation::Move,
        ],
        chunking: AgentTransferChunkPolicy {
            modes: vec![
                AgentTransferChunkMode::Single,
                AgentTransferChunkMode::ByteRange,
                AgentTransferChunkMode::Multipart,
            ],
            max_chunks: MAX_AGENT_TRANSFER_CHUNKS,
            max_parallel_chunks: 16,
        },
        checksums: vec![
            AgentTransferChecksumAlgorithm::Sha256,
            AgentTransferChecksumAlgorithm::Md5,
            AgentTransferChecksumAlgorithm::Etag,
        ],
        conflicts: vec![
            AgentTransferConflictPolicy::Fail,
            AgentTransferConflictPolicy::Skip,
            AgentTransferConflictPolicy::Replace,
        ],
        preconditions: vec![
            AgentTransferPrecondition::SourceMatch,
            AgentTransferPrecondition::DestinationAbsent,
            AgentTransferPrecondition::DestinationMatch,
        ],
        retry: AgentTransferRetryPolicy {
            max_retries: 5,
            initial_backoff_ms: 250,
            max_backoff_ms: 30_000,
        },
        resume: AgentTransferResumePolicy {
            mode: AgentTransferResumeMode::Exact,
            max_token_bytes: 16 * 1024,
            token_ttl_seconds: 24 * 60 * 60,
            require_scope_fingerprint: true,
        },
        local_path: AgentTransferLocalPathPolicy {
            access: vec![
                AgentTransferLocalAccess::ReadFile,
                AgentTransferLocalAccess::CreateFile,
                AgentTransferLocalAccess::ReplaceFile,
                AgentTransferLocalAccess::ResumeTransfer,
            ],
            disclose_absolute_paths: false,
        },
        cleanup: AgentTransferCleanupPolicy {
            on_cancel: vec![
                AgentTransferCleanupAction::RemoveLocalStaging,
                AgentTransferCleanupAction::AbortRemotePartial,
            ],
            on_failure: vec![
                AgentTransferCleanupAction::RemoveLocalStaging,
                AgentTransferCleanupAction::RetainBoundCheckpoint,
            ],
            timeout_ms: 30_000,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_s3_to_the_shared_transfer_contract() {
        let contract = s3_transfer_contract();
        contract.validate().expect("valid S3 transfer contract");
        assert!(
            contract
                .chunking
                .modes
                .contains(&AgentTransferChunkMode::Multipart)
        );
        assert_eq!(contract.resume.mode, AgentTransferResumeMode::Exact);
        assert!(
            contract
                .cleanup
                .on_cancel
                .contains(&AgentTransferCleanupAction::AbortRemotePartial)
        );
        assert!(!contract.local_path.disclose_absolute_paths);
    }
}
