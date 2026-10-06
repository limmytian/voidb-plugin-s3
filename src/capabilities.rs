#![allow(clippy::result_large_err)]

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Value, json};
use voidb_core::{
    CapabilityDefinition, CapabilityError, CapabilityErrorCategory, CapabilityInvocation,
    CapabilityInvocationResult, CapabilityRiskLevel, CredentialClass, InvocationOutputPage,
    InvocationStatus, LocalPathError, LocalPathScope, LocalScanLimits, LocalScanReport,
    RedactionStatus, TargetSystemFailure,
};

use crate::config::{S3Auth, S3Config, S3Provider};
use crate::service::S3Service;
use crate::types::{S3Entry, S3EntryType, SyncAction, SyncMode, SyncOptions, SyncPlan};

const PLUGIN_ID: &str = "s3";
const DEFAULT_PAGE_LIMIT: usize = 100;
const MAX_PAGE_LIMIT: usize = 500;
const DEFAULT_CONTENT_LIMIT_BYTES: usize = 64 * 1024;
const MAX_CONTENT_LIMIT_BYTES: usize = 1024 * 1024;
const DEFAULT_PRESIGN_SECONDS: u64 = 900;
const MAX_PRESIGN_SECONDS: u64 = 7 * 24 * 60 * 60;

pub fn s3_capabilities() -> Vec<CapabilityDefinition> {
    let mut capabilities = vec![
        capability(
            "buckets",
            "Discover accessible S3 buckets with provider-returned metadata.",
            json!({ "type": "object", "additionalProperties": false }),
            json!({
                "type": "object",
                "required": ["provider", "buckets", "bucket_count"],
                "properties": {
                    "provider": { "type": "string" },
                    "buckets": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["name", "creation_date"],
                            "properties": {
                                "name": { "type": "string" },
                                "creation_date": { "type": ["string", "null"] }
                            },
                            "additionalProperties": false
                        }
                    },
                    "bucket_count": { "type": "integer", "minimum": 0 }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "s3.buckets"],
            false,
            false,
            Some(30_000),
        ),
        capability(
            "list",
            "List objects and common prefixes in an S3 bucket prefix.",
            json!({
                "type": "object",
                "required": ["bucket"],
                "properties": {
                    "bucket": { "type": "string", "minLength": 1 },
                    "prefix": {
                        "type": "string",
                        "default": "",
                        "description": "Object prefix to list."
                    },
                    "delimiter": {
                        "type": ["string", "null"],
                        "default": "/",
                        "description": "Use '/' for one-level browsing or null/empty for a flat page."
                    }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["bucket", "prefix", "delimiter", "entries", "entry_count", "limit", "truncated"],
                "properties": {
                    "bucket": { "type": "string" },
                    "prefix": { "type": "string" },
                    "delimiter": { "type": ["string", "null"] },
                    "entries": { "type": "array", "items": entry_schema() },
                    "entry_count": { "type": "integer", "minimum": 0 },
                    "limit": { "type": "integer", "minimum": 1, "maximum": MAX_PAGE_LIMIT },
                    "cursor": { "type": ["string", "null"] },
                    "next_cursor": { "type": ["string", "null"] },
                    "truncated": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "s3.list"],
            false,
            false,
            Some(30_000),
        ),
        capability(
            "stat",
            "Read metadata for one S3 object.",
            json!({
                "type": "object",
                "required": ["bucket", "key"],
                "properties": {
                    "bucket": { "type": "string", "minLength": 1 },
                    "key": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["bucket", "entry"],
                "properties": {
                    "bucket": { "type": "string" },
                    "entry": entry_schema()
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "s3.stat"],
            false,
            false,
            Some(30_000),
        ),
        capability(
            "get",
            "Fetch one S3 object as bounded base64 content.",
            json!({
                "type": "object",
                "required": ["bucket", "key"],
                "properties": {
                    "bucket": { "type": "string", "minLength": 1 },
                    "key": { "type": "string", "minLength": 1 },
                    "max_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_CONTENT_LIMIT_BYTES,
                        "default": DEFAULT_CONTENT_LIMIT_BYTES
                    }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": [
                    "bucket",
                    "key",
                    "content_base64",
                    "bytes_returned",
                    "content_truncated",
                    "byte_limit"
                ],
                "properties": {
                    "bucket": { "type": "string" },
                    "key": { "type": "string" },
                    "content_base64": { "type": "string" },
                    "bytes_returned": { "type": "integer", "minimum": 0 },
                    "content_truncated": { "type": "boolean" },
                    "byte_limit": { "type": "integer", "minimum": 1, "maximum": MAX_CONTENT_LIMIT_BYTES }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "s3.get"],
            false,
            false,
            Some(30_000),
        ),
        capability(
            "put",
            "Upload one S3 object from inline bounded content.",
            json!({
                "type": "object",
                "required": ["bucket", "key"],
                "oneOf": [
                    {
                        "required": ["content_base64"],
                        "not": { "required": ["content_text"] }
                    },
                    {
                        "required": ["content_text"],
                        "not": { "required": ["content_base64"] }
                    }
                ],
                "properties": {
                    "bucket": { "type": "string", "minLength": 1 },
                    "key": { "type": "string", "minLength": 1 },
                    "content_base64": { "type": "string" },
                    "content_text": { "type": "string" }
                },
                "additionalProperties": false
            }),
            write_output_schema(),
            vec!["connection.write", "s3.put"],
            true,
            true,
            Some(30_000),
        ),
        capability(
            "delete",
            "Delete one S3 object.",
            json!({
                "type": "object",
                "required": ["bucket", "key"],
                "properties": {
                    "bucket": { "type": "string", "minLength": 1 },
                    "key": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            write_output_schema(),
            vec!["connection.write", "s3.delete"],
            true,
            true,
            Some(30_000),
        ),
        capability(
            "mkdir",
            "Create a virtual S3 directory placeholder object.",
            json!({
                "type": "object",
                "required": ["bucket", "prefix"],
                "properties": {
                    "bucket": { "type": "string", "minLength": 1 },
                    "prefix": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            write_output_schema(),
            vec!["connection.write", "s3.mkdir"],
            true,
            true,
            Some(30_000),
        ),
        capability(
            "copy",
            "Copy one S3 object with bounded conflict handling and destination verification.",
            copy_move_input_schema(),
            write_output_schema(),
            vec!["connection.write", "s3.copy"],
            true,
            true,
            Some(60_000),
        ),
        capability(
            "move",
            "Move one S3 object by verified copy and source identity recheck before deletion.",
            copy_move_input_schema(),
            write_output_schema(),
            vec!["connection.write", "s3.move"],
            true,
            true,
            Some(60_000),
        ),
        capability(
            "presign",
            "Create a short-lived, explicitly authorized S3 GET or PUT delegation URL.",
            json!({
                "type": "object",
                "required": ["bucket", "key", "method"],
                "properties": {
                    "bucket": { "type": "string", "minLength": 1 },
                    "key": { "type": "string", "minLength": 1 },
                    "method": { "type": "string", "enum": ["get", "put"] },
                    "expires_seconds": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_PRESIGN_SECONDS,
                        "default": DEFAULT_PRESIGN_SECONDS
                    }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["bucket", "key", "method", "expires_seconds", "url"],
                "properties": {
                    "bucket": { "type": "string" },
                    "key": { "type": "string" },
                    "method": { "type": "string", "enum": ["get", "put"] },
                    "expires_seconds": { "type": "integer" },
                    "url": { "type": "string" }
                },
                "additionalProperties": false
            }),
            vec!["connection.delegate", "s3.presign"],
            true,
            false,
            Some(30_000),
        ),
        capability(
            "sync_plan",
            "Compute a local/S3 sync plan without applying changes.",
            json!({
                "type": "object",
                "required": ["bucket", "local_root", "local_path"],
                "properties": {
                    "bucket": { "type": "string", "minLength": 1 },
                    "remote_prefix": { "type": "string", "default": "" },
                    "local_root": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Human-approved absolute local root; never returned in output."
                    },
                    "local_path": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Directory to scan, relative to local_root."
                    },
                    "disclose_relative_paths": {
                        "type": "boolean",
                        "default": false,
                        "description": "Include approved root-relative names instead of opaque entry references only."
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["pull", "push", "sync"],
                        "default": "sync"
                    },
                    "delete_extra": { "type": "boolean", "default": false },
                    "exclude": {
                        "type": "array",
                        "items": { "type": "string" },
                        "default": []
                    }
                },
                "additionalProperties": false
            }),
            sync_plan_schema(),
            vec!["connection.read", "s3.sync_plan", "local.scan"],
            false,
            false,
            Some(60_000),
        ),
    ];
    capabilities.extend(s3_transfer_capabilities());
    capabilities
}

pub async fn invoke_s3_capability(
    config: &S3Config,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    if invocation.plugin_id != PLUGIN_ID {
        return Err(validation_error(
            "validation.plugin_mismatch",
            "Invocation plugin_id does not match S3.",
            json!({ "expected": PLUGIN_ID, "actual": invocation.plugin_id }),
        ));
    }

    match invocation.capability_id.as_str() {
        "buckets" => invoke_buckets(config, invocation).await,
        "list" => invoke_list(config, invocation).await,
        "stat" => invoke_stat(config, invocation).await,
        "get" => invoke_get(config, invocation).await,
        "put" => invoke_put(config, invocation).await,
        "delete" => invoke_delete(config, invocation).await,
        "mkdir" => invoke_mkdir(config, invocation).await,
        "copy" => invoke_copy(config, invocation).await,
        "move" => invoke_move(config, invocation).await,
        "presign" => invoke_presign(config, invocation).await,
        "sync_plan" => invoke_sync_plan(config, invocation).await,
        "transfer" | "transfer_status" => Err(unavailable_error(
            "unavailable.session_required",
            "S3 transfer workflows require a persistent FileTransfer session.",
            json!({ "capability_id": invocation.capability_id }),
        )),
        other => Err(unavailable_error(
            "unavailable.capability_not_found",
            "S3 capability was not found.",
            json!({ "capability_id": other }),
        )),
    }
}

fn s3_transfer_capabilities() -> Vec<CapabilityDefinition> {
    let purpose = voidb_core::PluginSessionPurpose::FileTransfer;
    let family = ["s3.transfer", "s3.transfer_status"];
    vec![
        CapabilityDefinition {
            plugin_id: PLUGIN_ID.to_string(),
            id: "transfer".to_string(),
            description: "Run a cancellable, resumable S3 upload, download, verified copy, or safe move inside a plugin-owned session.".to_string(),
            input_schema: json!({
                "type": "object",
                "required": ["operation", "bucket", "key"],
                "properties": {
                    "operation": { "type": "string", "enum": ["upload", "download", "copy", "move"] },
                    "bucket": { "type": "string", "minLength": 1 },
                    "key": { "type": "string", "minLength": 1 },
                    "destination_bucket": { "type": "string", "minLength": 1 },
                    "destination_key": { "type": "string", "minLength": 1 },
                    "local_root": { "type": "string", "minLength": 1 },
                    "local_path": { "type": "string", "minLength": 1 },
                    "replace": { "type": "boolean", "default": false },
                    "expected_sha256": {
                        "type": "string",
                        "pattern": "^[A-Fa-f0-9]{64}$",
                        "description": "Optional whole-object SHA-256 required before local commit or reported success."
                    },
                    "resume_token": { "type": "string", "minLength": 1 },
                    "chunk_bytes": { "type": "integer", "minimum": 5242880, "maximum": 67108864 }
                },
                "additionalProperties": false
            }),
            output_schema: transfer_output_schema(false),
            permissions: vec![
                "connection.read".to_string(),
                "connection.write".to_string(),
                "local.read".to_string(),
                "local.write".to_string(),
                "s3.transfer".to_string(),
            ],
            authorization: s3_transfer_authorization(),
            risk: CapabilityRiskLevel::Destructive,
            destructive: true,
            streaming: true,
            execution_mode: voidb_core::CapabilityExecutionMode::SessionOnly,
            session_handoff: Some(voidb_core::CapabilitySessionHandoff::new(
                purpose.clone(),
                family,
            )),
            connection_required: true,
            required_secret_classes: Vec::new(),
            supports_dry_run: false,
            default_timeout_ms: Some(15 * 60 * 1_000),
        },
        CapabilityDefinition {
            plugin_id: PLUGIN_ID.to_string(),
            id: "transfer_status".to_string(),
            description: "Read the latest bounded S3 transfer lifecycle event without blocking the transfer.".to_string(),
            input_schema: json!({ "type": "object", "additionalProperties": false }),
            output_schema: transfer_output_schema(true),
            permissions: vec!["connection.read".to_string(), "s3.transfer_status".to_string()],
            authorization: voidb_core::CapabilityAuthorizationMetadata::declared()
                .with_session_purposes(vec![purpose.clone()])
                .with_note("Status exposes only the redacted shared transfer lifecycle."),
            risk: CapabilityRiskLevel::ReadOnly,
            destructive: false,
            streaming: true,
            execution_mode: voidb_core::CapabilityExecutionMode::SessionOnly,
            session_handoff: Some(voidb_core::CapabilitySessionHandoff::new(purpose, family)),
            connection_required: true,
            required_secret_classes: Vec::new(),
            supports_dry_run: false,
            default_timeout_ms: Some(5_000),
        },
    ]
}

fn transfer_output_schema(status: bool) -> Value {
    let mut event = voidb_core::agent_transfer_event_schema();
    let definitions = event
        .as_object_mut()
        .and_then(|schema| schema.remove("$defs"))
        .unwrap_or_else(|| json!({}));
    if status {
        json!({
            "type": "object",
            "required": ["active", "event"],
            "properties": {
                "active": { "type": "boolean" },
                "event": { "anyOf": [event, { "type": "null" }] }
            },
            "$defs": definitions,
            "additionalProperties": false
        })
    } else {
        json!({
            "type": "object",
            "required": ["event"],
            "properties": { "event": event },
            "$defs": definitions,
            "additionalProperties": false
        })
    }
}

fn s3_transfer_authorization() -> voidb_core::CapabilityAuthorizationMetadata {
    let fields = vec![
        voidb_core::CapabilityApprovalField::new(
            "/operation",
            "Transfer operation",
            voidb_core::CapabilityApprovalValueType::String,
        )
        .required()
        .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
        voidb_core::CapabilityApprovalField::new(
            "/bucket",
            "Source bucket",
            voidb_core::CapabilityApprovalValueType::ResourceId,
        )
        .required(),
        voidb_core::CapabilityApprovalField::new(
            "/key",
            "Source object key",
            voidb_core::CapabilityApprovalValueType::Path,
        )
        .required(),
        voidb_core::CapabilityApprovalField::new(
            "/destination_bucket",
            "Destination bucket",
            voidb_core::CapabilityApprovalValueType::ResourceId,
        ),
        voidb_core::CapabilityApprovalField::new(
            "/destination_key",
            "Destination object key",
            voidb_core::CapabilityApprovalValueType::Path,
        )
        .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
        voidb_core::CapabilityApprovalField::new(
            "/local_root",
            "Approved local transfer root",
            voidb_core::CapabilityApprovalValueType::Path,
        )
        .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::PrivilegeEscalation),
        voidb_core::CapabilityApprovalField::new(
            "/local_path",
            "Relative local transfer path",
            voidb_core::CapabilityApprovalValueType::Path,
        ),
        voidb_core::CapabilityApprovalField::new(
            "/replace",
            "Replace destination",
            voidb_core::CapabilityApprovalValueType::Boolean,
        )
        .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
    ];
    voidb_core::CapabilityAuthorizationMetadata::declared()
        .with_interactive_execute()
        .with_session_purposes(vec![voidb_core::PluginSessionPurpose::FileTransfer])
        .with_note(
            "Bucket, key, local scope, overwrite, multipart, resume, and move constraints are revalidated inside the S3 session.",
        )
        .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(fields))
        .without_capability_wide()
}

#[allow(clippy::too_many_arguments)]
fn capability(
    id: &str,
    description: &str,
    input_schema: Value,
    output_schema: Value,
    permissions: Vec<&str>,
    destructive: bool,
    supports_dry_run: bool,
    default_timeout_ms: Option<u64>,
) -> CapabilityDefinition {
    CapabilityDefinition {
        plugin_id: PLUGIN_ID.to_string(),
        id: id.to_string(),
        description: description.to_string(),
        input_schema,
        output_schema,
        permissions: permissions.into_iter().map(str::to_string).collect(),
        authorization: s3_authorization_metadata(id),
        risk: CapabilityRiskLevel::from_destructive(destructive),
        destructive,
        streaming: false,
        execution_mode: voidb_core::CapabilityExecutionMode::Stateless,
        session_handoff: None,
        connection_required: true,
        required_secret_classes: Vec::<CredentialClass>::new(),
        supports_dry_run,
        default_timeout_ms,
    }
}

fn s3_authorization_metadata(id: &str) -> voidb_core::CapabilityAuthorizationMetadata {
    let path = match id {
        "list" | "mkdir" => Some(("/prefix", "Object prefix")),
        "stat" | "get" | "put" | "delete" => Some(("/key", "Object key or prefix")),
        "presign" => Some(("/key", "Delegated object key")),
        "sync_plan" => Some(("/remote_prefix", "Remote object prefix")),
        _ => None,
    };
    let mut fields = if id == "buckets" {
        Vec::new()
    } else if matches!(id, "copy" | "move") {
        vec![
            voidb_core::CapabilityApprovalField::new(
                "/source_bucket",
                "Source bucket",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required(),
            voidb_core::CapabilityApprovalField::new(
                "/source_key",
                "Source object key",
                voidb_core::CapabilityApprovalValueType::Path,
            )
            .required(),
            voidb_core::CapabilityApprovalField::new(
                "/destination_bucket",
                "Destination bucket",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required(),
            voidb_core::CapabilityApprovalField::new(
                "/destination_key",
                "Destination object key",
                voidb_core::CapabilityApprovalValueType::Path,
            )
            .required()
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
        ]
    } else {
        vec![
            voidb_core::CapabilityApprovalField::new(
                "/bucket",
                "Bucket",
                voidb_core::CapabilityApprovalValueType::ResourceId,
            )
            .required(),
        ]
    };
    if let Some((path, label)) = path {
        let mut field = voidb_core::CapabilityApprovalField::new(
            path,
            label,
            voidb_core::CapabilityApprovalValueType::Path,
        )
        .with_constraint(voidb_core::CapabilityConstraintKind::Prefix);
        if !matches!(id, "list" | "sync_plan") {
            field = field.required();
        }
        if matches!(id, "put" | "delete" | "mkdir" | "presign") {
            field =
                field.with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive);
        }
        fields.push(field);
    }
    if id == "sync_plan" {
        fields.extend([
            voidb_core::CapabilityApprovalField::new(
                "/local_root",
                "Approved local scan root",
                voidb_core::CapabilityApprovalValueType::Path,
            )
            .required()
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::PrivilegeEscalation),
            voidb_core::CapabilityApprovalField::new(
                "/local_path",
                "Relative local scan path",
                voidb_core::CapabilityApprovalValueType::Path,
            )
            .required(),
            voidb_core::CapabilityApprovalField::new(
                "/disclose_relative_paths",
                "Disclose relative local paths",
                voidb_core::CapabilityApprovalValueType::Boolean,
            )
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::PrivilegeEscalation),
        ]);
    }
    if matches!(id, "copy" | "move") {
        fields.push(
            voidb_core::CapabilityApprovalField::new(
                "/replace",
                "Replace destination",
                voidb_core::CapabilityApprovalValueType::Boolean,
            )
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
        );
    }
    if id == "presign" {
        fields.extend([
            voidb_core::CapabilityApprovalField::new(
                "/method",
                "Delegated HTTP method",
                voidb_core::CapabilityApprovalValueType::String,
            )
            .required()
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::PrivilegeEscalation),
            voidb_core::CapabilityApprovalField::new(
                "/expires_seconds",
                "Delegation lifetime",
                voidb_core::CapabilityApprovalValueType::Integer,
            )
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::PrivilegeEscalation),
        ]);
    }
    let metadata = voidb_core::CapabilityAuthorizationMetadata::declared()
        .with_note(
            "Bucket and object-prefix constraints are revalidated; multipart work requires the separate FileTransfer session family.",
        )
        .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(fields));
    if id == "sync_plan" {
        metadata.without_capability_wide()
    } else {
        metadata
    }
}

async fn invoke_buckets(
    config: &S3Config,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let service = S3Service::new_direct(config.clone());
    let buckets = service
        .discover_buckets()
        .await
        .map_err(|error| discovery_error(config, error))?
        .into_iter()
        .map(|bucket| {
            json!({
                "name": bucket.name,
                "creation_date": bucket.creation_date,
            })
        })
        .collect::<Vec<_>>();
    let bucket_count = buckets.len();
    let output = json!({
        "provider": config.provider.label(),
        "buckets": buckets,
        "bucket_count": bucket_count,
    });
    Ok(result(
        invocation.id,
        output,
        json!({ "bucket_count": bucket_count, "provider": config.provider.label() }),
        None,
    ))
}

async fn invoke_list(
    config: &S3Config,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let bucket = required_string(&invocation.input, "bucket")?;
    let prefix = optional_string(&invocation.input, "prefix")?.unwrap_or_default();
    let page_request = page_request(&invocation)?;
    let delimiter = optional_string_allow_empty(&invocation.input, "delimiter")?
        .unwrap_or_else(|| "/".to_string());
    let delimiter = (!delimiter.is_empty()).then_some(delimiter);
    let service = S3Service::new_direct(config.clone());
    let page = service
        .list_objects_page(
            &bucket,
            &prefix,
            delimiter.as_deref(),
            page_request.cursor.clone(),
            page_request.limit,
        )
        .await
        .map_err(|e| target_error(config, "s3.list_failed", e))?;
    let output_entries = page
        .entries
        .into_iter()
        .map(entry_output)
        .collect::<Vec<_>>();
    let entry_count = output_entries.len();
    let next_cursor = page.next_continuation_token;
    let truncated = page.truncated || next_cursor.is_some();
    let output = json!({
        "bucket": bucket,
        "prefix": prefix,
        "delimiter": delimiter,
        "entries": output_entries,
        "entry_count": entry_count,
        "limit": page_request.limit,
        "cursor": page_request.cursor,
        "next_cursor": next_cursor,
        "truncated": truncated
    });
    let page = output["next_cursor"]
        .as_str()
        .map(|next_cursor| InvocationOutputPage {
            next_cursor: Some(next_cursor.to_string()),
        });
    let summary = json!({
        "entry_count": entry_count,
        "truncated": truncated,
        "next_cursor": output["next_cursor"]
    });

    Ok(result(invocation.id, output, summary, page))
}

async fn invoke_copy(
    config: &S3Config,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    invoke_copy_or_move(config, invocation, false).await
}

async fn invoke_move(
    config: &S3Config,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    invoke_copy_or_move(config, invocation, true).await
}

async fn invoke_copy_or_move(
    config: &S3Config,
    invocation: CapabilityInvocation,
    move_source: bool,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let source_bucket = required_string(&invocation.input, "source_bucket")?;
    let source_key = required_string(&invocation.input, "source_key")?;
    let destination_bucket = required_string(&invocation.input, "destination_bucket")?;
    let destination_key = required_string(&invocation.input, "destination_key")?;
    let replace = optional_bool(&invocation.input, "replace")?.unwrap_or(false);
    let operation = if move_source { "move" } else { "copy" };
    let details = json!({
        "source_bucket": source_bucket,
        "source_key": source_key,
        "destination_bucket": destination_bucket,
        "destination_key": destination_key,
        "replace": replace,
    });
    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, operation, details));
    }
    let service = S3Service::new_direct(config.clone());
    let entry = if move_source {
        service
            .move_object_verified(
                &source_bucket,
                &source_key,
                &destination_bucket,
                &destination_key,
                replace,
            )
            .await
    } else {
        service
            .copy_object_verified(
                &source_bucket,
                &source_key,
                &destination_bucket,
                &destination_key,
                replace,
            )
            .await
    }
    .map_err(|error| target_error(config, &format!("s3.{operation}_failed"), error))?;
    Ok(write_result(
        invocation.id,
        operation,
        json!({
            "source_bucket": source_bucket,
            "source_key": source_key,
            "destination_bucket": destination_bucket,
            "destination_key": destination_key,
            "replace": replace,
            "verified_size": entry.size,
            "verified_etag": entry.etag,
        }),
    ))
}

async fn invoke_presign(
    config: &S3Config,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let bucket = required_string(&invocation.input, "bucket")?;
    let key = required_string(&invocation.input, "key")?;
    let method = required_string(&invocation.input, "method")?;
    if !matches!(method.as_str(), "get" | "put") {
        return Err(validation_error(
            "validation.presign_method_invalid",
            "Presigned method must be get or put.",
            json!({ "method": method }),
        ));
    }
    let expires_seconds =
        optional_u64(&invocation.input, "expires_seconds")?.unwrap_or(DEFAULT_PRESIGN_SECONDS);
    if expires_seconds == 0 || expires_seconds > MAX_PRESIGN_SECONDS {
        return Err(validation_error(
            "validation.presign_expiry_invalid",
            "Presigned URL lifetime is outside the allowed bound.",
            json!({ "expires_seconds": expires_seconds, "maximum": MAX_PRESIGN_SECONDS }),
        ));
    }
    let expires = u32::try_from(expires_seconds).map_err(|_| {
        validation_error(
            "validation.presign_expiry_invalid",
            "Presigned URL lifetime is outside the platform bound.",
            json!({ "expires_seconds": expires_seconds }),
        )
    })?;
    let service = S3Service::new_direct(config.clone());
    let url = if method == "put" {
        service.presign_put(&bucket, &key, expires).await
    } else {
        service.presign_get(&bucket, &key, expires).await
    }
    .map_err(|error| target_error(config, "s3.presign_failed", error))?;
    let output = json!({
        "bucket": bucket,
        "key": key,
        "method": method,
        "expires_seconds": expires_seconds,
        "url": url,
    });
    Ok(result(
        invocation.id,
        output,
        json!({
            "bucket": bucket,
            "key": key,
            "method": method,
            "expires_seconds": expires_seconds,
        }),
        None,
    ))
}

async fn invoke_stat(
    config: &S3Config,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let bucket = required_string(&invocation.input, "bucket")?;
    let key = required_string(&invocation.input, "key")?;
    let service = S3Service::new_direct(config.clone());
    let entry = service
        .get_object_info(&bucket, &key)
        .await
        .map_err(|e| target_error(config, "s3.stat_failed", e))?;
    let output = json!({
        "bucket": bucket,
        "entry": entry_output(entry)
    });
    let summary = json!({
        "bucket": output["bucket"],
        "key": output["entry"]["key"],
        "entry_type": output["entry"]["entry_type"],
        "size": output["entry"]["size"]
    });

    Ok(result(invocation.id, output, summary, None))
}

async fn invoke_get(
    config: &S3Config,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let bucket = required_string(&invocation.input, "bucket")?;
    let key = required_string(&invocation.input, "key")?;
    let byte_limit = content_limit(&invocation.input)?;
    let service = S3Service::new_direct(config.clone());
    let bytes = service
        .download(&bucket, &key)
        .await
        .map_err(|e| target_error(config, "s3.get_failed", e))?;
    let content_truncated = bytes.len() > byte_limit;
    let returned = &bytes[..bytes.len().min(byte_limit)];
    let output = json!({
        "bucket": bucket,
        "key": key,
        "content_base64": BASE64.encode(returned),
        "bytes_returned": returned.len(),
        "content_truncated": content_truncated,
        "byte_limit": byte_limit
    });
    let summary = json!({
        "bucket": output["bucket"],
        "key": output["key"],
        "bytes_returned": output["bytes_returned"],
        "content_truncated": content_truncated
    });

    Ok(result(invocation.id, output, summary, None))
}

async fn invoke_put(
    config: &S3Config,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let bucket = required_string(&invocation.input, "bucket")?;
    let key = required_string(&invocation.input, "key")?;
    let content = content_bytes(&invocation.input)?;
    let details = json!({
        "bucket": bucket,
        "key": key,
        "bytes": content.len()
    });
    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "put", details));
    }

    let service = S3Service::new_direct(config.clone());
    service
        .upload(&bucket, &key, &content)
        .await
        .map_err(|e| target_error(config, "s3.put_failed", e))?;

    Ok(write_result(invocation.id, "put", details))
}

async fn invoke_delete(
    config: &S3Config,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let bucket = required_string(&invocation.input, "bucket")?;
    let key = required_string(&invocation.input, "key")?;
    let details = json!({ "bucket": bucket, "key": key });
    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "delete", details));
    }

    let service = S3Service::new_direct(config.clone());
    service
        .delete_object(&bucket, &key)
        .await
        .map_err(|e| target_error(config, "s3.delete_failed", e))?;

    Ok(write_result(invocation.id, "delete", details))
}

async fn invoke_mkdir(
    config: &S3Config,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let bucket = required_string(&invocation.input, "bucket")?;
    let prefix = required_string(&invocation.input, "prefix")?;
    let key = directory_placeholder_key(&prefix)?;
    let details = json!({ "bucket": bucket, "prefix": prefix, "key": key });
    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "mkdir", details));
    }

    let service = S3Service::new_direct(config.clone());
    service
        .upload(&bucket, &key, &[])
        .await
        .map_err(|e| target_error(config, "s3.mkdir_failed", e))?;

    Ok(write_result(invocation.id, "mkdir", details))
}

async fn invoke_sync_plan(
    config: &S3Config,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let bucket = required_string(&invocation.input, "bucket")?;
    let remote_prefix = optional_string(&invocation.input, "remote_prefix")?.unwrap_or_default();
    let local_root = required_string(&invocation.input, "local_root")?;
    let local_path = required_string(&invocation.input, "local_path")?;
    let disclose_relative_paths =
        optional_bool(&invocation.input, "disclose_relative_paths")?.unwrap_or(false);
    let local_scope_id = format!("local-scope:{}", invocation.id);
    let local_scope = LocalPathScope::new(&local_root)
        .map_err(|error| local_path_error(&local_scope_id, error))?;
    let (local_entries, scan_report) = crate::sync_ops::walk_local_tree_scoped(
        &local_scope,
        &local_path,
        LocalScanLimits::default(),
    )
    .map_err(|error| local_path_error(&local_scope_id, error))?;
    let options = SyncOptions {
        mode: sync_mode(&invocation.input)?,
        delete_extra: optional_bool(&invocation.input, "delete_extra")?.unwrap_or(false),
        dry_run: true,
        exclude: optional_string_array(&invocation.input, "exclude")?.unwrap_or_default(),
    };

    let service = S3Service::new_direct(config.clone());
    let remote_entries = service
        .walk_remote_tree(&bucket, &remote_prefix)
        .await
        .map_err(|e| target_error(config, "s3.sync_plan_failed", e))?;
    let plan = crate::sync_ops::compute_sync_plan(&remote_entries, &local_entries, &options);
    let output = sync_plan_output(
        "s3",
        &bucket,
        Some(&remote_prefix),
        LocalPlanOutput {
            scope_id: &local_scope_id,
            scan_report: &scan_report,
            disclose_relative_paths,
        },
        &options,
        plan,
    );
    let summary = json!({
        "change_count": output["change_count"],
        "total_transfer_bytes": output["total_transfer_bytes"],
        "mode": output["mode"],
        "delete_extra": output["delete_extra"]
    });

    Ok(result(invocation.id, output, summary, None))
}

fn entry_schema() -> Value {
    json!({
        "type": "object",
        "required": ["key", "display_name", "entry_type", "size"],
        "properties": {
            "key": { "type": "string" },
            "display_name": { "type": "string" },
            "entry_type": { "type": "string", "enum": ["object", "prefix"] },
            "size": { "type": "integer", "minimum": 0 },
            "last_modified": { "type": ["string", "null"] },
            "storage_class": { "type": ["string", "null"] },
            "etag": { "type": ["string", "null"] }
        },
        "additionalProperties": false
    })
}

fn copy_move_input_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "source_bucket", "source_key", "destination_bucket", "destination_key"
        ],
        "properties": {
            "source_bucket": { "type": "string", "minLength": 1 },
            "source_key": { "type": "string", "minLength": 1 },
            "destination_bucket": { "type": "string", "minLength": 1 },
            "destination_key": { "type": "string", "minLength": 1 },
            "replace": { "type": "boolean", "default": false }
        },
        "additionalProperties": false
    })
}

fn write_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["ok", "operation", "dry_run", "would_execute", "destructive", "details"],
        "properties": write_output_properties(),
        "additionalProperties": false
    })
}

fn write_output_properties() -> Value {
    json!({
        "ok": { "type": "boolean" },
        "operation": { "type": "string" },
        "dry_run": { "type": "boolean" },
        "would_execute": { "type": "boolean" },
        "destructive": { "type": "boolean" },
        "message": { "type": "string" },
        "details": { "type": "object" }
    })
}

fn sync_plan_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "target",
            "bucket",
            "remote_prefix",
            "local_scope_id",
            "scan",
            "mode",
            "delete_extra",
            "changes",
            "change_count",
            "total_transfer_bytes"
        ],
        "properties": {
            "target": { "type": "string" },
            "bucket": { "type": "string" },
            "remote_prefix": { "type": "string" },
            "local_scope_id": { "type": "string" },
            "scan": { "type": "object" },
            "mode": { "type": "string", "enum": ["pull", "push", "sync"] },
            "delete_extra": { "type": "boolean" },
            "exclude": { "type": "array", "items": { "type": "string" } },
            "changes": { "type": "array", "items": sync_change_schema() },
            "change_count": { "type": "integer", "minimum": 0 },
            "total_transfer_bytes": { "type": "integer", "minimum": 0 }
        },
        "additionalProperties": false
    })
}

fn sync_change_schema() -> Value {
    json!({
        "type": "object",
        "required": ["entry_ref", "action", "size", "is_dir"],
        "properties": {
            "entry_ref": { "type": "string" },
            "relative_path": { "type": "string" },
            "action": {
                "type": "string",
                "enum": ["download", "upload", "delete_local", "delete_remote", "conflict"]
            },
            "size": { "type": "integer", "minimum": 0 },
            "is_dir": { "type": "boolean" }
        },
        "additionalProperties": false
    })
}

fn entry_output(entry: S3Entry) -> Value {
    json!({
        "key": entry.key,
        "display_name": entry.display_name,
        "entry_type": match entry.entry_type {
            S3EntryType::Object => "object",
            S3EntryType::Prefix => "prefix",
        },
        "size": entry.size,
        "last_modified": entry.last_modified,
        "storage_class": entry.storage_class,
        "etag": entry.etag
    })
}

struct LocalPlanOutput<'a> {
    scope_id: &'a str,
    scan_report: &'a LocalScanReport,
    disclose_relative_paths: bool,
}

fn sync_plan_output(
    target: &str,
    bucket: &str,
    remote_prefix: Option<&str>,
    local: LocalPlanOutput<'_>,
    options: &SyncOptions,
    plan: SyncPlan,
) -> Value {
    let changes = plan
        .changes
        .into_iter()
        .enumerate()
        .map(|(index, change)| {
            let mut output = json!({
                "entry_ref": format!("{}:entry:{}", local.scope_id, index + 1),
                "action": sync_action_label(change.action),
                "size": change.size,
                "is_dir": change.is_dir
            });
            if local.disclose_relative_paths {
                output["relative_path"] = Value::String(change.rel_path);
            }
            output
        })
        .collect::<Vec<_>>();
    let change_count = changes.len();
    json!({
        "target": target,
        "bucket": bucket,
        "remote_prefix": remote_prefix.unwrap_or_default(),
        "local_scope_id": local.scope_id,
        "scan": scan_output(local.scan_report),
        "mode": sync_mode_label(options.mode),
        "delete_extra": options.delete_extra,
        "exclude": options.exclude,
        "changes": changes,
        "change_count": change_count,
        "total_transfer_bytes": plan.total_transfer_bytes
    })
}

fn scan_output(report: &LocalScanReport) -> Value {
    json!({
        "entry_count": report.entries.len(),
        "observed_depth": report.observed_depth,
        "observed_path_bytes": report.observed_path_bytes,
        "limits": {
            "max_depth": report.limits.max_depth,
            "max_entries": report.limits.max_entries,
            "max_path_bytes": report.limits.max_path_bytes,
        }
    })
}

fn sync_action_label(action: SyncAction) -> &'static str {
    match action {
        SyncAction::Download => "download",
        SyncAction::Upload => "upload",
        SyncAction::DeleteLocal => "delete_local",
        SyncAction::DeleteRemote => "delete_remote",
        SyncAction::Conflict => "conflict",
    }
}

fn sync_mode_label(mode: SyncMode) -> &'static str {
    match mode {
        SyncMode::Pull => "pull",
        SyncMode::Push => "push",
        SyncMode::Sync => "sync",
    }
}

fn sync_mode(input: &Value) -> Result<SyncMode, CapabilityError> {
    match optional_string(input, "mode")?.as_deref().unwrap_or("sync") {
        "pull" => Ok(SyncMode::Pull),
        "push" => Ok(SyncMode::Push),
        "sync" => Ok(SyncMode::Sync),
        other => Err(validation_error(
            "validation.sync_mode_invalid",
            "Sync mode must be pull, push, or sync.",
            json!({ "mode": other }),
        )),
    }
}

fn content_limit(input: &Value) -> Result<usize, CapabilityError> {
    let Some(max_bytes) = optional_u64(input, "max_bytes")? else {
        return Ok(DEFAULT_CONTENT_LIMIT_BYTES);
    };
    let max_bytes = usize::try_from(max_bytes).map_err(|_| {
        validation_error(
            "validation.max_bytes_invalid",
            "max_bytes is too large for this platform.",
            json!({ "maximum": MAX_CONTENT_LIMIT_BYTES }),
        )
    })?;
    if max_bytes == 0 || max_bytes > MAX_CONTENT_LIMIT_BYTES {
        return Err(validation_error(
            "validation.max_bytes_invalid",
            "max_bytes must be between 1 and the maximum content limit.",
            json!({ "max_bytes": max_bytes, "maximum": MAX_CONTENT_LIMIT_BYTES }),
        ));
    }
    Ok(max_bytes)
}

fn content_bytes(input: &Value) -> Result<Vec<u8>, CapabilityError> {
    let content_base64 = optional_string(input, "content_base64")?;
    let content_text = optional_string_allow_empty(input, "content_text")?;
    match (content_base64, content_text) {
        (Some(_), Some(_)) => Err(validation_error(
            "validation.content_source_conflict",
            "Provide either content_base64 or content_text, not both.",
            json!({}),
        )),
        (Some(value), None) => BASE64.decode(value).map_err(|error| {
            validation_error(
                "validation.content_base64_invalid",
                "content_base64 could not be decoded.",
                json!({ "message": error.to_string() }),
            )
        }),
        (None, Some(value)) => Ok(value.into_bytes()),
        (None, None) => Err(validation_error(
            "validation.content_source_missing",
            "Provide content_base64 or content_text.",
            json!({}),
        )),
    }
}

fn directory_placeholder_key(prefix: &str) -> Result<String, CapabilityError> {
    let trimmed = prefix.trim();
    if trimmed.is_empty() {
        return Err(validation_error(
            "validation.prefix_required",
            "Directory prefix cannot be empty.",
            json!({}),
        ));
    }
    Ok(if trimmed.ends_with('/') {
        trimmed.to_string()
    } else {
        format!("{}/", trimmed)
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PageRequest {
    limit: usize,
    cursor: Option<String>,
}

fn page_request(invocation: &CapabilityInvocation) -> Result<PageRequest, CapabilityError> {
    let limit = invocation
        .controls
        .page
        .as_ref()
        .map(|page| page.limit as usize)
        .unwrap_or(DEFAULT_PAGE_LIMIT);
    if limit == 0 || limit > MAX_PAGE_LIMIT {
        return Err(validation_error(
            "validation.page_limit_invalid",
            "S3 list page limit must be between 1 and the maximum page limit.",
            json!({ "limit": limit, "maximum": MAX_PAGE_LIMIT }),
        ));
    }

    let cursor = invocation
        .controls
        .page
        .as_ref()
        .and_then(|page| page.cursor.clone());
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.is_empty() || cursor.len() > 4096 || cursor.chars().any(char::is_control)
    }) {
        return Err(validation_error(
            "validation.invalid_cursor",
            "S3 continuation cursor is empty, oversized, or contains control characters.",
            json!({}),
        ));
    }
    Ok(PageRequest { limit, cursor })
}

fn required_string(input: &Value, field: &str) -> Result<String, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_string() => Err(validation_error(
            "validation.input_field_invalid",
            "Required input field must be a string.",
            json!({ "field": field }),
        )),
        Some(value) => value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                validation_error(
                    "validation.input_field_required",
                    "Required string input field is missing.",
                    json!({ "field": field }),
                )
            }),
        None => Err(validation_error(
            "validation.input_field_required",
            "Required string input field is missing.",
            json!({ "field": field }),
        )),
    }
}

fn optional_string(input: &Value, field: &str) -> Result<Option<String>, CapabilityError> {
    input
        .get(field)
        .filter(|value| !value.is_null())
        .map(|value| {
            value.as_str().map(str::to_string).ok_or_else(|| {
                validation_error(
                    "validation.input_field_invalid",
                    "Optional input field must be a string.",
                    json!({ "field": field }),
                )
            })
        })
        .transpose()
}

fn optional_string_allow_empty(
    input: &Value,
    field: &str,
) -> Result<Option<String>, CapabilityError> {
    input
        .get(field)
        .filter(|value| !value.is_null())
        .map(|value| {
            value.as_str().map(str::to_string).ok_or_else(|| {
                validation_error(
                    "validation.input_field_invalid",
                    "Optional input field must be a string.",
                    json!({ "field": field }),
                )
            })
        })
        .transpose()
}

fn optional_string_array(
    input: &Value,
    field: &str,
) -> Result<Option<Vec<String>>, CapabilityError> {
    input
        .get(field)
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_array()
                .ok_or_else(|| {
                    validation_error(
                        "validation.input_field_invalid",
                        "Optional input field must be an array of strings.",
                        json!({ "field": field }),
                    )
                })?
                .iter()
                .map(|item| {
                    item.as_str().map(str::to_string).ok_or_else(|| {
                        validation_error(
                            "validation.input_field_invalid",
                            "Optional array field must contain only strings.",
                            json!({ "field": field }),
                        )
                    })
                })
                .collect()
        })
        .transpose()
}

fn optional_bool(input: &Value, field: &str) -> Result<Option<bool>, CapabilityError> {
    input
        .get(field)
        .filter(|value| !value.is_null())
        .map(|value| {
            value.as_bool().ok_or_else(|| {
                validation_error(
                    "validation.input_field_invalid",
                    "Optional input field must be a boolean.",
                    json!({ "field": field }),
                )
            })
        })
        .transpose()
}

fn optional_u64(input: &Value, field: &str) -> Result<Option<u64>, CapabilityError> {
    input
        .get(field)
        .filter(|value| !value.is_null())
        .map(|value| {
            value.as_u64().ok_or_else(|| {
                validation_error(
                    "validation.input_field_invalid",
                    "Optional input field must be an unsigned integer.",
                    json!({ "field": field }),
                )
            })
        })
        .transpose()
}

fn dry_run_result(
    invocation_id: String,
    operation: &str,
    details: Value,
) -> CapabilityInvocationResult {
    result(
        invocation_id,
        json!({
            "ok": true,
            "dry_run": true,
            "would_execute": true,
            "destructive": true,
            "operation": operation,
            "message": "Dry-run completed; no S3 request was sent.",
            "details": details
        }),
        json!({ "dry_run": true, "operation": operation }),
        None,
    )
}

fn write_result(
    invocation_id: String,
    operation: &str,
    details: Value,
) -> CapabilityInvocationResult {
    result(
        invocation_id,
        json!({
            "ok": true,
            "dry_run": false,
            "would_execute": false,
            "destructive": true,
            "operation": operation,
            "message": "S3 operation completed.",
            "details": details
        }),
        json!({ "ok": true, "operation": operation }),
        None,
    )
}

fn result(
    invocation_id: String,
    output: Value,
    output_summary: Value,
    page: Option<InvocationOutputPage>,
) -> CapabilityInvocationResult {
    CapabilityInvocationResult {
        invocation_id,
        status: InvocationStatus::Succeeded,
        output,
        output_summary,
        page,
    }
}

fn validation_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Validation,
        code,
        message,
        details,
        None,
        false,
    )
}

fn unavailable_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Unavailable,
        code,
        message,
        details,
        None,
        true,
    )
}

fn target_error(config: &S3Config, code: &str, error: anyhow::Error) -> CapabilityError {
    let (message, redaction) = redact_s3_target_message(error.to_string(), config);
    capability_error_with_redaction(
        CapabilityErrorCategory::TargetSystem,
        code,
        "S3 target operation failed.",
        Value::Null,
        Some(TargetSystemFailure {
            system: Some(PLUGIN_ID.to_string()),
            code: None,
            message: Some(message),
        }),
        false,
        redaction,
    )
}

fn discovery_error(config: &S3Config, error: anyhow::Error) -> CapabilityError {
    let raw = error.to_string();
    let normalized = raw.to_ascii_lowercase();
    let (category, code, retryable) = if normalized.contains("invalidaccesskey")
        || normalized.contains("signaturedoesnotmatch")
        || normalized.contains("unauthorized")
        || normalized.contains("credential")
    {
        (
            CapabilityErrorCategory::Auth,
            "s3.discovery_auth_failed",
            false,
        )
    } else if normalized.contains("accessdenied") || normalized.contains("forbidden") {
        (
            CapabilityErrorCategory::Permission,
            "s3.discovery_policy_denied",
            false,
        )
    } else if normalized.contains("region")
        || normalized.contains("authorizationheadermalformed")
        || normalized.contains("permanentredirect")
    {
        (
            CapabilityErrorCategory::TargetSystem,
            "s3.discovery_region_mismatch",
            false,
        )
    } else {
        (
            CapabilityErrorCategory::Transport,
            "s3.discovery_endpoint_unreachable",
            true,
        )
    };
    let (message, redaction) = redact_s3_target_message(raw, config);
    capability_error_with_redaction(
        category,
        code,
        "S3 bucket discovery failed.",
        Value::Null,
        Some(TargetSystemFailure {
            system: Some(PLUGIN_ID.to_string()),
            code: Some(code.to_string()),
            message: Some(message),
        }),
        retryable,
        redaction,
    )
}

fn local_path_error(local_scope_id: &str, error: LocalPathError) -> CapabilityError {
    capability_error_with_redaction(
        error.category(),
        error.code(),
        error.safe_message(),
        json!({
            "local_scope_id": local_scope_id,
            "access": "scan_directory",
        }),
        None,
        error.retryable(),
        RedactionStatus::Applied,
    )
}

fn redact_s3_target_message(message: String, config: &S3Config) -> (String, RedactionStatus) {
    let original = message.clone();
    let mut redacted = redact_url_auth(redact_url_auth(message, "http://"), "https://");

    match &config.provider {
        S3Provider::Minio { endpoint } => redact_value(&mut redacted, endpoint),
        S3Provider::Custom { endpoint, .. } => redact_value(&mut redacted, endpoint),
        S3Provider::R2 { account_id } => redact_value(&mut redacted, account_id),
        S3Provider::Aws { region } => redact_value(&mut redacted, region),
    }

    if let S3Auth::AccessKey {
        access_key,
        secret_key,
    } = &config.auth
    {
        redact_value(&mut redacted, access_key);
        redact_value(&mut redacted, secret_key);
    }

    let status = if redacted != original {
        RedactionStatus::Applied
    } else {
        RedactionStatus::NotRequired
    };
    (redacted, status)
}

fn redact_value(message: &mut String, sensitive: &str) {
    if !sensitive.is_empty() {
        *message = message.replace(sensitive, "<redacted>");
    }
}

fn redact_url_auth(mut message: String, scheme: &str) -> String {
    let mut search_from = 0;
    while let Some(relative_start) = message[search_from..].find(scheme) {
        let scheme_start = search_from + relative_start;
        let auth_start = scheme_start + scheme.len();
        let tail = &message[auth_start..];
        let slash = tail.find('/');
        let Some(at) = tail.find('@') else {
            search_from = auth_start;
            continue;
        };

        if slash.is_some_and(|slash| slash < at) {
            search_from = auth_start;
            continue;
        }

        let auth_end = auth_start + at;
        message.replace_range(auth_start..auth_end, "<redacted>");
        search_from = auth_start + "<redacted>@".len();
    }
    message
}

fn capability_error(
    category: CapabilityErrorCategory,
    code: &str,
    message: &str,
    details: Value,
    target: Option<TargetSystemFailure>,
    retryable: bool,
) -> CapabilityError {
    capability_error_with_redaction(
        category,
        code,
        message,
        details,
        target,
        retryable,
        RedactionStatus::NotRequired,
    )
}

fn capability_error_with_redaction(
    category: CapabilityErrorCategory,
    code: &str,
    message: &str,
    details: Value,
    target: Option<TargetSystemFailure>,
    retryable: bool,
    redaction: RedactionStatus,
) -> CapabilityError {
    CapabilityError {
        category,
        code: code.to_string(),
        message: message.to_string(),
        details,
        target,
        retryable,
        redaction,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use voidb_core::{
        CapabilityInvocation, InvocationConnectionTarget, InvocationControls, Pagination,
    };

    use super::*;

    struct LocalFixture(PathBuf);

    impl LocalFixture {
        fn new() -> Self {
            static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos();
            let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "voidb-s3-local-boundary-{}-{nonce}-{sequence}",
                std::process::id()
            ));
            fs::create_dir(&path).expect("create S3 local fixture");
            Self(path)
        }

        fn display(&self) -> String {
            self.0.to_string_lossy().into_owned()
        }
    }

    impl Drop for LocalFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn catalog_marks_storage_writes_as_destructive_dry_run() {
        let capabilities = s3_capabilities();
        for id in ["put", "delete", "mkdir"] {
            let capability = capabilities
                .iter()
                .find(|capability| capability.id == id)
                .expect("capability exists");
            assert!(capability.destructive);
            assert!(capability.supports_dry_run);
            assert!(
                capability
                    .permissions
                    .iter()
                    .any(|permission| permission == &format!("s3.{}", id))
            );
        }

        let sync_plan = capabilities
            .iter()
            .find(|capability| capability.id == "sync_plan")
            .expect("sync_plan capability exists");
        assert!(!sync_plan.destructive);
        assert!(!sync_plan.supports_dry_run);
        assert!(!sync_plan.authorization.capability_wide_allowed);
        assert_eq!(
            sync_plan.input_schema["required"],
            json!(["bucket", "local_root", "local_path"])
        );
        assert!(
            sync_plan
                .authorization
                .approval_schema
                .as_ref()
                .unwrap()
                .fields
                .iter()
                .any(|field| field.path == "/local_root" && field.required)
        );
    }

    #[test]
    fn sync_plan_output_withholds_local_paths_by_default() {
        let report = LocalScanReport {
            entries: vec![voidb_core::LocalScanEntry {
                relative_path: "private/name.txt".into(),
                is_dir: false,
                size: 4,
                modified: None,
            }],
            observed_depth: 2,
            observed_path_bytes: 16,
            limits: LocalScanLimits::default(),
        };
        let options = SyncOptions {
            mode: SyncMode::Push,
            delete_extra: false,
            dry_run: true,
            exclude: Vec::new(),
        };
        let plan = SyncPlan {
            changes: vec![crate::types::SyncChange {
                rel_path: "private/name.txt".into(),
                action: SyncAction::Upload,
                size: 4,
                is_dir: false,
            }],
            total_transfer_bytes: 4,
        };

        let output = sync_plan_output(
            "s3",
            "bucket",
            Some("prefix"),
            LocalPlanOutput {
                scope_id: "local-scope:invoke-test",
                scan_report: &report,
                disclose_relative_paths: false,
            },
            &options,
            plan,
        );
        let encoded = serde_json::to_string(&output).unwrap();
        assert!(!encoded.contains("private/name.txt"));
        assert_eq!(
            output["changes"][0]["entry_ref"],
            "local-scope:invoke-test:entry:1"
        );
        assert!(output["changes"][0]["relative_path"].is_null());
        assert!(output.get("local_path").is_none());
    }

    #[tokio::test]
    async fn local_filesystem_boundary_fixture_rejects_unapproved_s3_scan_before_connect() {
        let fixture = LocalFixture::new();
        let error = invoke_s3_capability(
            &S3Config::default(),
            invocation(
                "sync_plan",
                json!({
                    "bucket": "unreachable",
                    "local_root": fixture.display(),
                    "local_path": "../outside",
                }),
            ),
        )
        .await
        .expect_err("traversal must fail before S3 access");
        let encoded = serde_json::to_string(&error).unwrap();

        assert_eq!(error.code, "permission.local_path_outside_scope");
        assert_eq!(error.redaction, RedactionStatus::Applied);
        assert!(!encoded.contains(&fixture.display()));
        assert!(!encoded.contains("../outside"));
    }

    #[test]
    fn local_filesystem_boundary_s3_adapter_enforces_limits_and_policy_metadata() {
        let fixture = LocalFixture::new();
        fs::write(fixture.0.join("one.txt"), b"one").unwrap();
        fs::write(fixture.0.join("two.txt"), b"two").unwrap();
        let scope = LocalPathScope::new(&fixture.0).unwrap();
        assert_eq!(
            crate::sync_ops::walk_local_tree_scoped(
                &scope,
                ".",
                LocalScanLimits {
                    max_entries: 1,
                    ..LocalScanLimits::default()
                },
            )
            .unwrap_err(),
            LocalPathError::ScanLimitExceeded
        );

        let capability = s3_capabilities()
            .into_iter()
            .find(|capability| capability.id == "sync_plan")
            .unwrap();
        assert!(capability.permissions.contains(&"local.scan".to_string()));
        assert!(!capability.authorization.capability_wide_allowed);
    }

    #[cfg(unix)]
    #[test]
    fn local_filesystem_boundary_s3_adapter_rejects_symlink_fixture() {
        use std::os::unix::fs::symlink;

        let fixture = LocalFixture::new();
        let outside = LocalFixture::new();
        fs::write(outside.0.join("secret.txt"), b"secret").unwrap();
        symlink(outside.0.join("secret.txt"), fixture.0.join("alias.txt")).unwrap();
        let scope = LocalPathScope::new(&fixture.0).unwrap();

        assert_eq!(
            crate::sync_ops::walk_local_tree_scoped(&scope, ".", LocalScanLimits::default())
                .unwrap_err(),
            LocalPathError::LinkDenied
        );
    }

    #[tokio::test]
    async fn write_dry_runs_do_not_open_s3_connection() {
        let config = S3Config::default();

        let mut put = invocation(
            "put",
            json!({
                "bucket": "missing",
                "key": "agent/probe.txt",
                "content_text": "hello"
            }),
        );
        put.controls.dry_run = true;
        let put_result = invoke_s3_capability(&config, put).await.unwrap();
        assert_eq!(put_result.output["dry_run"], true);
        assert_eq!(put_result.output["details"]["bytes"], 5);
        assert_eq!(
            put_result.output_summary,
            json!({ "dry_run": true, "operation": "put" })
        );
        let encoded = serde_json::to_string(&put_result).expect("serialize put result");
        assert!(!encoded.contains("hello"));

        let mut delete = invocation(
            "delete",
            json!({ "bucket": "missing", "key": "agent/probe.txt" }),
        );
        delete.controls.dry_run = true;
        let delete_result = invoke_s3_capability(&config, delete).await.unwrap();
        assert_eq!(delete_result.output["operation"], "delete");
        assert_eq!(
            delete_result.output_summary,
            json!({ "dry_run": true, "operation": "delete" })
        );

        let mut mkdir = invocation("mkdir", json!({ "bucket": "missing", "prefix": "agent" }));
        mkdir.controls.dry_run = true;
        let mkdir_result = invoke_s3_capability(&config, mkdir).await.unwrap();
        assert_eq!(mkdir_result.output["details"]["key"], "agent/");
        assert_eq!(
            mkdir_result.output_summary,
            json!({ "dry_run": true, "operation": "mkdir" })
        );
    }

    #[tokio::test]
    async fn expired_presign_lifetime_is_rejected_before_target_access() {
        let failure = invoke_s3_capability(
            &S3Config::default(),
            invocation(
                "presign",
                json!({
                    "bucket": "unreachable",
                    "key": "fixture.txt",
                    "method": "get",
                    "expires_seconds": 0
                }),
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(failure.code, "validation.presign_expiry_invalid");
        assert_eq!(failure.category, CapabilityErrorCategory::Validation);
    }

    #[test]
    fn native_page_cursor_preserves_opaque_provider_token() {
        let mut invocation = invocation("list", json!({ "bucket": "bucket" }));
        invocation.controls.page = Some(Pagination {
            limit: 10,
            cursor: Some("nope".into()),
        });
        let page = page_request(&invocation).expect("opaque S3 cursor");
        assert_eq!(page.cursor.as_deref(), Some("nope"));
    }

    #[test]
    fn discovery_errors_distinguish_auth_policy_region_and_endpoint() {
        let config = S3Config::default();
        for (message, code, retryable) in [
            (
                "InvalidAccessKey credential",
                "s3.discovery_auth_failed",
                false,
            ),
            (
                "AccessDenied forbidden",
                "s3.discovery_policy_denied",
                false,
            ),
            (
                "AuthorizationHeaderMalformed region mismatch",
                "s3.discovery_region_mismatch",
                false,
            ),
            (
                "connection refused",
                "s3.discovery_endpoint_unreachable",
                true,
            ),
        ] {
            let error = discovery_error(&config, anyhow::anyhow!(message));
            assert_eq!(error.code, code);
            assert_eq!(error.retryable, retryable);
        }
    }

    #[tokio::test]
    async fn transfer_family_requires_a_persistent_session() {
        let capabilities = s3_capabilities();
        for id in ["transfer", "transfer_status"] {
            let capability = capabilities
                .iter()
                .find(|capability| capability.id == id)
                .expect("transfer capability");
            assert_eq!(
                capability.execution_mode,
                voidb_core::CapabilityExecutionMode::SessionOnly
            );
            assert!(capability.output_schema.get("$defs").is_some());
            assert_eq!(
                capability.session_handoff.as_ref().unwrap().purpose,
                voidb_core::PluginSessionPurpose::FileTransfer
            );
            let error = invoke_s3_capability(&S3Config::default(), invocation(id, json!({})))
                .await
                .unwrap_err();
            assert_eq!(error.code, "unavailable.session_required");
        }
    }

    #[test]
    fn target_error_redacts_s3_credentials_and_endpoint() {
        let config = S3Config {
            provider: S3Provider::Minio {
                endpoint: "http://fixture-minio.local:9000".into(),
            },
            bucket: Some("voidb-fixture".into()),
            auth: S3Auth::AccessKey {
                access_key: "fixture-access-key".into(),
                secret_key: "fixture-secret-key".into(),
            },
            timeout: 30,
        };
        let error = target_error(
            &config,
            "s3.fixture_failed",
            anyhow::anyhow!(
                "request to http://user:password@fixture-minio.local:9000 failed for fixture-access-key and fixture-secret-key via http://fixture-minio.local:9000"
            ),
        );
        let encoded = serde_json::to_string(&error).expect("serialize error");

        for sensitive in [
            "fixture-access-key",
            "fixture-secret-key",
            "user:password",
            "http://fixture-minio.local:9000",
        ] {
            assert!(
                !encoded.contains(sensitive),
                "target error exposed sensitive S3 value: {encoded}"
            );
        }
        assert_eq!(error.redaction, RedactionStatus::Applied);
    }

    fn invocation(capability_id: &str, input: Value) -> CapabilityInvocation {
        CapabilityInvocation {
            id: "invoke-test".into(),
            plugin_id: PLUGIN_ID.into(),
            capability_id: capability_id.into(),
            connection: InvocationConnectionTarget::Stateless,
            input,
            controls: InvocationControls::default(),
            actor: None,
            requested_at: Utc::now(),
        }
    }
}
