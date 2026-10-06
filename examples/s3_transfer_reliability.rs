//! Live MinIO reliability proof for S3 Agent transfers.
//!
//! The script-facing example uses only a generated fixture bucket/prefix. It
//! proves conflict safety, expiring delegation, checksum failure, multipart
//! cancellation/resume, and close-time cleanup of retained multipart state.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{Context, Result, bail, ensure};
use chrono::{Duration, Utc};
use s3::creds::Credentials;
use s3::{Bucket, Region};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::sleep;
use voidb_core::{
    AgentSessionBinding, AgentSessionCallRequest, AgentSessionOpenContext, AgentSessionOpenRequest,
    AgentSessionRef, PluginAgentSession, PluginAgentSessionFactory, PluginSessionError,
    PluginSessionErrorCode, PluginSessionPurpose,
};
use voidb_plugin_s3::{
    S3AgentSessionFactory,
    config::{S3Auth, S3Config, S3Provider},
    service::S3Service,
};

const CHUNK_BYTES: usize = 5 * 1024 * 1024;
const PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let config = config_from_env()?;
    let bucket_name = required_env("VOIDB_S3_SMOKE_BUCKET")?;
    let prefix = required_env("VOIDB_S3_SMOKE_PREFIX")?;
    ensure!(
        bucket_name.starts_with("voidb-fixture-"),
        "refusing S3 reliability proof outside a generated fixture bucket"
    );
    ensure!(
        prefix.starts_with("voidb-smoke/") && prefix.ends_with('/'),
        "refusing S3 reliability proof outside a generated fixture prefix"
    );

    let run_id =
        std::env::var("VOIDB_FIXTURE_RUN_ID").unwrap_or_else(|_| "s3-transfer-reliability".into());
    let root = prepare_local_root(&run_id)?;
    let payload = deterministic_payload(PAYLOAD_BYTES);
    let checksum = hex::encode(Sha256::digest(&payload));
    std::fs::write(root.join("multipart.bin"), &payload)
        .with_context(|| format!("write {}", root.display()))?;

    let service = S3Service::new_direct(config.clone());
    ensure!(
        service
            .discover_buckets()
            .await?
            .iter()
            .any(|entry| entry.name == bucket_name),
        "provider-aware discovery omitted the generated MinIO bucket"
    );

    let source_key = format!("{prefix}reliability-source.bin");
    let destination_key = format!("{prefix}reliability-destination.bin");
    let moved_key = format!("{prefix}reliability-moved.bin");
    let resumed_key = format!("{prefix}reliability-resumed.bin");
    let mismatch_key = format!("{prefix}reliability-checksum-mismatch.bin");
    let cleanup_key = format!("{prefix}reliability-cleanup.bin");

    service.upload(&bucket_name, &source_key, b"source").await?;
    service
        .upload(&bucket_name, &destination_key, b"destination-sentinel")
        .await?;
    let conflict = service
        .copy_object_verified(
            &bucket_name,
            &source_key,
            &bucket_name,
            &destination_key,
            false,
        )
        .await
        .expect_err("copy without replacement must reject an existing destination");
    ensure!(
        conflict.to_string().contains("already exists"),
        "copy conflict should be explicit"
    );
    ensure!(
        service.download(&bucket_name, &destination_key).await? == b"destination-sentinel",
        "copy conflict changed the destination"
    );
    service
        .copy_object_verified(
            &bucket_name,
            &source_key,
            &bucket_name,
            &destination_key,
            true,
        )
        .await?;
    service
        .move_object_verified(
            &bucket_name,
            &destination_key,
            &bucket_name,
            &moved_key,
            false,
        )
        .await?;
    ensure!(
        !service
            .object_exists(&bucket_name, &destination_key)
            .await?,
        "verified move retained its source"
    );
    ensure!(
        service.download(&bucket_name, &moved_key).await? == b"source",
        "verified move changed object bytes"
    );

    prove_presign_expiry(&service, &bucket_name, &source_key).await?;

    let mismatch_session = open_session(&config, "checksum-mismatch").await?;
    let mismatch = call_transfer(
        mismatch_session.as_ref(),
        "s3-checksum-mismatch",
        json!({
            "operation": "upload",
            "bucket": bucket_name,
            "key": mismatch_key,
            "local_root": path_string(&root)?,
            "local_path": "multipart.bin",
            "expected_sha256": "0".repeat(64),
            "chunk_bytes": CHUNK_BYTES,
        }),
    )
    .await
    .expect_err("checksum mismatch must fail before remote mutation");
    ensure!(mismatch.code == PluginSessionErrorCode::HealthFailed);
    ensure!(
        !service.object_exists(&bucket_name, &mismatch_key).await?,
        "checksum mismatch created a remote object"
    );
    mismatch_session
        .close("checksum proof complete".into())
        .await?;

    let resume_session = open_session(&config, "multipart-resume").await?;
    let cancelled = cancel_upload_after_progress(
        resume_session.clone(),
        "s3-multipart-cancel",
        &bucket_name,
        &resumed_key,
        &root,
        &checksum,
    )
    .await?;
    let resume_token = cancelled["event"]["checkpoint"]["token"]
        .as_str()
        .context("cancelled multipart should expose a resume token")?;
    ensure!(
        cancelled["event"]["phase"] == "cancelled"
            && cancelled["event"]["cleanup"]["remote_partial"] == "retained_for_resume",
        "cancelled multipart should retain a scoped checkpoint: {cancelled}"
    );
    let resumed = call_transfer(
        resume_session.as_ref(),
        "s3-multipart-resume",
        json!({
            "operation": "upload",
            "bucket": bucket_name,
            "key": resumed_key,
            "local_root": path_string(&root)?,
            "local_path": "multipart.bin",
            "expected_sha256": checksum,
            "resume_token": resume_token,
            "chunk_bytes": CHUNK_BYTES,
        }),
    )
    .await?;
    ensure!(
        resumed["event"]["phase"] == "completed"
            && resumed["event"]["checksum"]["algorithm"] == "sha256"
            && resumed["event"]["checksum"]["verified"] == true,
        "resumed multipart should complete with verified SHA-256: {resumed}"
    );
    ensure!(
        service.download(&bucket_name, &resumed_key).await? == payload,
        "resumed multipart changed object bytes"
    );
    resume_session.close("resume proof complete".into()).await?;

    let cleanup_session = open_session(&config, "multipart-cleanup").await?;
    let _ = cancel_upload_after_progress(
        cleanup_session.clone(),
        "s3-multipart-cleanup-cancel",
        &bucket_name,
        &cleanup_key,
        &root,
        &checksum,
    )
    .await?;
    ensure!(
        multipart_exists(&config, &bucket_name, &cleanup_key).await?,
        "cancelled multipart should exist only as retained resumable state"
    );
    cleanup_session
        .close("abort retained multipart".into())
        .await?;
    wait_for_multipart_cleanup(&config, &bucket_name, &cleanup_key).await?;
    ensure!(
        !service.object_exists(&bucket_name, &cleanup_key).await?,
        "close-time multipart cleanup committed a partial object"
    );

    for key in [
        source_key,
        moved_key,
        resumed_key,
        mismatch_key,
        cleanup_key,
    ] {
        let _ = service.delete_object(&bucket_name, &key).await;
    }
    std::fs::remove_dir_all(&root).with_context(|| format!("remove {}", root.display()))?;

    println!("s3 transfer reliability fixture passed");
    println!(
        "coverage: discovery, conflict, verified move, expired presign, checksum mismatch, multipart cancel/resume, close cleanup"
    );
    Ok(())
}

async fn prove_presign_expiry(service: &S3Service, bucket: &str, key: &str) -> Result<()> {
    let url = service.presign_get(bucket, key, 1).await?;
    let live_status = curl_status(&url)?;
    ensure!(
        (200..300).contains(&live_status),
        "fresh presigned URL returned HTTP {live_status}"
    );
    sleep(StdDuration::from_secs(2)).await;
    let expired_status = curl_status(&url)?;
    ensure!(
        !(200..300).contains(&expired_status),
        "expired presigned URL remained usable"
    );
    Ok(())
}

fn curl_status(url: &str) -> Result<u16> {
    let output = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--output",
            "/dev/null",
            "--write-out",
            "%{http_code}",
            url,
        ])
        .output()
        .context("run curl for presigned URL")?;
    ensure!(output.status.success(), "curl failed for presigned URL");
    String::from_utf8(output.stdout)?
        .parse()
        .context("parse presigned URL HTTP status")
}

async fn cancel_upload_after_progress(
    session: Arc<dyn PluginAgentSession>,
    call_id: &str,
    bucket: &str,
    key: &str,
    root: &Path,
    checksum: &str,
) -> Result<Value> {
    let task_session = session.clone();
    let task_call_id = call_id.to_string();
    let input = json!({
        "operation": "upload",
        "bucket": bucket,
        "key": key,
        "local_root": path_string(root)?,
        "local_path": "multipart.bin",
        "expected_sha256": checksum,
        "chunk_bytes": CHUNK_BYTES,
    });
    let task =
        tokio::spawn(
            async move { call_transfer(task_session.as_ref(), &task_call_id, input).await },
        );

    let mut cancelled = false;
    for sequence in 0..1_000 {
        let status = call_status(session.as_ref(), &format!("s3-status-{sequence}")).await?;
        if status["event"]["progress"]["chunks_completed"]
            .as_u64()
            .unwrap_or_default()
            >= 1
        {
            session.cancel(call_id).await?;
            cancelled = true;
            break;
        }
        if status["event"]["phase"] == "completed" {
            bail!("multipart upload completed before cancellation checkpoint");
        }
        sleep(StdDuration::from_millis(5)).await;
    }
    ensure!(
        cancelled,
        "multipart upload never reached a cancellable checkpoint"
    );
    let failure = task
        .await
        .context("join multipart cancellation")?
        .expect_err("cancelled multipart call should return a structured error");
    ensure!(failure.code == PluginSessionErrorCode::Cancelled);
    call_status(session.as_ref(), "s3-status-cancelled").await
}

async fn open_session(config: &S3Config, label: &str) -> Result<Arc<dyn PluginAgentSession>> {
    S3AgentSessionFactory::new(config.clone())
        .open(session_context(label))
        .await
        .map_err(|error| anyhow::anyhow!("open S3 transfer session: {error}"))
}

fn session_context(label: &str) -> AgentSessionOpenContext {
    let purpose = PluginSessionPurpose::FileTransfer;
    let capabilities = vec!["s3.transfer".into(), "s3.transfer_status".into()];
    AgentSessionOpenContext {
        binding: AgentSessionBinding {
            grant_id: format!("s3-fixture-{label}-grant"),
            profile_id: "s3-fixture-profile".into(),
            plugin_id: "s3".into(),
            purpose: purpose.clone(),
            allowed_capabilities: capabilities.clone(),
            host_generation: 1,
        },
        request: AgentSessionOpenRequest {
            purpose,
            capabilities,
            lease_seconds: 300,
            concurrency: Default::default(),
            destructive_acknowledged: true,
            input: Value::Null,
        },
        lease_expires_at: Utc::now() + Duration::minutes(5),
    }
}

async fn call_transfer(
    session: &dyn PluginAgentSession,
    call_id: &str,
    input: Value,
) -> std::result::Result<Value, PluginSessionError> {
    session
        .call(AgentSessionCallRequest {
            session: AgentSessionRef::new("s3-reliability-session", 1),
            call_id: call_id.into(),
            capability: "s3.transfer".into(),
            input,
            destructive_acknowledged: true,
            timeout_ms: Some(180_000),
            output_limit_bytes: 128 * 1024,
        })
        .await
        .map(|result| result.output)
}

async fn call_status(session: &dyn PluginAgentSession, call_id: &str) -> Result<Value> {
    session
        .call(AgentSessionCallRequest {
            session: AgentSessionRef::new("s3-reliability-session", 1),
            call_id: call_id.into(),
            capability: "s3.transfer_status".into(),
            input: json!({}),
            destructive_acknowledged: false,
            timeout_ms: Some(5_000),
            output_limit_bytes: 128 * 1024,
        })
        .await
        .map(|result| result.output)
        .map_err(|error| anyhow::anyhow!("read S3 transfer status: {error}"))
}

async fn multipart_exists(config: &S3Config, bucket_name: &str, key: &str) -> Result<bool> {
    let bucket = fixture_bucket(config, bucket_name)?;
    Ok(bucket
        .list_multiparts_uploads(Some(key), None)
        .await?
        .into_iter()
        .flat_map(|page| page.uploads)
        .any(|upload| upload.key == key))
}

async fn wait_for_multipart_cleanup(config: &S3Config, bucket_name: &str, key: &str) -> Result<()> {
    for _ in 0..100 {
        if !multipart_exists(config, bucket_name, key).await? {
            return Ok(());
        }
        sleep(StdDuration::from_millis(20)).await;
    }
    bail!("close left retained multipart state behind")
}

fn fixture_bucket(config: &S3Config, bucket_name: &str) -> Result<Box<Bucket>> {
    let endpoint = match &config.provider {
        S3Provider::Minio { endpoint } => endpoint.clone(),
        _ => bail!("S3 reliability fixture requires MinIO"),
    };
    let credentials = match &config.auth {
        S3Auth::AccessKey {
            access_key,
            secret_key,
        } => Credentials::new(Some(access_key), Some(secret_key), None, None, None)?,
        _ => bail!("S3 reliability fixture requires access-key auth"),
    };
    Ok(Bucket::new(
        bucket_name,
        Region::Custom {
            region: "us-east-1".into(),
            endpoint,
        },
        credentials,
    )?
    .with_path_style())
}

fn deterministic_payload(length: usize) -> Vec<u8> {
    (0..length)
        .map(|index| ((index.wrapping_mul(31).wrapping_add(17)) % 251) as u8)
        .collect()
}

fn prepare_local_root(run_id: &str) -> Result<PathBuf> {
    let root = std::env::temp_dir().join(format!("voidb-s3-transfer-reliability-{run_id}"));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir(&root).with_context(|| format!("create {}", root.display()))?;
    Ok(root)
}

fn path_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_string)
        .context("fixture path must be valid UTF-8")
}

fn config_from_env() -> Result<S3Config> {
    Ok(S3Config {
        provider: S3Provider::Minio {
            endpoint: required_env("VOIDB_S3_SMOKE_ENDPOINT")?,
        },
        bucket: Some(required_env("VOIDB_S3_SMOKE_BUCKET")?),
        auth: S3Auth::AccessKey {
            access_key: required_env("VOIDB_S3_SMOKE_ACCESS_KEY")?,
            secret_key: required_env("VOIDB_S3_SMOKE_SECRET_KEY")?,
        },
        timeout: 30,
    })
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} is required"))
}
