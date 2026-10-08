//! S3 fixture-backed capability smoke driver.
//!
//! This example is script-facing. It exercises the S3 plugin capability
//! surface against a disposable local MinIO fixture using only the generated
//! bucket and scratch prefix from the fixture environment.

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::Utc;
use serde_json::{Value, json};
use std::path::PathBuf;
use voidb_core::{
    ActorRef, ActorType, CapabilityError, CapabilityErrorCategory, CapabilityInvocation,
    CapabilityInvocationResult, InvocationConnectionTarget, InvocationControls, InvocationStatus,
    Pagination, RedactionStatus,
};
use voidb_plugin_s3::{
    config::{S3Auth, S3Config, S3Provider},
    invoke_s3_capability,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let config = config_from_env()?;
    let bucket = required_env("VOIDB_S3_SMOKE_BUCKET")?;
    let prefix = required_env("VOIDB_S3_SMOKE_PREFIX")?;
    ensure!(
        bucket.starts_with("voidb-fixture-"),
        "refusing S3 smoke outside a generated fixture bucket: {bucket}"
    );
    ensure!(
        prefix.starts_with("voidb-smoke/") && prefix.ends_with('/'),
        "refusing S3 smoke outside a generated fixture prefix: {prefix}"
    );

    let run_id =
        std::env::var("VOIDB_FIXTURE_RUN_ID").unwrap_or_else(|_| "s3-fixture-smoke".into());
    let alpha_key = format!("{prefix}alpha.txt");
    let beta_key = format!("{prefix}beta.txt");
    let nested_key = format!("{prefix}nested/gamma.txt");
    let delete_key = format!("{prefix}delete-me.txt");
    let missing_key = format!("{prefix}missing-after-delete.txt");
    let alpha_content = format!("VoidB S3 fixture smoke alpha payload for {run_id}");
    let beta_content = format!("VoidB S3 fixture smoke beta payload for {run_id}");
    let secret_content = "voidb-s3-fixture-dry-run-secret";

    let dry_put = invoke_checked(
        &unavailable_config(),
        "put",
        json!({
            "bucket": bucket,
            "key": alpha_key,
            "content_text": secret_content
        }),
        true,
        None,
        "s3.put dry-run should not require a live target",
    )
    .await?;
    ensure_succeeded(&dry_put, "s3.put dry-run")?;
    ensure!(
        dry_put.output["details"]["bytes"].as_u64() == Some(secret_content.len() as u64),
        "s3.put dry-run should report byte count: {}",
        dry_put.output
    );
    ensure_result_excludes(&dry_put, secret_content, "s3.put dry-run")?;

    let dry_delete = invoke_checked(
        &unavailable_config(),
        "delete",
        json!({ "bucket": bucket, "key": delete_key }),
        true,
        None,
        "s3.delete dry-run should not require a live target",
    )
    .await?;
    ensure_succeeded(&dry_delete, "s3.delete dry-run")?;

    let dry_mkdir = invoke_checked(
        &unavailable_config(),
        "mkdir",
        json!({ "bucket": bucket, "prefix": format!("{prefix}dry-run-dir") }),
        true,
        None,
        "s3.mkdir dry-run should not require a live target",
    )
    .await?;
    ensure_succeeded(&dry_mkdir, "s3.mkdir dry-run")?;

    invoke_checked(
        &config,
        "mkdir",
        json!({ "bucket": bucket, "prefix": &prefix }),
        false,
        None,
        "s3.mkdir scratch prefix",
    )
    .await?;

    for (key, content) in [
        (alpha_key.as_str(), alpha_content.as_str()),
        (beta_key.as_str(), beta_content.as_str()),
        (nested_key.as_str(), "nested fixture payload"),
        (delete_key.as_str(), "delete fixture payload"),
    ] {
        let result = invoke_checked(
            &config,
            "put",
            json!({ "bucket": bucket, "key": key, "content_text": content }),
            false,
            None,
            "s3.put fixture object",
        )
        .await?;
        ensure_succeeded(&result, "s3.put fixture object")?;
        ensure_result_excludes_config(&result, &config, "s3.put fixture object")?;
    }

    let paged = invoke_checked(
        &config,
        "list",
        json!({ "bucket": bucket, "prefix": prefix }),
        false,
        Some(Pagination {
            limit: 1,
            cursor: None,
        }),
        "s3.list paged scratch prefix",
    )
    .await?;
    ensure_succeeded(&paged, "s3.list paged")?;
    ensure!(
        paged.output["entry_count"].as_u64().unwrap_or_default() <= 1
            && paged.output["truncated"] == true
            && paged.output["next_cursor"].is_string(),
        "s3.list should honor pagination: {}",
        paged.output
    );

    let full_list = invoke_checked(
        &config,
        "list",
        json!({ "bucket": bucket, "prefix": prefix }),
        false,
        Some(Pagination {
            limit: 20,
            cursor: None,
        }),
        "s3.list full scratch prefix",
    )
    .await?;
    ensure_succeeded(&full_list, "s3.list full")?;
    ensure_entry_present(&full_list.output, &alpha_key)?;
    ensure_entry_present(&full_list.output, &beta_key)?;
    ensure_entry_present(&full_list.output, &delete_key)?;
    ensure_entry_present(&full_list.output, &format!("{prefix}nested/"))?;

    let stat = invoke_checked(
        &config,
        "stat",
        json!({ "bucket": bucket, "key": alpha_key }),
        false,
        None,
        "s3.stat alpha object",
    )
    .await?;
    ensure_succeeded(&stat, "s3.stat alpha")?;
    ensure!(
        stat.output["entry"]["size"].as_u64() == Some(alpha_content.len() as u64),
        "s3.stat should return alpha object size: {}",
        stat.output
    );

    let truncated_get = invoke_checked(
        &config,
        "get",
        json!({ "bucket": bucket, "key": alpha_key, "max_bytes": 8 }),
        false,
        None,
        "s3.get alpha truncated",
    )
    .await?;
    ensure_succeeded(&truncated_get, "s3.get alpha truncated")?;
    ensure!(
        truncated_get.output["content_truncated"] == true
            && truncated_get.output["bytes_returned"] == 8,
        "s3.get should enforce max_bytes: {}",
        truncated_get.output
    );

    let full_get = invoke_checked(
        &config,
        "get",
        json!({ "bucket": bucket, "key": beta_key }),
        false,
        None,
        "s3.get beta object",
    )
    .await?;
    ensure_succeeded(&full_get, "s3.get beta")?;
    let decoded = decode_content(&full_get)?;
    ensure!(
        decoded == beta_content.as_bytes(),
        "s3.get should return beta object bytes"
    );
    ensure_result_excludes_config(&full_get, &config, "s3.get beta")?;

    let sync_dir = prepare_sync_dir(&run_id)?;
    let sync_plan = invoke_checked(
        &config,
        "sync_plan",
        json!({
            "bucket": bucket,
            "remote_prefix": prefix,
            "local_root": &sync_dir,
            "local_path": ".",
            "mode": "sync",
            "delete_extra": false
        }),
        false,
        None,
        "s3.sync_plan dry-run",
    )
    .await?;
    ensure_succeeded(&sync_plan, "s3.sync_plan")?;
    ensure!(
        sync_plan.output["target"] == "s3" && sync_plan.output["change_count"].is_number(),
        "s3.sync_plan should return a dry-run plan: {}",
        sync_plan.output
    );

    let dry_delete_live = invoke_checked(
        &config,
        "delete",
        json!({ "bucket": bucket, "key": delete_key }),
        true,
        None,
        "s3.delete live dry-run",
    )
    .await?;
    ensure_succeeded(&dry_delete_live, "s3.delete live dry-run")?;
    invoke_checked(
        &config,
        "stat",
        json!({ "bucket": bucket, "key": delete_key }),
        false,
        None,
        "s3.stat delete object after dry-run",
    )
    .await?;

    invoke_checked(
        &config,
        "delete",
        json!({ "bucket": bucket, "key": delete_key }),
        false,
        None,
        "s3.delete fixture object",
    )
    .await?;
    ensure_missing_object_error(
        invoke(
            &config,
            "get",
            json!({ "bucket": bucket, "key": delete_key }),
            false,
            None,
        )
        .await,
        &config,
        "s3.get deleted object",
    )?;
    ensure_missing_object_error(
        invoke(
            &config,
            "get",
            json!({ "bucket": bucket, "key": missing_key }),
            false,
            None,
        )
        .await,
        &config,
        "s3.get missing object",
    )?;

    ensure_failed_auth_redacts(&config, &bucket, &prefix).await?;
    cleanup_objects(&config, &bucket, [&alpha_key, &beta_key, &nested_key]).await?;
    let _ = std::fs::remove_dir_all(&sync_dir);

    println!("s3 fixture capability smoke passed");
    println!("capabilities: list, stat, get, put, delete, mkdir, sync_plan");
    println!("scratch_prefix: {prefix}");
    Ok(())
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

fn unavailable_config() -> S3Config {
    S3Config {
        provider: S3Provider::Minio {
            endpoint: "http://127.0.0.1:0".into(),
        },
        bucket: Some("missing".into()),
        auth: S3Auth::Anonymous,
        timeout: 1,
    }
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} is required"))
}

#[allow(clippy::result_large_err)]
async fn invoke(
    config: &S3Config,
    capability_id: &str,
    input: Value,
    dry_run: bool,
    page: Option<Pagination>,
) -> std::result::Result<CapabilityInvocationResult, CapabilityError> {
    invoke_s3_capability(
        config,
        CapabilityInvocation {
            id: format!("s3-fixture-smoke-{capability_id}"),
            plugin_id: "s3".into(),
            capability_id: capability_id.into(),
            connection: InvocationConnectionTarget::Stateless,
            input,
            controls: InvocationControls {
                dry_run,
                page,
                ..InvocationControls::default()
            },
            actor: Some(ActorRef {
                id: "agent:s3-fixture-smoke".into(),
                actor_type: ActorType::Agent,
            }),
            requested_at: Utc::now(),
        },
    )
    .await
}

async fn invoke_checked(
    config: &S3Config,
    capability_id: &str,
    input: Value,
    dry_run: bool,
    page: Option<Pagination>,
    label: &str,
) -> Result<CapabilityInvocationResult> {
    invoke(config, capability_id, input, dry_run, page)
        .await
        .map_err(|error| {
            let error_json = serde_json::to_string(&error).unwrap_or_else(|_| format!("{error:?}"));
            anyhow::anyhow!("{label}: {error_json}")
        })
}

fn ensure_succeeded(result: &CapabilityInvocationResult, label: &str) -> Result<()> {
    ensure!(
        result.status == InvocationStatus::Succeeded,
        "{label} returned non-success status: {:?}",
        result.status
    );
    Ok(())
}

fn ensure_result_excludes(
    result: &CapabilityInvocationResult,
    sample: &str,
    label: &str,
) -> Result<()> {
    let output = serde_json::to_string(&result.output)?;
    let summary = serde_json::to_string(&result.output_summary)?;
    ensure!(
        !output.contains(sample) && !summary.contains(sample),
        "{label} output exposed protected sample"
    );
    Ok(())
}

fn ensure_result_excludes_config(
    result: &CapabilityInvocationResult,
    config: &S3Config,
    label: &str,
) -> Result<()> {
    let output = serde_json::to_string(&result.output)?;
    let summary = serde_json::to_string(&result.output_summary)?;
    for sample in protected_samples(config) {
        ensure!(
            !output.contains(&sample) && !summary.contains(&sample),
            "{label} output exposed S3 config material"
        );
    }
    Ok(())
}

fn protected_samples(config: &S3Config) -> Vec<String> {
    let mut samples = Vec::new();
    match &config.provider {
        S3Provider::Minio { endpoint } | S3Provider::Custom { endpoint, .. } => {
            samples.push(endpoint.clone());
        }
        S3Provider::R2 { account_id } => samples.push(account_id.clone()),
        S3Provider::Aws { region } => samples.push(region.clone()),
    }
    if let S3Auth::AccessKey {
        access_key,
        secret_key,
    } = &config.auth
    {
        samples.push(access_key.clone());
        samples.push(secret_key.clone());
    }
    samples
        .into_iter()
        .filter(|sample| !sample.is_empty())
        .collect()
}

fn ensure_entry_present(output: &Value, key: &str) -> Result<()> {
    let entries = output["entries"]
        .as_array()
        .context("s3.list output should include entries array")?;
    ensure!(
        entries.iter().any(|item| item["key"] == key),
        "s3.list did not include expected key {key}: {}",
        output
    );
    Ok(())
}

fn decode_content(result: &CapabilityInvocationResult) -> Result<Vec<u8>> {
    let encoded = result.output["content_base64"]
        .as_str()
        .context("s3.get output should include content_base64")?;
    BASE64.decode(encoded).context("decode s3.get content")
}

fn prepare_sync_dir(run_id: &str) -> Result<String> {
    let dir = std::env::temp_dir().join(format!("voidb-s3-fixture-sync-{run_id}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
    std::fs::write(dir.join("local-only.txt"), b"local fixture sync payload")
        .with_context(|| format!("write {}", dir.display()))?;
    path_string(dir)
}

fn path_string(path: PathBuf) -> Result<String> {
    path.into_os_string()
        .into_string()
        .map_err(|_| anyhow::anyhow!("fixture path is not valid UTF-8"))
}

fn ensure_missing_object_error(
    result: std::result::Result<CapabilityInvocationResult, CapabilityError>,
    config: &S3Config,
    label: &str,
) -> Result<()> {
    match result {
        Ok(result) => bail!(
            "{label}: expected missing object error, got {}",
            result.output
        ),
        Err(error) => {
            ensure!(
                error.category == CapabilityErrorCategory::TargetSystem,
                "{label}: expected target error, got {:?}",
                error.category
            );
            ensure!(
                error.code == "s3.get_failed",
                "{label}: expected s3.get_failed, got {}",
                error.code
            );
            ensure_error_excludes_config(&error, config, label)?;
        }
    }
    Ok(())
}

async fn ensure_failed_auth_redacts(config: &S3Config, bucket: &str, prefix: &str) -> Result<()> {
    let mut bad = config.clone();
    if let S3Auth::AccessKey { secret_key, .. } = &mut bad.auth {
        secret_key.push_str("-wrong-secret");
    }
    match invoke(
        &bad,
        "list",
        json!({ "bucket": bucket, "prefix": prefix }),
        false,
        None,
    )
    .await
    {
        Ok(result) => bail!(
            "expected s3.list auth failure, got output: {}",
            result.output
        ),
        Err(error) => {
            ensure_error_excludes_config(&error, &bad, "s3.list bad auth")?;
            ensure_error_excludes_config(&error, config, "s3.list bad auth")?;
            ensure!(
                matches!(
                    error.redaction,
                    RedactionStatus::Applied | RedactionStatus::NotRequired
                ),
                "target error should report a non-failed redaction state: {:?}",
                error.redaction
            );
        }
    }
    Ok(())
}

fn ensure_error_excludes_config(
    error: &CapabilityError,
    config: &S3Config,
    label: &str,
) -> Result<()> {
    let text = serde_json::to_string(error)?;
    for sample in protected_samples(config) {
        ensure!(
            !text.contains(&sample),
            "{label} error exposed S3 config material: {text}"
        );
    }
    Ok(())
}

async fn cleanup_objects<'a>(
    config: &S3Config,
    bucket: &str,
    keys: impl IntoIterator<Item = &'a String>,
) -> Result<()> {
    for key in keys {
        let _ = invoke(
            config,
            "delete",
            json!({ "bucket": bucket, "key": key }),
            false,
            None,
        )
        .await;
    }
    Ok(())
}
