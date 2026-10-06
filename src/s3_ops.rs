//! S3 operations wrapping rust-s3

use anyhow::Result;
use http::{HeaderMap, HeaderName, HeaderValue};
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::{Bucket, BucketConfiguration, Region};

use crate::config::{S3Auth, S3Config, S3Provider};
use crate::types::{S3BucketEntry, S3Entry, S3EntryType, S3ListPage};

/// Build rust-s3 Region from our provider config
fn build_region(provider: &S3Provider) -> Result<Region> {
    match provider {
        S3Provider::Aws { region } => region
            .parse::<Region>()
            .map_err(|e| anyhow::anyhow!("Invalid AWS region '{}': {}", region, e)),
        S3Provider::Minio { endpoint } => Ok(Region::Custom {
            region: "us-east-1".to_owned(),
            endpoint: endpoint.clone(),
        }),
        S3Provider::R2 { account_id } => Ok(Region::R2 {
            account_id: account_id.clone(),
        }),
        S3Provider::Custom {
            endpoint, region, ..
        } => Ok(Region::Custom {
            region: region.clone(),
            endpoint: endpoint.clone(),
        }),
    }
}

/// Build rust-s3 Credentials from our auth config
fn build_credentials(auth: &S3Auth) -> Result<Credentials> {
    match auth {
        S3Auth::AccessKey {
            access_key,
            secret_key,
        } => Credentials::new(
            Some(access_key.as_str()),
            Some(secret_key.as_str()),
            None,
            None,
            None,
        )
        .map_err(|e| anyhow::anyhow!("Failed to create credentials: {}", e)),
        S3Auth::Auto => {
            Credentials::default().map_err(|e| anyhow::anyhow!("Failed to load credentials: {}", e))
        }
        S3Auth::Anonymous => Credentials::anonymous()
            .map_err(|e| anyhow::anyhow!("Failed to create anonymous credentials: {}", e)),
    }
}

/// Create a rust-s3 Bucket instance for the given config and bucket name
pub fn create_bucket(config: &S3Config, bucket_name: &str) -> Result<Box<Bucket>> {
    let region = build_region(&config.provider)?;
    let credentials = build_credentials(&config.auth)?;

    let mut bucket = Bucket::new(bucket_name, region, credentials)
        .map_err(|e| anyhow::anyhow!("Failed to create bucket handle: {}", e))?;

    // Apply path-style for MinIO and Custom providers
    match &config.provider {
        S3Provider::Minio { .. } => {
            bucket = bucket.with_path_style();
        }
        S3Provider::Custom { path_style, .. } if *path_style => {
            bucket = bucket.with_path_style();
        }
        S3Provider::R2 { .. } => {
            bucket = bucket.with_path_style();
        }
        _ => {}
    }

    Ok(bucket)
}

/// List all buckets accessible with the given config
pub async fn list_buckets(config: &S3Config) -> Result<Vec<String>> {
    Ok(discover_buckets(config)
        .await?
        .into_iter()
        .map(|bucket| bucket.name)
        .collect())
}

/// Discover buckets with the provider metadata returned by ListBuckets.
pub async fn discover_buckets(config: &S3Config) -> Result<Vec<S3BucketEntry>> {
    let region = build_region(&config.provider)?;
    let credentials = build_credentials(&config.auth)?;

    let response = Bucket::list_buckets(region, credentials)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list buckets: {}", e))?;

    let mut buckets = response
        .buckets
        .bucket
        .into_iter()
        .map(|bucket| S3BucketEntry {
            name: bucket.name,
            creation_date: (!bucket.creation_date.is_empty()).then_some(bucket.creation_date),
        })
        .collect::<Vec<_>>();
    buckets.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(buckets)
}

/// Fetch one bounded native ListObjects page without materializing the full bucket prefix.
pub async fn list_objects_page(
    bucket: &Bucket,
    prefix: &str,
    delimiter: Option<&str>,
    continuation_token: Option<String>,
    max_keys: usize,
) -> Result<S3ListPage> {
    let first_page = continuation_token.is_none();
    let (mut page, status) = bucket
        .list_page(
            prefix.to_string(),
            delimiter.map(str::to_string),
            continuation_token,
            None,
            Some(max_keys),
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list objects: {}", e))?;
    if !(200..300).contains(&status) {
        anyhow::bail!("Failed to list objects: HTTP {status}");
    }

    let mut entries = entries_from_page(&page, prefix);
    // Some S3-compatible targets count the zero-byte directory placeholder
    // that exactly matches `prefix` as the whole first page, then incorrectly
    // report that page as non-truncated. The placeholder is not a child entry,
    // so retry the first page strictly after it instead of returning a silent
    // empty listing.
    if first_page
        && !prefix.is_empty()
        && entries.is_empty()
        && page.contents.iter().any(|object| object.key == prefix)
    {
        let (retry, retry_status) = bucket
            .list_page(
                prefix.to_string(),
                delimiter.map(str::to_string),
                None,
                Some(prefix.to_string()),
                Some(max_keys),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list objects after prefix marker: {e}"))?;
        if !(200..300).contains(&retry_status) {
            anyhow::bail!("Failed to list objects after prefix marker: HTTP {retry_status}");
        }
        page = retry;
        entries = entries_from_page(&page, prefix);
    }
    Ok(S3ListPage {
        entries,
        continuation_token: page.continuation_token,
        next_continuation_token: page.next_continuation_token,
        truncated: page.is_truncated,
    })
}

/// List objects at the given prefix with optional delimiter
pub async fn list_objects(
    bucket: &Bucket,
    prefix: &str,
    delimiter: Option<&str>,
) -> Result<Vec<S3Entry>> {
    let results = bucket
        .list(prefix.to_string(), delimiter.map(|d| d.to_string()))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list objects: {}", e))?;

    let mut entries = results
        .iter()
        .flat_map(|page| entries_from_page(page, prefix))
        .collect::<Vec<_>>();

    // Sort: prefixes first, then alphabetical
    entries.sort_by(|a, b| match (&a.entry_type, &b.entry_type) {
        (S3EntryType::Prefix, S3EntryType::Object) => std::cmp::Ordering::Less,
        (S3EntryType::Object, S3EntryType::Prefix) => std::cmp::Ordering::Greater,
        _ => a
            .display_name
            .to_lowercase()
            .cmp(&b.display_name.to_lowercase()),
    });

    Ok(entries)
}

fn entries_from_page(page: &s3::serde_types::ListBucketResult, prefix: &str) -> Vec<S3Entry> {
    let mut entries = Vec::new();
    if let Some(prefixes) = &page.common_prefixes {
        entries.extend(prefixes.iter().map(|common| S3Entry {
            key: common.prefix.clone(),
            display_name: extract_display_name(&common.prefix),
            entry_type: S3EntryType::Prefix,
            size: 0,
            last_modified: None,
            storage_class: None,
            etag: None,
        }));
    }
    entries.extend(
        page.contents
            .iter()
            .filter(|object| object.key != prefix)
            .map(|object| S3Entry {
                key: object.key.clone(),
                display_name: extract_display_name(&object.key),
                entry_type: S3EntryType::Object,
                size: object.size,
                last_modified: Some(object.last_modified.clone()),
                storage_class: object.storage_class.clone(),
                etag: object.e_tag.clone(),
            }),
    );
    entries.sort_by(|left, right| match (&left.entry_type, &right.entry_type) {
        (S3EntryType::Prefix, S3EntryType::Object) => std::cmp::Ordering::Less,
        (S3EntryType::Object, S3EntryType::Prefix) => std::cmp::Ordering::Greater,
        _ => left
            .display_name
            .to_lowercase()
            .cmp(&right.display_name.to_lowercase()),
    });
    entries
}

/// Download an object and return its contents
pub async fn download(bucket: &Bucket, key: &str) -> Result<Vec<u8>> {
    let response = bucket
        .get_object(key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to download object: {}", e))?;

    Ok(response.to_vec())
}

/// Upload data to an object key
pub async fn upload(
    bucket: &Bucket,
    key: &str,
    data: &[u8],
    content_type: Option<&str>,
) -> Result<()> {
    let ct = content_type.unwrap_or("application/octet-stream");
    bucket
        .put_object_with_content_type(key, data, ct)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to upload object: {}", e))?;

    Ok(())
}

/// Delete an object
pub async fn delete(bucket: &Bucket, key: &str) -> Result<()> {
    bucket
        .delete_object(key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to delete object: {}", e))?;

    Ok(())
}

/// Copy an object within the same bucket
pub async fn copy_object(bucket: &Bucket, src: &str, dst: &str) -> Result<()> {
    copy_object_between(bucket, &bucket.name(), src, dst).await
}

/// Copy between buckets and verify the destination identity before returning.
pub async fn copy_object_verified(
    config: &S3Config,
    source_bucket: &str,
    source_key: &str,
    destination_bucket: &str,
    destination_key: &str,
    replace: bool,
) -> Result<S3Entry> {
    let source = create_bucket(config, source_bucket)?;
    let destination = create_bucket(config, destination_bucket)?;
    let source_info = get_object_info(&source, source_key).await?;
    if !replace && object_exists(&destination, destination_key).await? {
        anyhow::bail!("Destination already exists and replace was not authorized");
    }
    copy_object_between(&destination, source_bucket, source_key, destination_key).await?;
    let copied = get_object_info(&destination, destination_key).await?;
    if copied.size != source_info.size {
        anyhow::bail!("Copied object size did not match the source");
    }
    Ok(copied)
}

async fn copy_object_between(
    destination: &Bucket,
    source_bucket: &str,
    source_key: &str,
    destination_key: &str,
) -> Result<()> {
    let source = format!("{source_bucket}/{}", source_key.trim_start_matches('/'));
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("x-amz-copy-source"),
        HeaderValue::from_str(&source)
            .map_err(|_| anyhow::anyhow!("S3 copy source contains invalid header characters"))?,
    );
    destination
        .put_object_with_content_type_and_headers(
            destination_key,
            &[],
            "application/octet-stream",
            Some(headers),
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to copy object: {e}"))?;
    Ok(())
}

/// Check destination existence without treating authorization or endpoint
/// failures as absence.
pub async fn object_exists(bucket: &Bucket, key: &str) -> Result<bool> {
    match bucket.head_object(key).await {
        Ok((_, 200)) => Ok(true),
        Ok((_, 404)) | Err(S3Error::HttpFailWithBody(404, _)) => Ok(false),
        Ok((_, status)) => anyhow::bail!("Object existence check returned status {status}"),
        Err(error) => Err(anyhow::anyhow!("Failed to check object existence: {error}")),
    }
}

/// Move by verified server-side copy followed by a source identity recheck.
pub async fn move_object_verified(
    config: &S3Config,
    source_bucket: &str,
    source_key: &str,
    destination_bucket: &str,
    destination_key: &str,
    replace: bool,
) -> Result<S3Entry> {
    let source = create_bucket(config, source_bucket)?;
    let before = get_object_info(&source, source_key).await?;
    let copied = copy_object_verified(
        config,
        source_bucket,
        source_key,
        destination_bucket,
        destination_key,
        replace,
    )
    .await?;
    let current = get_object_info(&source, source_key).await?;
    if current.size != before.size || current.etag != before.etag {
        anyhow::bail!(
            "Source changed during move; copied destination retained and source was not deleted"
        );
    }
    delete(&source, source_key).await?;
    Ok(copied)
}

/// Get object metadata (HEAD request)
pub async fn get_object_info(bucket: &Bucket, key: &str) -> Result<S3Entry> {
    let (head, status) = bucket
        .head_object(key)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get object info: {}", e))?;

    if status != 200 {
        anyhow::bail!("HEAD request returned status {}", status);
    }

    let display_name = extract_display_name(key);

    Ok(S3Entry {
        key: key.to_string(),
        display_name,
        entry_type: S3EntryType::Object,
        size: head.content_length.unwrap_or(0) as u64,
        last_modified: head.last_modified,
        storage_class: None,
        etag: head.e_tag,
    })
}

/// Generate a presigned GET URL
pub async fn presign_get(bucket: &Bucket, key: &str, expire_secs: u32) -> Result<String> {
    bucket
        .presign_get(key, expire_secs, None)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to generate presigned GET URL: {}", e))
}

/// Generate a presigned PUT URL
pub async fn presign_put(bucket: &Bucket, key: &str, expire_secs: u32) -> Result<String> {
    bucket
        .presign_put(key, expire_secs, None, None)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to generate presigned PUT URL: {}", e))
}

/// Create a new bucket
pub async fn create_bucket_op(config: &S3Config, name: &str) -> Result<()> {
    let region = build_region(&config.provider)?;
    let credentials = build_credentials(&config.auth)?;

    let response = Bucket::create(name, region, credentials, BucketConfiguration::default())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create bucket: {}", e))?;

    if !response.success() {
        anyhow::bail!(
            "Failed to create bucket '{}': HTTP {}",
            name,
            response.response_code
        );
    }

    Ok(())
}

/// Delete an empty bucket
pub async fn delete_bucket_op(config: &S3Config, name: &str) -> Result<()> {
    let bucket = create_bucket(config, name)?;
    bucket
        .delete()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to delete bucket: {}", e))?;

    Ok(())
}

/// Extract the display name (last path component) from a key or prefix
fn extract_display_name(key: &str) -> String {
    let trimmed = key.trim_end_matches('/');
    trimmed.rsplit('/').next().unwrap_or(trimmed).to_string()
}

/// Recursively list all objects under a prefix (for sync operations).
/// Returns entries with relative paths from the given prefix.
pub async fn walk_remote_tree(
    bucket: &Bucket,
    base_prefix: &str,
) -> Result<Vec<crate::types::SyncEntry>> {
    let base = base_prefix.trim_end_matches('/');
    let prefix_with_slash = if base.is_empty() {
        String::new()
    } else {
        format!("{}/", base)
    };

    let results = bucket
        .list(prefix_with_slash.clone(), None)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list objects for sync: {}", e))?;

    let mut entries = Vec::new();

    for page in &results {
        for obj in &page.contents {
            let rel = if prefix_with_slash.is_empty() {
                obj.key.clone()
            } else if let Some(stripped) = obj.key.strip_prefix(&prefix_with_slash) {
                stripped.to_string()
            } else {
                continue;
            };

            if rel.is_empty() {
                continue;
            }

            let mtime = chrono::DateTime::parse_from_rfc3339(&obj.last_modified)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc));

            // S3 has no real directories; treat trailing / keys as dirs
            let is_dir = rel.ends_with('/');

            entries.push(crate::types::SyncEntry {
                rel_path: rel.trim_end_matches('/').to_string(),
                is_dir,
                size: obj.size,
                mtime,
            });
        }
    }

    Ok(entries)
}
