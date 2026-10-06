//! Directory synchronization between remote S3 and local filesystem

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::Ordering;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use voidb_core::{LocalPathError, LocalPathScope, LocalScanLimits, LocalScanReport};

use crate::s3_ops;
use crate::types::{
    S3Event, SyncAction, SyncChange, SyncContext, SyncEntry, SyncMode, SyncOptions, SyncPlan,
    SyncProgress, SyncResult,
};

/// Recursively walk a local directory tree and collect all entries.
pub fn walk_local_tree(base_path: &str) -> Result<Vec<SyncEntry>> {
    let base = Path::new(base_path);
    if !base.exists() {
        return Ok(Vec::new());
    }
    let canonical = std::fs::canonicalize(base)
        .with_context(|| "Failed to resolve the local sync directory".to_string())?;
    let scope = LocalPathScope::new(canonical).map_err(anyhow::Error::new)?;
    let (entries, _) = walk_local_tree_scoped(&scope, ".", LocalScanLimits::default())
        .map_err(anyhow::Error::new)?;
    Ok(entries)
}

pub fn walk_local_tree_scoped(
    scope: &LocalPathScope,
    relative_path: &str,
    limits: LocalScanLimits,
) -> std::result::Result<(Vec<SyncEntry>, LocalScanReport), LocalPathError> {
    let report = scope.scan_directory(relative_path, limits)?;
    let entries = report
        .entries
        .iter()
        .map(|entry| SyncEntry {
            rel_path: entry.relative_path.clone(),
            is_dir: entry.is_dir,
            size: entry.size,
            mtime: entry.modified.map(DateTime::<Utc>::from),
        })
        .collect();
    Ok((entries, report))
}

/// Compute what changes are needed to synchronize two directory trees.
pub fn compute_sync_plan(
    remote_entries: &[SyncEntry],
    local_entries: &[SyncEntry],
    options: &SyncOptions,
) -> SyncPlan {
    let remote_map: HashMap<&str, &SyncEntry> = remote_entries
        .iter()
        .map(|e| (e.rel_path.as_str(), e))
        .collect();
    let local_map: HashMap<&str, &SyncEntry> = local_entries
        .iter()
        .map(|e| (e.rel_path.as_str(), e))
        .collect();

    let mut changes = Vec::new();

    let mut all_paths: Vec<&str> = remote_map.keys().chain(local_map.keys()).copied().collect();
    all_paths.sort();
    all_paths.dedup();

    for path in all_paths {
        if matches_exclude(path, &options.exclude) {
            continue;
        }

        let remote = remote_map.get(path);
        let local = local_map.get(path);

        let action = match (remote, local, &options.mode) {
            // Remote only
            (Some(r), None, SyncMode::Pull | SyncMode::Sync) => {
                Some((SyncAction::Download, r.size, r.is_dir))
            }
            (Some(_), None, SyncMode::Push) => {
                if options.delete_extra {
                    Some((SyncAction::DeleteRemote, 0, remote.unwrap().is_dir))
                } else {
                    None
                }
            }

            // Local only
            (None, Some(l), SyncMode::Push | SyncMode::Sync) => {
                Some((SyncAction::Upload, l.size, l.is_dir))
            }
            (None, Some(_), SyncMode::Pull) => {
                if options.delete_extra {
                    Some((SyncAction::DeleteLocal, 0, local.unwrap().is_dir))
                } else {
                    None
                }
            }

            // Both exist - compare
            (Some(r), Some(l), mode) => {
                if r.is_dir && l.is_dir {
                    None
                } else {
                    compare_entries(r, l, *mode)
                }
            }

            (None, None, _) => None,
        };

        if let Some((act, size, is_dir)) = action {
            changes.push(SyncChange {
                rel_path: path.to_string(),
                action: act,
                size,
                is_dir,
            });
        }
    }

    // Sort: for creation, directories first; for deletion, files first
    changes.sort_by(|a, b| {
        let a_is_delete = matches!(a.action, SyncAction::DeleteLocal | SyncAction::DeleteRemote);
        let b_is_delete = matches!(b.action, SyncAction::DeleteLocal | SyncAction::DeleteRemote);

        match (a_is_delete, b_is_delete) {
            (true, true) => match (a.is_dir, b.is_dir) {
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                _ => a.rel_path.cmp(&b.rel_path),
            },
            (false, false) => match (a.is_dir, b.is_dir) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => a.rel_path.cmp(&b.rel_path),
            },
            (false, true) => std::cmp::Ordering::Less,
            (true, false) => std::cmp::Ordering::Greater,
        }
    });

    let total_transfer_bytes = changes
        .iter()
        .filter(|c| matches!(c.action, SyncAction::Download | SyncAction::Upload))
        .map(|c| c.size)
        .sum();

    SyncPlan {
        changes,
        total_transfer_bytes,
    }
}

/// Compare two entries that exist on both sides and determine the needed action.
fn compare_entries(
    remote: &SyncEntry,
    local: &SyncEntry,
    mode: SyncMode,
) -> Option<(SyncAction, u64, bool)> {
    match (remote.mtime, local.mtime) {
        (Some(rm), Some(lm)) => {
            let diff = rm.signed_duration_since(lm);
            if diff.num_seconds().abs() <= 2 {
                return None;
            }
            let remote_newer = rm > lm;

            match mode {
                SyncMode::Pull => {
                    if remote_newer {
                        Some((SyncAction::Download, remote.size, remote.is_dir))
                    } else {
                        None
                    }
                }
                SyncMode::Push => {
                    if !remote_newer {
                        Some((SyncAction::Upload, local.size, local.is_dir))
                    } else {
                        None
                    }
                }
                SyncMode::Sync => {
                    if remote_newer {
                        Some((SyncAction::Download, remote.size, remote.is_dir))
                    } else {
                        Some((SyncAction::Upload, local.size, local.is_dir))
                    }
                }
            }
        }
        (Some(_), None) => match mode {
            SyncMode::Pull | SyncMode::Sync => {
                Some((SyncAction::Download, remote.size, remote.is_dir))
            }
            SyncMode::Push => None,
        },
        (None, Some(_)) => match mode {
            SyncMode::Push | SyncMode::Sync => Some((SyncAction::Upload, local.size, local.is_dir)),
            SyncMode::Pull => None,
        },
        (None, None) => {
            if remote.size != local.size {
                match mode {
                    SyncMode::Pull => Some((SyncAction::Download, remote.size, remote.is_dir)),
                    SyncMode::Push => Some((SyncAction::Upload, local.size, local.is_dir)),
                    SyncMode::Sync => Some((SyncAction::Conflict, 0, false)),
                }
            } else {
                None
            }
        }
    }
}

/// Execute a sync plan, performing actual file transfers and deletions.
pub async fn execute_sync_plan(
    bucket: &s3::Bucket,
    plan: &SyncPlan,
    remote_prefix: &str,
    local_base: &str,
    ctx: &SyncContext,
) -> Result<SyncResult> {
    let mut downloaded = 0usize;
    let mut uploaded = 0usize;
    let mut deleted = 0usize;
    let mut bytes_transferred = 0u64;

    let prefix = remote_prefix.trim_end_matches('/');
    let local_base_path = Path::new(local_base);

    for (i, change) in plan.changes.iter().enumerate() {
        if ctx.cancel_flag.load(Ordering::Relaxed) {
            break;
        }

        let _ = ctx.event_tx.send(S3Event::SyncProgress(SyncProgress {
            total_changes: plan.changes.len(),
            completed: i,
            current_file: change.rel_path.clone(),
            bytes_transferred,
            total_bytes: plan.total_transfer_bytes,
        }));

        match &change.action {
            SyncAction::Download => {
                let key = if prefix.is_empty() {
                    change.rel_path.clone()
                } else {
                    format!("{}/{}", prefix, &change.rel_path)
                };
                let local_path = local_base_path.join(&change.rel_path);

                if change.is_dir {
                    std::fs::create_dir_all(&local_path)
                        .with_context(|| format!("mkdir {}", local_path.display()))?;
                } else {
                    if let Some(parent) = local_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    let data = s3_ops::download(bucket, &key).await?;
                    bytes_transferred += data.len() as u64;
                    std::fs::write(&local_path, &data)
                        .with_context(|| format!("write {}", local_path.display()))?;
                }
                downloaded += 1;
            }

            SyncAction::Upload => {
                let key = if prefix.is_empty() {
                    change.rel_path.clone()
                } else {
                    format!("{}/{}", prefix, &change.rel_path)
                };
                let local_path = local_base_path.join(&change.rel_path);

                if !change.is_dir {
                    let data = std::fs::read(&local_path)
                        .with_context(|| format!("read {}", local_path.display()))?;
                    bytes_transferred += data.len() as u64;
                    s3_ops::upload(bucket, &key, &data, None).await?;
                }
                // S3 doesn't need explicit directory creation
                uploaded += 1;
            }

            SyncAction::DeleteLocal => {
                let local_path = local_base_path.join(&change.rel_path);
                if change.is_dir {
                    let _ = std::fs::remove_dir_all(&local_path);
                } else {
                    let _ = std::fs::remove_file(&local_path);
                }
                deleted += 1;
            }

            SyncAction::DeleteRemote => {
                let key = if prefix.is_empty() {
                    change.rel_path.clone()
                } else {
                    format!("{}/{}", prefix, &change.rel_path)
                };
                s3_ops::delete(bucket, &key).await?;
                deleted += 1;
            }

            SyncAction::Conflict => {
                tracing::warn!("Sync conflict, skipping: {}", change.rel_path);
            }
        }
    }

    Ok(SyncResult {
        downloaded,
        uploaded,
        deleted,
    })
}

/// Check if a path matches any of the exclude patterns.
pub fn matches_exclude(path: &str, patterns: &[String]) -> bool {
    for pattern in patterns {
        if simple_glob_match(pattern, path) {
            return true;
        }
    }
    false
}

fn simple_glob_match(pattern: &str, path: &str) -> bool {
    let target = if pattern.contains('/') {
        path
    } else {
        path.rsplit('/').next().unwrap_or(path)
    };

    glob_match_impl(pattern, target)
}

fn glob_match_impl(pattern: &str, text: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0, 0);
    let (mut star_pi, mut star_ti) = (usize::MAX, 0);

    while ti < txt.len() {
        if pi < pat.len() && (pat[pi] == '?' || pat[pi] == txt[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pat.len() && pat[pi] == '*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < pat.len() && pat[pi] == '*' {
        pi += 1;
    }

    pi == pat.len()
}

/// Format a sync plan as a human-readable string for CLI output.
pub fn format_sync_plan(plan: &SyncPlan) -> String {
    let mut out = String::new();
    if plan.changes.is_empty() {
        out.push_str("No changes needed.\n");
        return out;
    }

    for change in &plan.changes {
        let icon = change.action.icon();
        let label = change.action.label();
        let size_str = if change.size > 0 {
            format_size(change.size)
        } else {
            String::new()
        };
        out.push_str(&format!(
            "  {} {:<8} {:<40} {}\n",
            icon, label, change.rel_path, size_str
        ));
    }

    out.push_str(&format!(
        "\nTotal: {} changes, {} to transfer\n",
        plan.changes.len(),
        format_size(plan.total_transfer_bytes)
    ));

    out
}

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
