---
name: voidb-plugin-s3
description: Guide for the VoidB S3 plugin. Use when modifying crates/plugins/voidb-plugin-s3, s3.* capabilities, S3 CLI commands, object storage service/sync operations, standalone S3 TUI, provider/auth config, sync_plan behavior, or fixture smoke checks.
---

# VoidB S3 Plugin

## Start Here

Primary crate: `crates/plugins/voidb-plugin-s3`.

Inspect:

- `src/config.rs` for `S3Config`, providers, auth, endpoint, region, TLS, and path-style settings.
- `src/s3_ops.rs` for object and bucket operations.
- `src/sync_ops.rs` for pull/push/sync planning.
- `src/service/` for commands, events, and service facade.
- `src/capabilities.rs` for `s3.*` metadata and invocation.
- `src/cli_plugin.rs` for `voidb-cli s3 ...`.
- `src/tui.rs` for standalone S3 browser TUI.
- `docs/s3-release-readiness.md` and `docs/storage-tui-ux-decisions.md`.

## Boundaries

- Keep S3 provider/client details inside this plugin crate.
- Treat `put`, `delete`, `mkdir`, bucket create/delete, copy/move, and sync as policy-sensitive.
- Preserve `sync_plan` as a planning surface; do not perform writes from plan-only paths.
- Redact access keys, secret keys, tokens, endpoints where policy requires it, and presigned URL secrets.
- Keep object path normalization and traversal checks explicit.

## CLI And Capabilities

- CLI commands: `buckets`, `mb`, `rb`, `ls`, `get`, `put`, `rm`, `cp`, `mv`, `info`, `presign`, `test`, `tui`, `pull`, `push`, `sync`.
- Capabilities: `s3.list`, `s3.stat`, `s3.get`, `s3.put`, `s3.delete`, `s3.mkdir`, `s3.sync_plan`.

## Validation

- Focused gate: `cargo test -p voidb-plugin-s3`.
- Add `cargo test -p voidb-cli invoke` for capability or generic invoke changes.
- Secret-free smoke: `scripts/release-plugin-smoke.sh --plugin s3`.
- Fixture gate when feasible: `scripts/s3-fixture-smoke.sh`.
- Always run `git diff --check`.
