# VoidB S3 Object Storage Plugin (`voidb-plugin-s3`)

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Independent process plugin for [VoidB](https://github.com/limmytian/voidb) to connect, inspect, and manage Amazon S3 and S3-compatible (MinIO, Cloudflare R2, Ceph) object storage.

## Features

- **Autonomous Process Architecture**: Runs in an isolated OS process communicating with VoidB via `stdio-jsonrpc`.
- **Tri-Modal Operation**:
  - `test`: Automated connectivity verification.
  - `tui`: Standalone full-screen interactive terminal interface.
  - `serve`: `stdio-jsonrpc` protocol server for AI Agents and automated orchestration.
  - Domain commands: Full bucket, object, copy, move, sync, and presigning command suite.
- **Capability Surface**:
  - `buckets`: Discover accessible S3 buckets with provider-returned metadata.
  - `list`: List objects and common prefixes in an S3 bucket with prefix/delimiter.
  - `stat`: Read metadata for one S3 object.
  - `get`: Fetch S3 object content as bounded base64.
  - `put`: Upload S3 object from inline bounded content with dry-run support.
  - `delete`: Delete one S3 object.
  - `mkdir`: Create a virtual S3 directory placeholder object.
  - `copy`: Verified object copy with destination recheck.
  - `move`: Verified object move by copy and source recheck before deletion.
  - `presign`: Create short-lived, authorized GET/PUT presigned delegation URLs.
  - `sync_plan`: Compute local/S3 sync diff plans without mutating storage.
  - `transfer` & `transfer_status`: Resumable, cancellable multipart upload/download sessions.

## Usage & Operational Modes

When installed, `voidb-cli` automatically discovers this plugin and provides seamless transparent dispatch:

### 1. Human Operations (CLI & TUI)

```bash
# Connectivity test
voidb-cli s3 test --profile my-s3

# Launch standalone interactive fullscreen TUI
voidb-cli s3 tui --profile my-s3

# Bucket and object management
voidb-cli s3 buckets --profile my-s3
voidb-cli s3 ls s3://my-bucket/prefix/ --profile my-s3
voidb-cli s3 info s3://my-bucket/key.txt --profile my-s3
voidb-cli s3 put s3://my-bucket/key.txt ./local.txt --profile my-s3
voidb-cli s3 get s3://my-bucket/key.txt ./download.txt --profile my-s3
voidb-cli s3 presign s3://my-bucket/key.txt --expires 3600 --profile my-s3
```

### 2. Machine Communication (AI Agents & JSON-RPC)

```bash
# Run the stdio-jsonrpc server directly or via voidb-cli
voidb-cli s3 serve
# Or directly run the binary:
./bin/voidb-plugin-s3 serve
```

## Quick Start

### Installation

Place this plugin directory or a packaged release archive under your VoidB plugins directory (`~/Library/Application Support/voidb/plugins/s3` on macOS, or `~/.local/share/voidb/plugins/s3` on Linux):

```bash
mkdir -p "$HOME/Library/Application Support/voidb/plugins/s3"
cp -r plugin.toml bin schemas "$HOME/Library/Application Support/voidb/plugins/s3/"
```

Verify discovery via `voidb`:

```bash
voidb-cli plugin list
voidb-cli s3 --help
```

### Development & Build

```bash
cargo build --release
mkdir -p bin
cp target/release/voidb-plugin-s3 bin/
```

## Protocol Specifications

Complies with the [VoidB Process Plugin Protocol](https://github.com/limmytian/voidb/blob/main/docs/quickstart-process-plugin.md) specification (v1.0).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Optional local SDK development

Release and CI builds resolve SDK/core from the immutable Git revision in Cargo.toml. To test against a sibling `../voidb` checkout, explicitly opt in with `cargo --config .cargo/config.local.example.toml test --all-targets`. Do not commit an active `.cargo/config.toml` path override.
