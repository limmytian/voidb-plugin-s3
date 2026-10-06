# VoidB S3 Object Storage Plugin (`voidb-plugin-s3`)

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Independent process plugin for [VoidB](https://github.com/limmytian/voidb) to connect, inspect, and manage Amazon S3 and S3-compatible (MinIO, Cloudflare R2, Ceph) object storage.

## Features

- **Autonomous Process Architecture**: Runs in an isolated OS process communicating with VoidB via `stdio-jsonrpc`.
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
- **Dual Mode**: Can run as a JSON-RPC worker server (`voidb-plugin-s3 serve`) or standalone interactive TUI.

## Quick Start

### Installation

Place this plugin directory or a packaged release archive under your VoidB plugins directory:

```bash
mkdir -p ~/.config/voidb/plugins/s3
cp -r plugin.toml bin schemas ~/.config/voidb/plugins/s3/
```

Verify discovery via `voidb`:

```bash
voidb-cli plugin list
voidb-cli plugin describe s3
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
