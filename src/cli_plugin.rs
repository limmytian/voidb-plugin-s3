//! S3 CLI plugin for voidb-cli

use std::fs;
use std::future::Future;

use async_trait::async_trait;
use chrono::Utc;
use clap::{Arg, ArgAction, ArgMatches, Command};
use voidb_core::plugin::cli::{CliContext, CliPlugin};
use voidb_core::{
    AgentTransferEvent, AgentTransferOperation, AgentTransferPhase, ConnectionProfileRef,
    TuiLaunchRequest, VoidbError, build_tui_launch_plan,
};

use crate::config::S3Config;
use crate::service::{S3Service, SyncOutcome};
use crate::tui::{
    S3TuiLaunch, S3TuiSource, build_s3_tui_evidence, run_s3_tui, write_s3_tui_preflight,
};
use crate::types::{S3EntryType, SyncMode, SyncOptions};

pub struct S3CliPlugin;

pub fn create_s3_cli_plugin() -> Box<dyn CliPlugin> {
    Box::new(S3CliPlugin)
}

#[async_trait]
impl CliPlugin for S3CliPlugin {
    fn plugin_id(&self) -> &str {
        "s3"
    }

    fn name(&self) -> &str {
        "S3"
    }

    fn commands(&self) -> Vec<Command> {
        let conn_arg = Arg::new("connection")
            .short('c')
            .long("connection")
            .required(true)
            .help("Connection name");

        vec![
            Command::new("buckets")
                .about("List all buckets")
                .arg(conn_arg.clone()),
            Command::new("mb")
                .about("Create a new bucket")
                .arg(conn_arg.clone())
                .arg(Arg::new("name").required(true).help("Bucket name")),
            Command::new("rb")
                .about("Delete an empty bucket")
                .arg(conn_arg.clone())
                .arg(Arg::new("name").required(true).help("Bucket name")),
            Command::new("ls")
                .about("List objects in a bucket")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("path")
                        .required(true)
                        .help("Bucket/prefix path (e.g. my-bucket/prefix/)"),
                ),
            Command::new("get")
                .about("Download an object")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("remote")
                        .required(true)
                        .help("Remote path (bucket/key)"),
                )
                .arg(
                    Arg::new("local")
                        .required(true)
                        .help("Local destination path"),
                )
                .arg(overwrite_arg())
                .arg(transfer_format_arg()),
            Command::new("put")
                .about("Upload a file")
                .arg(conn_arg.clone())
                .arg(Arg::new("local").required(true).help("Local file path"))
                .arg(
                    Arg::new("remote")
                        .required(true)
                        .help("Remote destination (bucket/key)"),
                )
                .arg(overwrite_arg())
                .arg(transfer_format_arg()),
            Command::new("rm")
                .about("Delete an object")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("path")
                        .required(true)
                        .help("Remote path (bucket/key)"),
                ),
            Command::new("cp")
                .about("Copy an object, with destination verification")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("from")
                        .required(true)
                        .help("Source path (bucket/key)"),
                )
                .arg(
                    Arg::new("to")
                        .required(true)
                        .help("Destination path (bucket/key)"),
                )
                .arg(overwrite_arg())
                .arg(transfer_format_arg()),
            Command::new("mv")
                .about("Move an object after verified copy")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("from")
                        .required(true)
                        .help("Source path (bucket/key)"),
                )
                .arg(
                    Arg::new("to")
                        .required(true)
                        .help("Destination path (bucket/key)"),
                )
                .arg(overwrite_arg())
                .arg(transfer_format_arg()),
            Command::new("info")
                .about("View object properties")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("path")
                        .required(true)
                        .help("Remote path (bucket/key)"),
                ),
            Command::new("presign")
                .about("Generate a presigned URL")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("path")
                        .required(true)
                        .help("Remote path (bucket/key)"),
                )
                .arg(
                    Arg::new("put")
                        .long("put")
                        .action(ArgAction::SetTrue)
                        .help("Generate upload URL instead of download"),
                )
                .arg(
                    Arg::new("expire")
                        .long("expire")
                        .default_value("3600")
                        .help("Expiry time in seconds"),
                ),
            Command::new("test")
                .about("Test S3 connection")
                .arg(conn_arg.clone()),
            Command::new("tui")
                .about("Launch the standalone S3 browser TUI")
                .arg(
                    Arg::new("profile")
                        .long("profile")
                        .value_name("PROFILE")
                        .conflicts_with("connection")
                        .help("Profile name, id:<id>, or name:<name>"),
                )
                .arg(
                    Arg::new("connection")
                        .short('c')
                        .long("connection")
                        .value_name("CONNECTION")
                        .conflicts_with("profile")
                        .help("Legacy connection name"),
                )
                .arg(
                    Arg::new("fixture")
                        .long("fixture")
                        .value_name("PATH")
                        .help("Load deterministic S3 TUI fixture JSON instead of opening a target"),
                )
                .arg(
                    Arg::new("purpose")
                        .long("purpose")
                        .value_name("PURPOSE")
                        .default_value("browser")
                        .help("Launch purpose, for example browser or transfer"),
                )
                .arg(
                    Arg::new("readonly")
                        .long("readonly")
                        .action(ArgAction::SetTrue)
                        .help("Request read-only behavior where the TUI can enforce it"),
                )
                .arg(
                    Arg::new("no-restore")
                        .long("no-restore")
                        .action(ArgAction::SetTrue)
                        .help("Start without restoring plugin-owned UI state"),
                )
                .arg(
                    Arg::new("format")
                        .long("format")
                        .value_parser(["json"])
                        .help("Emit secret-free preflight JSON and exit"),
                )
                .arg(
                    Arg::new("evidence")
                        .long("evidence")
                        .value_name("PATH")
                        .help("Write fixture-backed standalone S3 TUI evidence JSON and exit"),
                ),
            Command::new("pull")
                .about("Sync remote prefix to local (remote -> local)")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("remote")
                        .required(true)
                        .help("Remote path (bucket/prefix/)"),
                )
                .arg(
                    Arg::new("local")
                        .required(true)
                        .help("Local directory path"),
                )
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .action(ArgAction::SetTrue)
                        .help("Preview changes without executing"),
                )
                .arg(
                    Arg::new("delete")
                        .long("delete")
                        .action(ArgAction::SetTrue)
                        .help("Delete local files not on remote"),
                )
                .arg(
                    Arg::new("exclude")
                        .long("exclude")
                        .action(ArgAction::Append)
                        .help("Glob pattern to exclude"),
                ),
            Command::new("push")
                .about("Sync local directory to remote (local -> remote)")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("local")
                        .required(true)
                        .help("Local directory path"),
                )
                .arg(
                    Arg::new("remote")
                        .required(true)
                        .help("Remote path (bucket/prefix/)"),
                )
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .action(ArgAction::SetTrue)
                        .help("Preview changes without executing"),
                )
                .arg(
                    Arg::new("delete")
                        .long("delete")
                        .action(ArgAction::SetTrue)
                        .help("Delete remote objects not in local"),
                )
                .arg(
                    Arg::new("exclude")
                        .long("exclude")
                        .action(ArgAction::Append)
                        .help("Glob pattern to exclude"),
                ),
            Command::new("sync")
                .about("Bidirectional sync between remote and local")
                .arg(conn_arg)
                .arg(
                    Arg::new("remote")
                        .required(true)
                        .help("Remote path (bucket/prefix/)"),
                )
                .arg(
                    Arg::new("local")
                        .required(true)
                        .help("Local directory path"),
                )
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .action(ArgAction::SetTrue)
                        .help("Preview changes without executing"),
                )
                .arg(
                    Arg::new("delete")
                        .long("delete")
                        .action(ArgAction::SetTrue)
                        .help("Delete files not present on either side"),
                )
                .arg(
                    Arg::new("exclude")
                        .long("exclude")
                        .action(ArgAction::Append)
                        .help("Glob pattern to exclude"),
                ),
        ]
    }

    async fn execute(
        &self,
        command: &str,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        match command {
            "buckets" => self.handle_buckets(matches, ctx).await,
            "mb" => self.handle_mb(matches, ctx).await,
            "rb" => self.handle_rb(matches, ctx).await,
            "ls" => self.handle_ls(matches, ctx).await,
            "get" => self.handle_get(matches, ctx).await,
            "put" => self.handle_put(matches, ctx).await,
            "rm" => self.handle_rm(matches, ctx).await,
            "cp" => self.handle_cp(matches, ctx).await,
            "mv" => self.handle_mv(matches, ctx).await,
            "info" => self.handle_info(matches, ctx).await,
            "presign" => self.handle_presign(matches, ctx).await,
            "test" => self.handle_test(matches, ctx).await,
            "tui" => self.handle_tui(matches, ctx).await,
            "pull" => self.handle_sync(SyncMode::Pull, matches, ctx).await,
            "push" => self.handle_sync(SyncMode::Push, matches, ctx).await,
            "sync" => self.handle_sync(SyncMode::Sync, matches, ctx).await,
            _ => Err(VoidbError::Plugin(format!("Unknown command: {}", command))),
        }
    }
}

impl S3CliPlugin {
    /// Build an `S3Service` in Direct mode from the named connection.
    fn make_service(matches: &ArgMatches, ctx: &CliContext) -> Result<S3Service, VoidbError> {
        let config = Self::get_config(matches, ctx)?;
        Ok(S3Service::new_direct(config))
    }

    fn get_config(matches: &ArgMatches, ctx: &CliContext) -> Result<S3Config, VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        Self::parse_config(conn_name, ctx)
    }

    fn parse_config(conn_name: &str, ctx: &CliContext) -> Result<S3Config, VoidbError> {
        let config = ctx
            .find_connection(conn_name)
            .ok_or_else(|| VoidbError::Plugin(format!("Connection '{}' not found", conn_name)))?;

        if config.effective_plugin_id() != "s3" {
            return Err(VoidbError::Plugin(format!(
                "Connection '{}' is not an S3 connection (plugin: {})",
                conn_name,
                config.effective_plugin_id()
            )));
        }

        let s3_config: S3Config = config
            .plugin_config
            .as_ref()
            .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
            .and_then(|pc| {
                serde_json::from_value(pc.clone())
                    .map_err(|e| VoidbError::Connection(format!("Invalid S3 config: {}", e)))
            })?;

        Ok(s3_config)
    }

    fn parse_tui_launch(matches: &ArgMatches, ctx: &CliContext) -> Result<S3TuiLaunch, VoidbError> {
        let fixture_path = matches.get_one::<String>("fixture").cloned();
        let purpose = matches
            .get_one::<String>("purpose")
            .cloned()
            .unwrap_or_else(|| "browser".to_string());
        let readonly = matches.get_flag("readonly");
        let restore = !matches.get_flag("no-restore");

        if let Some(profile_ref) = matches.get_one::<String>("profile") {
            let (profile, connection) = ctx.resolve_profile_connection(profile_ref, Some("s3"))?;
            let config = connection
                .plugin_config
                .as_ref()
                .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
                .and_then(|pc| {
                    serde_json::from_value(pc.clone())
                        .map_err(|e| VoidbError::Connection(format!("Invalid S3 config: {}", e)))
                })?;
            let profile_arg = profile_ref_arg(profile_ref, &profile.id);
            let request = TuiLaunchRequest::new("s3", profile_arg, purpose.clone())
                .readonly(readonly)
                .restore(restore)
                .raw_input(false);
            let launch_plan = build_tui_launch_plan(&profile, request, "voidb-cli", Utc::now())?;

            return Ok(S3TuiLaunch {
                profile_label: profile.name,
                config: Some(config),
                source: S3TuiSource::Profile,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: Some(launch_plan),
            });
        }

        if let Some(conn_name) = matches.get_one::<String>("connection") {
            return Ok(S3TuiLaunch {
                profile_label: conn_name.clone(),
                config: Some(Self::parse_config(conn_name, ctx)?),
                source: S3TuiSource::Connection,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: None,
            });
        }

        if fixture_path.is_some() {
            return Ok(S3TuiLaunch {
                profile_label: "fixture".to_string(),
                config: None,
                source: S3TuiSource::Fixture,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: None,
            });
        }

        Err(VoidbError::Plugin(
            "s3 tui requires --profile, --connection, or --fixture".to_string(),
        ))
    }

    async fn handle_tui(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let launch = Self::parse_tui_launch(matches, ctx)?;
        if let Some(path) = matches.get_one::<String>("evidence") {
            let evidence = build_s3_tui_evidence(&launch)
                .map_err(|e| VoidbError::Plugin(format!("S3 TUI evidence failed: {}", e)))?;
            let rendered = serde_json::to_string_pretty(&evidence).map_err(|e| {
                VoidbError::Plugin(format!("S3 TUI evidence serialization failed: {}", e))
            })?;
            if let Some(parent) = std::path::Path::new(path).parent()
                && !parent.as_os_str().is_empty()
            {
                fs::create_dir_all(parent).map_err(|e| {
                    VoidbError::Plugin(format!(
                        "Failed to create S3 TUI evidence directory '{}': {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
            fs::write(path, rendered).map_err(|e| {
                VoidbError::Plugin(format!("Failed to write S3 TUI evidence '{}': {}", path, e))
            })?;
            println!("Wrote S3 TUI evidence to {path}");
            return Ok(());
        }

        if matches
            .get_one::<String>("format")
            .is_some_and(|format| format == "json")
        {
            write_s3_tui_preflight(&launch)
                .map_err(|e| VoidbError::Plugin(format!("S3 TUI preflight failed: {}", e)))?;
            return Ok(());
        }

        run_s3_tui(launch)
            .await
            .map_err(|e| VoidbError::Plugin(format!("S3 TUI failed: {}", e)))
    }

    /// Parse "bucket/key/path" into (bucket_name, key)
    fn parse_path(path: &str) -> Result<(&str, &str), VoidbError> {
        let path = path.strip_prefix("s3://").unwrap_or(path);
        match path.split_once('/') {
            Some((bucket, key)) => Ok((bucket, key)),
            None => Ok((path, "")),
        }
    }

    async fn handle_buckets(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let buckets = svc
            .list_buckets()
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        for name in &buckets {
            println!("{}", name);
        }
        eprintln!("({} buckets)", buckets.len());
        Ok(())
    }

    async fn handle_mb(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let name = matches.get_one::<String>("name").unwrap();

        svc.create_bucket_op(name)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("Created bucket: {}", name);
        Ok(())
    }

    async fn handle_rb(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let name = matches.get_one::<String>("name").unwrap();

        svc.delete_bucket_op(name)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("Deleted bucket: {}", name);
        Ok(())
    }

    async fn handle_ls(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let path = matches.get_one::<String>("path").unwrap();
        let (bucket_name, prefix) = Self::parse_path(path)?;

        let entries = svc
            .list_objects(bucket_name, prefix)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        for entry in &entries {
            let type_indicator = match entry.entry_type {
                S3EntryType::Prefix => "[PRE] ",
                S3EntryType::Object => "      ",
            };
            let size_str = match entry.entry_type {
                S3EntryType::Prefix => String::from("-"),
                S3EntryType::Object => format_size(entry.size),
            };
            let modified = entry.last_modified.as_deref().unwrap_or("-");
            println!(
                "{}{:<40} {:>10}  {}",
                type_indicator, entry.display_name, size_str, modified
            );
        }
        eprintln!("({} entries)", entries.len());
        Ok(())
    }

    async fn handle_get(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let remote = matches.get_one::<String>("remote").unwrap();
        let local = matches.get_one::<String>("local").unwrap();
        let (bucket_name, key) = Self::parse_path(remote)?;
        if std::path::Path::new(local).exists() && !matches.get_flag("overwrite") {
            return Err(VoidbError::Plugin(
                "Local destination exists; pass --overwrite to authorize replacement.".to_string(),
            ));
        }
        let total = svc
            .get_object_info(bucket_name, key)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?
            .size;
        let data = await_transfer(matches, AgentTransferOperation::Download, total, async {
            let data = svc
                .download(bucket_name, key)
                .await
                .map_err(|e| VoidbError::Plugin(e.to_string()))?;
            std::fs::write(local, &data)
                .map_err(|e| VoidbError::Plugin(format!("Failed to write local file: {e}")))?;
            Ok(data)
        })
        .await?;

        if transfer_output_format(matches) == TransferOutputFormat::Human {
            println!(
                "Downloaded: s3://{}/{} -> {} ({})",
                bucket_name,
                key,
                local,
                format_size(data.len() as u64)
            );
        }
        Ok(())
    }

    async fn handle_put(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let local = matches.get_one::<String>("local").unwrap();
        let remote = matches.get_one::<String>("remote").unwrap();
        let (bucket_name, key) = Self::parse_path(remote)?;
        if svc
            .object_exists(bucket_name, key)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?
            && !matches.get_flag("overwrite")
        {
            return Err(VoidbError::Plugin(
                "Remote destination exists; pass --overwrite to authorize replacement.".to_string(),
            ));
        }

        let data = std::fs::read(local)
            .map_err(|e| VoidbError::Plugin(format!("Failed to read local file: {}", e)))?;

        let size = data.len() as u64;
        await_transfer(matches, AgentTransferOperation::Upload, size, async {
            svc.upload(bucket_name, key, &data)
                .await
                .map_err(|e| VoidbError::Plugin(e.to_string()))
        })
        .await?;

        if transfer_output_format(matches) == TransferOutputFormat::Human {
            println!(
                "Uploaded: {} -> s3://{}/{} ({})",
                local,
                bucket_name,
                key,
                format_size(size)
            );
        }
        Ok(())
    }

    async fn handle_rm(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let path = matches.get_one::<String>("path").unwrap();
        let (bucket_name, key) = Self::parse_path(path)?;

        svc.delete_object(bucket_name, key)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("Deleted: s3://{}/{}", bucket_name, key);
        Ok(())
    }

    async fn handle_cp(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let from = matches.get_one::<String>("from").unwrap();
        let to = matches.get_one::<String>("to").unwrap();
        let (source_bucket, src_key) = Self::parse_path(from)?;
        let (destination_bucket, dst_key) = Self::parse_path(to)?;

        let total = svc
            .get_object_info(source_bucket, src_key)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?
            .size;
        await_transfer(matches, AgentTransferOperation::Copy, total, async {
            svc.copy_object_verified(
                source_bucket,
                src_key,
                destination_bucket,
                dst_key,
                matches.get_flag("overwrite"),
            )
            .await
            .map(|_| ())
            .map_err(|e| VoidbError::Plugin(e.to_string()))
        })
        .await?;

        if transfer_output_format(matches) == TransferOutputFormat::Human {
            println!(
                "Copied: s3://{}/{} -> s3://{}/{}",
                source_bucket, src_key, destination_bucket, dst_key
            );
        }
        Ok(())
    }

    async fn handle_mv(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let from = matches.get_one::<String>("from").unwrap();
        let to = matches.get_one::<String>("to").unwrap();
        let (source_bucket, src_key) = Self::parse_path(from)?;
        let (destination_bucket, dst_key) = Self::parse_path(to)?;

        let total = svc
            .get_object_info(source_bucket, src_key)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?
            .size;
        await_transfer(matches, AgentTransferOperation::Move, total, async {
            svc.move_object_verified(
                source_bucket,
                src_key,
                destination_bucket,
                dst_key,
                matches.get_flag("overwrite"),
            )
            .await
            .map(|_| ())
            .map_err(|e| VoidbError::Plugin(e.to_string()))
        })
        .await?;

        if transfer_output_format(matches) == TransferOutputFormat::Human {
            println!(
                "Moved: s3://{}/{} -> s3://{}/{}",
                source_bucket, src_key, destination_bucket, dst_key
            );
        }
        Ok(())
    }

    async fn handle_info(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let path = matches.get_one::<String>("path").unwrap();
        let (bucket_name, key) = Self::parse_path(path)?;

        let entry = svc
            .get_object_info(bucket_name, key)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("Key:           {}", entry.key);
        println!("Bucket:        {}", bucket_name);
        println!("Size:          {}", format_size(entry.size));
        println!(
            "Last Modified: {}",
            entry.last_modified.as_deref().unwrap_or("-")
        );
        println!(
            "Storage Class: {}",
            entry.storage_class.as_deref().unwrap_or("-")
        );
        println!("ETag:          {}", entry.etag.as_deref().unwrap_or("-"));
        Ok(())
    }

    async fn handle_presign(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let path = matches.get_one::<String>("path").unwrap();
        let is_put = matches.get_flag("put");
        let expire: u32 = matches
            .get_one::<String>("expire")
            .unwrap()
            .parse()
            .map_err(|e| VoidbError::Plugin(format!("Invalid expire value: {}", e)))?;
        if expire == 0 || expire > 7 * 24 * 60 * 60 {
            return Err(VoidbError::Plugin(
                "Presigned URL expiry must be between 1 and 604800 seconds.".to_string(),
            ));
        }

        let (bucket_name, key) = Self::parse_path(path)?;

        let url = if is_put {
            svc.presign_put(bucket_name, key, expire).await
        } else {
            svc.presign_get(bucket_name, key, expire).await
        }
        .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("{}", url);
        Ok(())
    }

    async fn handle_test(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let config = Self::get_config(matches, ctx)?;
        let provider_label = config.provider.label();
        let svc = S3Service::new_direct(config);

        let buckets = svc
            .list_buckets()
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("Connection successful!");
        println!("Provider: {}", provider_label);
        println!("Accessible buckets: {}", buckets.len());
        Ok(())
    }

    async fn handle_sync(
        &self,
        mode: SyncMode,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::make_service(matches, ctx)?;
        let remote = matches.get_one::<String>("remote").unwrap();
        let local = matches.get_one::<String>("local").unwrap();
        let dry_run = matches.get_flag("dry-run");
        let delete = matches.get_flag("delete");
        let exclude: Vec<String> = matches
            .get_many::<String>("exclude")
            .map(|vals| vals.cloned().collect())
            .unwrap_or_default();

        let (bucket_name, prefix) = Self::parse_path(remote)?;

        let options = SyncOptions {
            mode,
            delete_extra: delete,
            dry_run,
            exclude,
        };

        eprintln!("Scanning remote: s3://{}/{}...", bucket_name, prefix);
        eprintln!("Scanning local: {}...", local);

        let outcome = svc
            .sync(bucket_name, prefix, local, &options)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        match outcome {
            SyncOutcome::DryRun(plan) => {
                use crate::sync_ops;
                println!("{}", sync_ops::format_sync_plan(&plan));
                println!("(dry run - no changes made)");
            }
            SyncOutcome::Result(result) => {
                println!(
                    "Sync complete: {} downloaded, {} uploaded, {} deleted",
                    result.downloaded, result.uploaded, result.deleted
                );
            }
        }

        Ok(())
    }
}

fn overwrite_arg() -> Arg {
    Arg::new("overwrite")
        .long("overwrite")
        .action(ArgAction::SetTrue)
        .help("Explicitly authorize replacement of an existing destination")
}

fn transfer_format_arg() -> Arg {
    Arg::new("format")
        .long("format")
        .value_parser(["human", "json", "ndjson"])
        .default_value("human")
        .help("Transfer progress output format")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferOutputFormat {
    Human,
    Json,
    Ndjson,
}

fn transfer_output_format(matches: &ArgMatches) -> TransferOutputFormat {
    match matches.get_one::<String>("format").map(String::as_str) {
        Some("json") => TransferOutputFormat::Json,
        Some("ndjson") => TransferOutputFormat::Ndjson,
        _ => TransferOutputFormat::Human,
    }
}

async fn await_transfer<T>(
    matches: &ArgMatches,
    operation: AgentTransferOperation,
    total: u64,
    future: impl Future<Output = Result<T, VoidbError>>,
) -> Result<T, VoidbError> {
    let transfer_id = format!(
        "s3-cli-{}-{}",
        operation.as_str(),
        Utc::now().timestamp_micros()
    );
    emit_transfer_event(
        matches,
        &AgentTransferEvent::single_object_snapshot(
            &transfer_id,
            operation,
            AgentTransferPhase::Transferring,
            1,
            0,
            Some(total),
        ),
    )?;
    tokio::select! {
        result = future => match result {
            Ok(value) => {
                emit_transfer_event(
                    matches,
                    &AgentTransferEvent::single_object_snapshot(
                        transfer_id,
                        operation,
                        AgentTransferPhase::Completed,
                        2,
                        total,
                        Some(total),
                    ),
                )?;
                Ok(value)
            }
            Err(failure) => {
                emit_transfer_event(
                    matches,
                    &AgentTransferEvent::single_object_snapshot(
                        transfer_id,
                        operation,
                        AgentTransferPhase::Failed,
                        2,
                        0,
                        Some(total),
                    ),
                )?;
                Err(failure)
            }
        },
        _ = tokio::signal::ctrl_c() => {
            emit_transfer_event(
                matches,
                &AgentTransferEvent::single_object_snapshot(
                    &transfer_id,
                    operation,
                    AgentTransferPhase::Cancelling,
                    2,
                    0,
                    Some(total),
                ),
            )?;
            emit_transfer_event(
                matches,
                &AgentTransferEvent::single_object_snapshot(
                    transfer_id,
                    operation,
                    AgentTransferPhase::Cancelled,
                    3,
                    0,
                    Some(total),
                ),
            )?;
            Err(VoidbError::Plugin(
                "S3 transfer cancelled; remote state was not assumed clean.".to_string(),
            ))
        }
    }
}

fn emit_transfer_event(matches: &ArgMatches, event: &AgentTransferEvent) -> Result<(), VoidbError> {
    match transfer_output_format(matches) {
        TransferOutputFormat::Human => {
            if event.phase != AgentTransferPhase::Completed {
                eprintln!("{}", event.human_summary());
            }
        }
        TransferOutputFormat::Json if event.terminal => {
            println!(
                "{}",
                serde_json::to_string_pretty(event).map_err(|error| {
                    VoidbError::Plugin(format!("Failed to serialize transfer event: {error}"))
                })?
            );
        }
        TransferOutputFormat::Json => {}
        TransferOutputFormat::Ndjson => {
            println!(
                "{}",
                serde_json::to_string(event).map_err(|error| {
                    VoidbError::Plugin(format!("Failed to serialize transfer event: {error}"))
                })?
            );
        }
    }
    Ok(())
}

/// Format file size in human-readable form
fn format_size(bytes: u64) -> String {
    if bytes == 0 {
        return "0 B".to_string();
    }
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;
    while size >= 1024.0 && unit_idx < units.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{} {}", bytes, units[0])
    } else {
        format!("{:.1} {}", size, units[unit_idx])
    }
}

fn profile_ref_arg(input: &str, resolved_profile_id: &str) -> ConnectionProfileRef {
    if let Some(id) = input.strip_prefix("id:") {
        ConnectionProfileRef::Id(id.to_string())
    } else if let Some(name) = input
        .strip_prefix("name:")
        .or_else(|| input.strip_prefix("alias:"))
    {
        ConnectionProfileRef::Name(name.to_string())
    } else if input == resolved_profile_id {
        ConnectionProfileRef::Id(input.to_string())
    } else {
        ConnectionProfileRef::Name(input.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_commands_expose_structured_output_and_conflict_controls() {
        let command = S3CliPlugin
            .commands()
            .into_iter()
            .find(|command| command.get_name() == "cp")
            .unwrap();
        let matches = command
            .try_get_matches_from([
                "cp",
                "--connection",
                "fixture",
                "source/a",
                "destination/b",
                "--overwrite",
                "--format",
                "ndjson",
            ])
            .expect("S3 transfer CLI arguments");
        assert!(matches.get_flag("overwrite"));
        assert_eq!(
            transfer_output_format(&matches),
            TransferOutputFormat::Ndjson
        );
    }
}
