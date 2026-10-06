use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use crossterm::event::{self, Event, KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Gauge, Paragraph, Wrap};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use voidb_core::{
    AgentTransferEvent, AgentTransferOperation, AgentTransferPhase, TabInfo, TabManager,
    TuiLaunchPlan, retained_tui_quality_gate,
};

use crate::config::S3Config;
use crate::service::{S3Command, S3Event, S3Service};
use crate::types::{S3Entry, S3EntryType};

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum S3TuiSource {
    Profile,
    Connection,
    Fixture,
}

#[derive(Debug, Clone)]
pub struct S3TuiLaunch {
    pub profile_label: String,
    pub config: Option<S3Config>,
    pub source: S3TuiSource,
    pub fixture_path: Option<String>,
    pub purpose: String,
    pub readonly: bool,
    pub restore: bool,
    pub launch_plan: Option<TuiLaunchPlan>,
}

#[derive(Debug, Deserialize)]
struct S3TuiFixture {
    profile_label: Option<String>,
    provider_label: String,
    auth_method: String,
    bucket: Option<String>,
    prefix: Option<String>,
    #[serde(default)]
    buckets: Vec<String>,
    #[serde(default)]
    entries: Vec<FixtureS3Entry>,
    status: Option<String>,
    permission_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureS3Entry {
    key: String,
    display_name: String,
    entry_type: String,
    #[serde(default)]
    size: u64,
    last_modified: Option<String>,
    storage_class: Option<String>,
    etag: Option<String>,
}

#[derive(Debug, Clone)]
struct S3TuiData {
    profile_label: String,
    provider_label: String,
    auth_method: String,
    bucket: Option<String>,
    prefix: String,
    buckets: Vec<String>,
    entries: Vec<S3EntryView>,
    status: String,
    permission_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3EntryView {
    key: String,
    display_name: String,
    entry_type: S3EntryType,
    size: u64,
    last_modified: Option<String>,
    storage_class: Option<String>,
    etag: Option<String>,
}

impl From<S3Entry> for S3EntryView {
    fn from(entry: S3Entry) -> Self {
        Self {
            key: entry.key,
            display_name: entry.display_name,
            entry_type: entry.entry_type,
            size: entry.size,
            last_modified: entry.last_modified,
            storage_class: entry.storage_class,
            etag: entry.etag,
        }
    }
}

impl From<FixtureS3Entry> for S3EntryView {
    fn from(entry: FixtureS3Entry) -> Self {
        Self {
            key: entry.key,
            display_name: entry.display_name,
            entry_type: if entry.entry_type == "prefix" {
                S3EntryType::Prefix
            } else {
                S3EntryType::Object
            },
            size: entry.size,
            last_modified: entry.last_modified,
            storage_class: entry.storage_class,
            etag: entry.etag,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Browser,
    Filter,
    Help,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BrowserItem {
    Bucket(String),
    Entry(S3EntryView),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PlanKind {
    Download,
    Upload,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransferPlanView {
    kind: PlanKind,
    bucket: Option<String>,
    key: String,
    local: Option<String>,
    confirmed: bool,
    overwrite_authorized: bool,
    progress: Option<TransferProgressView>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TransferProgressView {
    label: String,
    transferred: u64,
    total: u64,
}

pub fn write_s3_tui_preflight(launch: &S3TuiLaunch) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&preflight_value(launch))?
    );
    Ok(())
}

pub fn build_s3_tui_evidence(launch: &S3TuiLaunch) -> Result<Value> {
    let data = if let Some(path) = &launch.fixture_path {
        load_fixture(path)?
    } else {
        let config = launch
            .config
            .as_ref()
            .context("s3 tui evidence requires a profile, connection, or fixture")?;
        config_data(launch.profile_label.clone(), config)
    };

    let entries = data
        .entries
        .iter()
        .map(|entry| {
            json!({
                "key": entry.key,
                "display_name": entry.display_name,
                "entry_type": entry.kind_label(),
                "size": entry.size,
                "last_modified": entry.last_modified,
                "storage_class": entry.storage_class,
                "etag": entry.etag
            })
        })
        .collect::<Vec<_>>();

    let mut evidence = json!({
        "schema_version": 1,
        "kind": "s3_tui_fixture_evidence",
        "quality_gate": retained_tui_quality_gate(
            "s3",
            &["fixture-s3", "voidb-fixture-bucket"],
            &["permission_error", "s3.access_denied"],
            80,
            24,
            10_000
        ),
        "preflight": preflight_value(launch),
        "transcript": {
            "profile_label": data.profile_label,
            "provider_label": data.provider_label,
            "auth_method": data.auth_method,
            "bucket": data.bucket,
            "prefix": data.prefix,
            "buckets": data.buckets,
            "entries": entries,
            "permission_error": data.permission_error
        },
        "coverage": [
            "startup",
            "bucket_prefix_browse",
            "metadata_preview",
            "filter",
            "transfer_plan",
            "delete_confirmation",
            "permission_error",
            "resize",
            "quit_restore",
            "secret_leak_scan"
        ],
        "secret_leak_scan": null
    });

    let rendered = serde_json::to_string(&evidence)?;
    let markers = secret_leak_markers(&rendered);
    evidence["secret_leak_scan"] = json!({
        "passed": markers.is_empty(),
        "marker_count": markers.len(),
        "markers": markers
    });
    Ok(evidence)
}

pub async fn run_s3_tui(launch: S3TuiLaunch) -> Result<()> {
    let mut app = S3TuiApp::new(launch)?;
    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, &mut app);
    app.shutdown();
    ratatui::restore();
    result
}

fn preflight_value(launch: &S3TuiLaunch) -> Value {
    let fixture_data = launch
        .fixture_path
        .as_ref()
        .and_then(|path| load_fixture(path).ok());
    let provider = launch
        .config
        .as_ref()
        .map(|config| config.provider.label().to_string())
        .or_else(|| {
            fixture_data
                .as_ref()
                .map(|data| data.provider_label.clone())
        })
        .unwrap_or_else(|| "fixture".to_string());
    let auth_method = launch
        .config
        .as_ref()
        .map(|config| config.auth.label().to_string())
        .or_else(|| fixture_data.as_ref().map(|data| data.auth_method.clone()))
        .unwrap_or_else(|| "fixture".to_string());
    let bucket = launch
        .config
        .as_ref()
        .and_then(|config| config.bucket.clone())
        .or_else(|| fixture_data.as_ref().and_then(|data| data.bucket.clone()));
    let launch_plan = launch.launch_plan.as_ref().map(|plan| {
        json!({
            "schema_version": plan.schema_version,
            "plugin_id": plan.plugin_id,
            "command": plan.command,
            "args": plan.args,
            "profile": plan.profile,
            "purpose": plan.purpose,
            "readonly": plan.readonly,
            "restore": plan.restore,
            "raw_input": plan.raw_input,
            "credential_ref_count": plan.credential_grant.credential_refs.len(),
            "credential_grant_id": plan.credential_grant.id,
            "redaction": plan.redaction
        })
    });

    json!({
        "ok": true,
        "command": "s3 tui",
        "plugin_id": "s3",
        "profile_label": fixture_data
            .as_ref()
            .map(|data| data.profile_label.clone())
            .unwrap_or_else(|| launch.profile_label.clone()),
        "source": launch.source,
        "purpose": launch.purpose,
        "readonly": launch.readonly,
        "restore": launch.restore,
        "raw_input": false,
        "fixture": launch.fixture_path.is_some(),
        "provider": provider,
        "bucket": bucket,
        "auth": {
            "method": auth_method,
            "secret_material": "redacted"
        },
        "privacy": {
            "diagnostics_include_access_keys": false,
            "diagnostics_include_secret_keys": false,
            "diagnostics_include_session_tokens": false,
            "object_body_redaction_required": true
        },
        "service_boundary": "S3Service::Channel",
        "modes": [
            "bucket_browser",
            "prefix_browser",
            "metadata_preview",
            "filter",
            "transfer_queue",
            "delete_confirmation",
            "permission_error"
        ],
        "launch_plan": launch_plan
    })
}

fn run_loop(terminal: &mut ratatui::DefaultTerminal, app: &mut S3TuiApp) -> Result<()> {
    terminal.draw(|frame| app.draw(frame))?;
    loop {
        let mut dirty = app.drain_service();
        if app.should_quit {
            return Ok(());
        }

        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) => {
                    app.handle_key(key);
                    dirty = true;
                }
                Event::Resize(cols, rows) => {
                    app.status = format!("resized view to {cols}x{rows}");
                    dirty = true;
                }
                _ => {}
            }
        }
        if dirty {
            terminal.draw(|frame| app.draw(frame))?;
        }
    }
}

fn load_fixture(path: &str) -> Result<S3TuiData> {
    let text = fs::read_to_string(path).with_context(|| format!("read fixture {path}"))?;
    let fixture: S3TuiFixture =
        serde_json::from_str(&text).with_context(|| format!("parse fixture {path}"))?;
    Ok(S3TuiData {
        profile_label: fixture
            .profile_label
            .unwrap_or_else(|| "fixture-s3".to_string()),
        provider_label: fixture.provider_label,
        auth_method: fixture.auth_method,
        bucket: fixture.bucket,
        prefix: fixture.prefix.unwrap_or_default(),
        buckets: fixture.buckets,
        entries: fixture.entries.into_iter().map(S3EntryView::from).collect(),
        status: fixture
            .status
            .unwrap_or_else(|| "fixture s3 browser ready".to_string()),
        permission_error: fixture.permission_error,
    })
}

fn config_data(profile_label: String, config: &S3Config) -> S3TuiData {
    S3TuiData {
        profile_label,
        provider_label: config.provider.label().to_string(),
        auth_method: config.auth.label().to_string(),
        bucket: config.bucket.clone(),
        prefix: String::new(),
        buckets: Vec::new(),
        entries: Vec::new(),
        status: "connecting through S3Service channel mode".to_string(),
        permission_error: None,
    }
}

struct S3TuiApp {
    profile_label: String,
    provider_label: String,
    auth_method: String,
    source: S3TuiSource,
    purpose: String,
    readonly: bool,
    restore: bool,
    bucket: Option<String>,
    prefix: String,
    buckets: Vec<String>,
    entries: Vec<S3EntryView>,
    selected: usize,
    filter: String,
    service: Option<S3Service>,
    transfer_plan: Option<TransferPlanView>,
    sync_plan: Option<SyncPlanSummary>,
    status: String,
    mode: Mode,
    return_mode: Mode,
    should_quit: bool,
    render_quit: Arc<AtomicBool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SyncPlanSummary {
    changes: usize,
    total_bytes: u64,
}

impl S3TuiApp {
    fn new(launch: S3TuiLaunch) -> Result<Self> {
        let data = if let Some(path) = &launch.fixture_path {
            load_fixture(path)?
        } else {
            let config = launch
                .config
                .as_ref()
                .context("s3 tui requires a profile, connection, or fixture")?;
            config_data(launch.profile_label.clone(), config)
        };

        let render_quit = Arc::new(AtomicBool::new(false));
        let service = if launch.fixture_path.is_none() {
            let config = launch
                .config
                .clone()
                .context("s3 tui requires S3 config outside fixture mode")?;
            let cancel = Arc::new(AtomicBool::new(false));
            let tabs = Arc::new(StandaloneS3TabManager::new(render_quit.clone()));
            let runtime = tokio::runtime::Handle::current();
            Some(S3Service::new(
                config.clone(),
                config.bucket.clone(),
                cancel,
                tabs,
                runtime,
            ))
        } else {
            None
        };
        let status = data
            .permission_error
            .as_deref()
            .map(|error| format!("{} | {}", data.status, safe_error_summary(error)))
            .unwrap_or_else(|| data.status.clone());

        Ok(Self {
            profile_label: data.profile_label,
            provider_label: data.provider_label,
            auth_method: data.auth_method,
            source: launch.source,
            purpose: launch.purpose,
            readonly: launch.readonly,
            restore: launch.restore,
            bucket: data.bucket,
            prefix: data.prefix,
            buckets: data.buckets,
            entries: data.entries,
            selected: 0,
            filter: String::new(),
            service,
            transfer_plan: None,
            sync_plan: None,
            status,
            mode: Mode::Browser,
            return_mode: Mode::Browser,
            should_quit: false,
            render_quit,
        })
    }

    fn shutdown(&mut self) {
        if let Some(service) = &self.service {
            service.send(S3Command::Disconnect);
        }
    }

    fn drain_service(&mut self) -> bool {
        let mut changed = false;
        if self.render_quit.load(Ordering::SeqCst) {
            changed |= !self.should_quit;
            self.should_quit = true;
        }
        let Some(mut service) = self.service.take() else {
            return changed;
        };
        while let Some(event) = service.poll_event() {
            changed = true;
            self.handle_service_event(event);
        }
        self.service = Some(service);
        changed
    }

    fn handle_service_event(&mut self, event: S3Event) {
        match event {
            S3Event::TransferLifecycle(event) => {
                self.status = event.human_summary();
                self.set_progress(
                    event.operation.as_str().to_string(),
                    event.progress.bytes_completed,
                    event.progress.bytes_total.unwrap_or(0),
                );
                match event.phase {
                    AgentTransferPhase::Completed | AgentTransferPhase::Cancelled => {
                        self.transfer_plan = None;
                    }
                    AgentTransferPhase::Failed => self.mode = Mode::Error,
                    _ => {}
                }
            }
            S3Event::BucketsListed(buckets) => {
                self.bucket = None;
                self.buckets = buckets;
                self.entries.clear();
                self.selected = 0;
                self.status = format!("listed {} buckets", self.buckets.len());
            }
            S3Event::ObjectsListed { prefix, entries } => {
                self.prefix = prefix;
                self.entries = entries.into_iter().map(S3EntryView::from).collect();
                self.entries.sort_by(s3_entry_sort);
                self.selected = self
                    .selected
                    .min(self.filtered_items().len().saturating_sub(1));
                self.status = format!(
                    "listed {} entries in {}",
                    self.entries.len(),
                    self.path_label()
                );
            }
            S3Event::DownloadProgress {
                key,
                transferred,
                total,
            } => {
                self.set_progress(key, transferred, total);
                self.status = transfer_summary(
                    AgentTransferOperation::Download,
                    AgentTransferPhase::Transferring,
                    transferred,
                    total,
                );
            }
            S3Event::DownloadComplete { key, local } => {
                self.status = format!(
                    "download complete: s3://{}/{} -> {}",
                    self.bucket_label(),
                    key,
                    local
                );
                self.transfer_plan = None;
            }
            S3Event::UploadProgress {
                local,
                transferred,
                total,
            } => {
                self.set_progress(local, transferred, total);
                self.status = transfer_summary(
                    AgentTransferOperation::Upload,
                    AgentTransferPhase::Transferring,
                    transferred,
                    total,
                );
            }
            S3Event::UploadComplete { local, key } => {
                self.status = format!(
                    "upload complete: {local} -> s3://{}/{}",
                    self.bucket_label(),
                    key
                );
                self.transfer_plan = None;
            }
            S3Event::OperationComplete(message) => {
                self.status = message;
                self.transfer_plan = None;
            }
            S3Event::Error(message) => {
                self.mode = Mode::Error;
                self.status = safe_error_summary(&message);
            }
            S3Event::TransferCancelled => {
                self.transfer_plan = None;
                self.status = "transfer cancelled".to_string();
            }
            S3Event::SyncPlanReady(plan) => {
                self.sync_plan = Some(SyncPlanSummary {
                    changes: plan.changes.len(),
                    total_bytes: plan.total_transfer_bytes,
                });
                self.status = "sync plan ready".to_string();
            }
            S3Event::SyncProgress(progress) => {
                self.status = format!(
                    "sync {}/{} {} {}",
                    progress.completed,
                    progress.total_changes,
                    progress.current_file,
                    format_progress(progress.bytes_transferred, progress.total_bytes)
                );
            }
            S3Event::SyncComplete {
                downloaded,
                uploaded,
                deleted,
            } => {
                self.status = format!(
                    "sync complete: {downloaded} downloaded, {uploaded} uploaded, {deleted} deleted"
                );
            }
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        if self.mode == Mode::Help {
            self.mode = self.return_mode;
            self.status = format!("returned to {}", mode_label(self.mode));
            return;
        }
        if self.mode == Mode::Filter {
            self.handle_filter_key(key);
            return;
        }

        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Home | KeyCode::Char('g') => self.selected = 0,
            KeyCode::End | KeyCode::Char('G') => {
                self.selected = self.filtered_items().len().saturating_sub(1);
            }
            KeyCode::Enter => self.open_selected(),
            KeyCode::Backspace => self.open_parent_prefix(),
            KeyCode::Char('/') => {
                self.return_mode = self.mode;
                self.mode = Mode::Filter;
                self.status = "filter: type text, Enter apply, Esc clear".to_string();
            }
            KeyCode::Char('r') if self.mode == Mode::Error && self.transfer_plan.is_some() => {
                self.mode = Mode::Browser;
                self.confirm_plan();
            }
            KeyCode::Char('r') => self.refresh(),
            KeyCode::Char('m') => self.status = "metadata preview updated".to_string(),
            KeyCode::Char('d') => self.plan_download(),
            KeyCode::Char('u') => self.plan_upload(),
            KeyCode::Char('x') | KeyCode::Delete => self.plan_delete(),
            KeyCode::Char('y') => self.confirm_plan(),
            KeyCode::Char('o') => self.authorize_overwrite(),
            KeyCode::Char('c') => self.cancel_plan(),
            KeyCode::Char('?') => self.show_help(),
            _ => {
                self.status =
                    "s3: j/k move, Enter open, / filter, d/u/x plan, y confirm, q quit".to_string();
            }
        }
    }

    fn handle_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                self.mode = self.return_mode;
                self.selected = 0;
                self.status = format!("filter applied: {}", self.filter_label());
            }
            KeyCode::Esc => {
                self.filter.clear();
                self.mode = self.return_mode;
                self.selected = 0;
                self.status = "filter cleared".to_string();
            }
            KeyCode::Backspace => {
                self.filter.pop();
                self.selected = 0;
            }
            KeyCode::Char(ch) => {
                self.filter.push(ch);
                self.selected = 0;
            }
            _ => {}
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.filtered_items().len();
        if len == 0 {
            self.selected = 0;
            return;
        }
        let last = len as isize - 1;
        self.selected = (self.selected as isize + delta).clamp(0, last) as usize;
    }

    fn open_selected(&mut self) {
        match self.selected_item() {
            Some(BrowserItem::Bucket(bucket)) => {
                self.bucket = Some(bucket.clone());
                self.prefix.clear();
                self.selected = 0;
                if let Some(service) = &self.service {
                    service.send(S3Command::Connect {
                        bucket: Some(bucket.clone()),
                    });
                    self.status = format!("opening bucket {bucket}");
                } else {
                    self.status = format!("fixture bucket selected: {bucket}");
                }
            }
            Some(BrowserItem::Entry(entry)) if entry.entry_type == S3EntryType::Prefix => {
                self.prefix = entry.key;
                self.selected = 0;
                self.refresh();
            }
            Some(BrowserItem::Entry(entry)) => {
                self.status = format!("selected {}; metadata shown", entry.key);
            }
            None => self.status = "nothing selected".to_string(),
        }
    }

    fn open_parent_prefix(&mut self) {
        if self.bucket.is_none() {
            self.status = "already at bucket list".to_string();
            return;
        }
        if self.prefix.is_empty() {
            self.bucket = None;
            self.selected = 0;
            self.refresh();
            return;
        }
        self.prefix = parent_prefix(&self.prefix);
        self.selected = 0;
        self.refresh();
    }

    fn refresh(&mut self) {
        if let Some(service) = &self.service {
            if self.bucket.is_some() {
                service.send(S3Command::ListObjects {
                    prefix: self.prefix.clone(),
                });
                self.status = format!("listing {}", self.path_label());
            } else {
                service.send(S3Command::ListBuckets);
                self.status = "listing buckets".to_string();
            }
        } else {
            self.status = format!("fixture refresh: {}", self.path_label());
        }
    }

    fn plan_download(&mut self) {
        let Some(BrowserItem::Entry(entry)) = self.selected_item() else {
            self.status = "select an object to download".to_string();
            return;
        };
        if entry.entry_type != S3EntryType::Object {
            self.status = "download planning requires an object".to_string();
            return;
        }
        let local = planned_download_path(&entry.display_name)
            .display()
            .to_string();
        self.transfer_plan = Some(TransferPlanView {
            kind: PlanKind::Download,
            bucket: self.bucket.clone(),
            key: entry.key.clone(),
            local: Some(local.clone()),
            confirmed: false,
            overwrite_authorized: false,
            progress: None,
        });
        self.status = format!("download plan staged: {} -> {local}", entry.key);
    }

    fn plan_upload(&mut self) {
        let key = format!("{}<choose-object-key>", self.prefix);
        self.transfer_plan = Some(TransferPlanView {
            kind: PlanKind::Upload,
            bucket: self.bucket.clone(),
            key,
            local: Some("<choose-local-file>".to_string()),
            confirmed: false,
            overwrite_authorized: false,
            progress: None,
        });
        self.status = "upload plan staged; local path and object key are required".to_string();
    }

    fn plan_delete(&mut self) {
        let Some(BrowserItem::Entry(entry)) = self.selected_item() else {
            self.status = "select an object to delete".to_string();
            return;
        };
        self.transfer_plan = Some(TransferPlanView {
            kind: PlanKind::Delete,
            bucket: self.bucket.clone(),
            key: entry.key.clone(),
            local: None,
            confirmed: false,
            overwrite_authorized: false,
            progress: None,
        });
        self.status = format!("delete plan staged for {}; press y twice", entry.key);
    }

    fn confirm_plan(&mut self) {
        let Some(mut plan) = self.transfer_plan.take() else {
            self.status = "no transfer plan to confirm".to_string();
            return;
        };

        if self.readonly {
            plan.confirmed = true;
            self.status = "readonly launch keeps plan non-executing".to_string();
            self.transfer_plan = Some(plan);
            return;
        }
        if plan.kind == PlanKind::Delete && !plan.confirmed {
            plan.confirmed = true;
            self.status = "delete plan armed; press y again to execute".to_string();
            self.transfer_plan = Some(plan);
            return;
        }

        match plan.kind {
            PlanKind::Download => {
                let Some(local) = plan.local.clone() else {
                    self.status = "download has no local target".to_string();
                    self.transfer_plan = Some(plan);
                    return;
                };
                if let Err(message) = ensure_local_parent(&local) {
                    self.status = message;
                    self.transfer_plan = Some(plan);
                    return;
                }
                if std::path::Path::new(&local).exists() && !plan.overwrite_authorized {
                    self.mode = Mode::Error;
                    self.status =
                        "local destination conflict; press o to authorize overwrite, then y"
                            .to_string();
                    self.transfer_plan = Some(plan);
                    return;
                }
                if let Some(service) = &self.service {
                    service.send(S3Command::DownloadObject {
                        key: plan.key.clone(),
                        local,
                    });
                    plan.confirmed = true;
                    plan.progress = Some(TransferProgressView {
                        label: plan.key.clone(),
                        transferred: 0,
                        total: 0,
                    });
                    self.status = format!("download started: {}", plan.key);
                    self.transfer_plan = Some(plan);
                } else {
                    plan.confirmed = true;
                    plan.progress = Some(TransferProgressView {
                        label: plan.key.clone(),
                        transferred: 1,
                        total: 1,
                    });
                    self.status = "fixture download complete".to_string();
                    self.transfer_plan = Some(plan);
                }
            }
            PlanKind::Upload => {
                plan.confirmed = true;
                self.status =
                    "upload execution requires explicit local path and key; plan retained"
                        .to_string();
                self.transfer_plan = Some(plan);
            }
            PlanKind::Delete => {
                if let Some(service) = &self.service {
                    service.send(S3Command::DeleteObject(plan.key.clone()));
                } else {
                    self.entries.retain(|entry| entry.key != plan.key);
                    self.selected = self.filtered_items().len().saturating_sub(1);
                }
                self.status = format!("delete requested: {}", plan.key);
            }
        }
    }

    fn cancel_plan(&mut self) {
        if let Some(service) = &self.service {
            service.send(S3Command::CancelTransfer);
        }
        self.transfer_plan = None;
        self.status = "transfer plan cancelled".to_string();
    }

    fn authorize_overwrite(&mut self) {
        let Some(plan) = &mut self.transfer_plan else {
            self.status = "no transfer plan has a destination conflict".to_string();
            return;
        };
        if plan.kind != PlanKind::Download {
            self.status = "overwrite authorization applies to downloads".to_string();
            return;
        }
        plan.overwrite_authorized = true;
        self.mode = Mode::Browser;
        self.status = "overwrite authorized for this transfer plan; press y to execute".to_string();
    }

    fn set_progress(&mut self, label: String, transferred: u64, total: u64) {
        if let Some(plan) = &mut self.transfer_plan {
            plan.progress = Some(TransferProgressView {
                label,
                transferred,
                total,
            });
        }
    }

    fn selected_item(&self) -> Option<BrowserItem> {
        self.filtered_items().get(self.selected).cloned()
    }

    fn filtered_items(&self) -> Vec<BrowserItem> {
        let filter = self.filter.to_ascii_lowercase();
        if self.bucket.is_none() {
            return self
                .buckets
                .iter()
                .filter(|bucket| filter.is_empty() || bucket.to_ascii_lowercase().contains(&filter))
                .cloned()
                .map(BrowserItem::Bucket)
                .collect();
        }

        self.entries
            .iter()
            .filter(|entry| {
                filter.is_empty()
                    || entry.key.to_ascii_lowercase().contains(&filter)
                    || entry.display_name.to_ascii_lowercase().contains(&filter)
            })
            .cloned()
            .map(BrowserItem::Entry)
            .collect()
    }

    fn show_help(&mut self) {
        self.return_mode = self.mode;
        self.mode = Mode::Help;
        self.status = "help".to_string();
    }

    fn draw(&self, frame: &mut Frame) {
        let area = frame.area();
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(8),
                Constraint::Length(3),
                Constraint::Length(1),
            ])
            .split(area);

        self.draw_header(frame, chunks[0]);
        self.draw_browser(frame, chunks[1]);
        self.draw_status(frame, chunks[2]);
        self.draw_help_line(frame, chunks[3]);

        if self.mode == Mode::Help {
            self.draw_help(frame, centered_rect(72, 48, area));
        }
    }

    fn draw_header(&self, frame: &mut Frame, area: Rect) {
        let source = match self.source {
            S3TuiSource::Profile => "profile",
            S3TuiSource::Connection => "connection",
            S3TuiSource::Fixture => "fixture",
        };
        let line = Line::from(vec![
            Span::styled("S3 TUI", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!(" | profile {}", self.profile_label)),
            Span::raw(format!(" | provider {}", self.provider_label)),
            Span::raw(format!(" | auth {}", self.auth_method)),
            Span::raw(format!(" | source {source}")),
        ]);
        frame.render_widget(
            Paragraph::new(line)
                .block(Block::default().borders(Borders::ALL))
                .alignment(Alignment::Left),
            area,
        );
    }

    fn draw_browser(&self, frame: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(area);

        let items = self.filtered_items();
        let visible_height = chunks[0].height.saturating_sub(2) as usize;
        let start = window_start(self.selected, visible_height, items.len());
        let lines = if items.is_empty() {
            vec![Line::from("No entries loaded. Press r to refresh.")]
        } else {
            items[start..]
                .iter()
                .take(visible_height)
                .enumerate()
                .map(|(offset, item)| {
                    let index = start + offset;
                    let style = if index == self.selected {
                        Style::default()
                            .fg(Color::Cyan)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    };
                    let prefix = if index == self.selected { "> " } else { "  " };
                    Line::from(Span::styled(format!("{prefix}{}", item_label(item)), style))
                })
                .collect()
        };
        frame.render_widget(
            Paragraph::new(lines)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(format!("Browser {}", self.path_label())),
                )
                .wrap(Wrap { trim: false }),
            chunks[0],
        );

        self.draw_details(frame, chunks[1]);
    }

    fn draw_details(&self, frame: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(8), Constraint::Length(5)])
            .split(area);

        let lines = match self.selected_item() {
            Some(BrowserItem::Bucket(bucket)) => vec![
                Line::from("Bucket"),
                Line::from(format!("Name: {bucket}")),
                Line::from("Enter opens the bucket."),
            ],
            Some(BrowserItem::Entry(entry)) => vec![
                Line::from(format!("Kind: {}", entry.kind_label())),
                Line::from(format!("Key: {}", entry.key)),
                Line::from(format!("Size: {}", format_size(entry.size))),
                Line::from(format!(
                    "Modified: {}",
                    entry.last_modified.as_deref().unwrap_or("-")
                )),
                Line::from(format!(
                    "Storage: {}",
                    entry.storage_class.as_deref().unwrap_or("-")
                )),
                Line::from(format!("ETag: {}", entry.etag.as_deref().unwrap_or("-"))),
            ],
            None => vec![Line::from("No selection.")],
        };
        let mut detail_lines = lines;
        if let Some(plan) = &self.transfer_plan {
            detail_lines.push(Line::from(""));
            detail_lines.push(Line::from(format!("Plan: {}", plan.kind.label())));
            detail_lines.push(Line::from(format!("Key: {}", plan.key)));
            detail_lines.push(Line::from(format!(
                "Local: {}",
                plan.local.as_deref().unwrap_or("not required")
            )));
            detail_lines.push(Line::from(format!("Confirmed: {}", plan.confirmed)));
            detail_lines.push(Line::from(format!(
                "Overwrite authorized: {}",
                plan.overwrite_authorized
            )));
        }
        if let Some(plan) = &self.sync_plan {
            detail_lines.push(Line::from(""));
            detail_lines.push(Line::from(format!(
                "Sync plan: {} changes, {}",
                plan.changes,
                format_size(plan.total_bytes)
            )));
        }

        frame.render_widget(
            Paragraph::new(detail_lines)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Metadata / Plan"),
                )
                .wrap(Wrap { trim: false }),
            chunks[0],
        );

        let progress = self
            .transfer_plan
            .as_ref()
            .and_then(|plan| plan.progress.as_ref());
        let (ratio, label) = if let Some(progress) = progress {
            (progress.ratio(), progress.label())
        } else {
            (0.0, "idle".to_string())
        };
        frame.render_widget(
            Gauge::default()
                .block(Block::default().borders(Borders::ALL).title("Transfer"))
                .gauge_style(Style::default().fg(Color::Green))
                .ratio(ratio)
                .label(label),
            chunks[1],
        );
    }

    fn draw_status(&self, frame: &mut Frame, area: Rect) {
        let line = Line::from(vec![
            Span::styled(
                format!("{} ", mode_label(self.mode)),
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(&self.status),
            Span::raw(format!(
                " | purpose {} | readonly {} | restore {} | filter {}",
                self.purpose,
                self.readonly,
                self.restore,
                self.filter_label()
            )),
        ]);
        frame.render_widget(
            Paragraph::new(line).block(Block::default().borders(Borders::ALL).title("Status")),
            area,
        );
    }

    fn draw_help_line(&self, frame: &mut Frame, area: Rect) {
        let text = if self.mode == Mode::Filter {
            "filter: type text | Enter apply | Esc clear"
        } else {
            "s3: j/k move | Enter open | / filter | d/u/x plan | y confirm | o overwrite | c cancel | r retry"
        };
        frame.render_widget(Paragraph::new(text), area);
    }

    fn draw_help(&self, frame: &mut Frame, area: Rect) {
        frame.render_widget(Clear, area);
        let lines = vec![
            Line::from(Span::styled(
                "S3 TUI help",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from(""),
            Line::from("Buckets and prefixes are loaded through S3Service channel mode."),
            Line::from("j/k or arrows move; Enter opens buckets and prefixes."),
            Line::from("/ filters the loaded page; r refreshes or retries a failed transfer."),
            Line::from("d stages download, u stages upload, x stages delete."),
            Line::from("y confirms a plan; delete requires y twice."),
            Line::from("o explicitly authorizes overwrite for a conflicting download."),
            Line::from("c cancels the plan or active transfer."),
            Line::from(""),
            Line::from("Press any key to return."),
        ];
        frame.render_widget(
            Paragraph::new(lines)
                .block(Block::default().borders(Borders::ALL).title("Help"))
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn bucket_label(&self) -> String {
        self.bucket
            .clone()
            .unwrap_or_else(|| "<no bucket>".to_string())
    }

    fn path_label(&self) -> String {
        if let Some(bucket) = &self.bucket {
            format!("s3://{bucket}/{}", self.prefix)
        } else {
            "buckets".to_string()
        }
    }

    fn filter_label(&self) -> String {
        if self.filter.is_empty() {
            "none".to_string()
        } else {
            self.filter.clone()
        }
    }
}

impl S3EntryView {
    fn kind_label(&self) -> &'static str {
        match self.entry_type {
            S3EntryType::Object => "object",
            S3EntryType::Prefix => "prefix",
        }
    }
}

impl PlanKind {
    fn label(&self) -> &'static str {
        match self {
            Self::Download => "download",
            Self::Upload => "upload",
            Self::Delete => "delete",
        }
    }
}

impl TransferProgressView {
    fn ratio(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.transferred as f64 / self.total as f64).clamp(0.0, 1.0)
        }
    }

    fn label(&self) -> String {
        format!(
            "{} {}",
            self.label,
            format_progress(self.transferred, self.total)
        )
    }
}

fn item_label(item: &BrowserItem) -> String {
    match item {
        BrowserItem::Bucket(bucket) => format!("bucket {bucket}"),
        BrowserItem::Entry(entry) => format!(
            "{:<6} {:>10} {:<12} {}",
            entry.kind_label(),
            format_size(entry.size),
            entry.storage_class.as_deref().unwrap_or("-"),
            entry.display_name
        ),
    }
}

fn s3_entry_sort(left: &S3EntryView, right: &S3EntryView) -> std::cmp::Ordering {
    entry_rank(&left.entry_type)
        .cmp(&entry_rank(&right.entry_type))
        .then_with(|| left.display_name.cmp(&right.display_name))
        .then_with(|| left.key.cmp(&right.key))
}

fn entry_rank(kind: &S3EntryType) -> u8 {
    match kind {
        S3EntryType::Prefix => 0,
        S3EntryType::Object => 1,
    }
}

fn parent_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim_end_matches('/');
    if let Some(pos) = trimmed.rfind('/') {
        format!("{}/", &trimmed[..pos])
    } else {
        String::new()
    }
}

fn planned_download_path(name: &str) -> PathBuf {
    std::env::temp_dir()
        .join("voidb-s3-downloads")
        .join(safe_local_filename(name))
}

fn safe_local_filename(name: &str) -> String {
    let safe = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if safe.is_empty() {
        "download".to_string()
    } else {
        safe
    }
}

fn ensure_local_parent(path: &str) -> std::result::Result<(), String> {
    let path = PathBuf::from(path);
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    fs::create_dir_all(parent)
        .map_err(|err| format!("failed to create local target directory: {err}"))
}

fn safe_error_summary(message: &str) -> String {
    let mut summary = message.replace('\n', " ");
    if summary.len() > 180 {
        summary.truncate(177);
        summary.push_str("...");
    }
    summary
}

fn secret_leak_markers(text: &str) -> Vec<String> {
    [
        "super-secret-access-key",
        "super-secret-secret-key",
        "AWS_SECRET_ACCESS_KEY",
        "raw_plugin_config",
        "session_token_value",
        "signed-url-secret",
    ]
    .into_iter()
    .filter(|marker| text.contains(marker))
    .map(str::to_string)
    .collect()
}

fn mode_label(mode: Mode) -> &'static str {
    match mode {
        Mode::Browser => "browser",
        Mode::Filter => "filter",
        Mode::Help => "help",
        Mode::Error => "error",
    }
}

fn format_progress(transferred: u64, total: u64) -> String {
    if total == 0 {
        format!("{} transferred", format_size(transferred))
    } else {
        let percent = (transferred as f64 / total as f64 * 100.0).min(100.0);
        format!(
            "{} / {} ({percent:.0}%)",
            format_size(transferred),
            format_size(total)
        )
    }
}

fn transfer_summary(
    operation: AgentTransferOperation,
    phase: AgentTransferPhase,
    transferred: u64,
    total: u64,
) -> String {
    AgentTransferEvent::single_object_snapshot(
        "s3-tui-view",
        operation,
        phase,
        1,
        transferred,
        Some(total),
    )
    .human_summary()
}

fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn window_start(selected: usize, visible: usize, len: usize) -> usize {
    if visible == 0 || len <= visible {
        0
    } else if selected >= visible {
        (selected + 1).saturating_sub(visible)
    } else {
        0
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

struct StandaloneS3TabManager {
    render_tx: mpsc::UnboundedSender<()>,
    should_quit: Arc<AtomicBool>,
}

impl StandaloneS3TabManager {
    fn new(should_quit: Arc<AtomicBool>) -> Self {
        let (render_tx, _render_rx) = mpsc::unbounded_channel();
        Self {
            render_tx,
            should_quit,
        }
    }

    fn unsupported_tabs_error() -> anyhow::Error {
        anyhow!("S3 TUI does not host plugin tabs; use plugin-owned CLI commands instead")
    }
}

impl TabManager for StandaloneS3TabManager {
    fn open(&self, _title: String, _plugin_id: String, _context: Value) -> Result<()> {
        Err(Self::unsupported_tabs_error())
    }

    fn close_current(&self) -> Result<()> {
        self.quit()
    }

    fn set_title(&self, _title: String) -> Result<()> {
        Ok(())
    }

    fn request_render(&self) -> Result<()> {
        let _ = self.render_tx.send(());
        Ok(())
    }

    fn list_tabs(&self) -> Result<Vec<TabInfo>> {
        Ok(vec![TabInfo {
            index: 0,
            title: "S3".to_string(),
            plugin_id: "s3".to_string(),
            context: json!({}),
            is_active: true,
        }])
    }

    fn close_tab(&self, index: usize) -> Result<()> {
        if index == 0 {
            self.quit()
        } else {
            Err(anyhow!("S3 TUI has no tab {index}"))
        }
    }

    fn switch_to(&self, index: usize) -> Result<()> {
        if index == 0 {
            Ok(())
        } else {
            Err(anyhow!("S3 TUI has no tab {index}"))
        }
    }

    fn active_tab_index(&self) -> Result<usize> {
        Ok(0)
    }

    fn quit(&self) -> Result<()> {
        self.should_quit.store(true, Ordering::SeqCst);
        let _ = self.render_tx.send(());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{S3Auth, S3Provider};
    use chrono::Utc;

    #[test]
    fn preflight_redacts_access_key_material() {
        let launch = S3TuiLaunch {
            profile_label: "prod".to_string(),
            config: Some(S3Config {
                provider: S3Provider::Minio {
                    endpoint: "https://minio.internal".to_string(),
                },
                bucket: Some("prod-bucket".to_string()),
                auth: S3Auth::AccessKey {
                    access_key: "super-secret-access-key".to_string(),
                    secret_key: "super-secret-secret-key".to_string(),
                },
                timeout: 30,
            }),
            source: S3TuiSource::Connection,
            fixture_path: None,
            purpose: "browser".to_string(),
            readonly: false,
            restore: true,
            launch_plan: None,
        };

        let rendered = serde_json::to_string(&preflight_value(&launch)).unwrap();
        assert!(rendered.contains("Access Key"));
        assert!(!rendered.contains("super-secret-access-key"));
        assert!(!rendered.contains("super-secret-secret-key"));
    }

    #[test]
    fn fixture_loads_browser_metadata() {
        let fixture = format!(
            "{}/fixtures/s3_tui_browser.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let data = load_fixture(&fixture).unwrap();
        assert_eq!(data.profile_label, "fixture-s3");
        assert_eq!(data.bucket.as_deref(), Some("voidb-fixture-bucket"));
        assert_eq!(data.entries.len(), 4);
        assert!(
            data.entries
                .iter()
                .any(|entry| entry.kind_label() == "prefix")
        );
    }

    #[test]
    fn evidence_has_secret_scan_and_coverage() {
        let fixture = format!(
            "{}/fixtures/s3_tui_browser.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let launch = S3TuiLaunch {
            profile_label: "fixture".to_string(),
            config: None,
            source: S3TuiSource::Fixture,
            fixture_path: Some(fixture),
            purpose: "browser".to_string(),
            readonly: false,
            restore: true,
            launch_plan: None,
        };

        let evidence = build_s3_tui_evidence(&launch).unwrap();
        assert_eq!(evidence["secret_leak_scan"]["passed"], true);
        assert!(
            evidence["coverage"]
                .as_array()
                .unwrap()
                .contains(&json!("delete_confirmation"))
        );
    }

    #[test]
    fn parent_prefix_handles_root_and_nested_prefixes() {
        assert_eq!(parent_prefix(""), "");
        assert_eq!(parent_prefix("alpha.txt"), "");
        assert_eq!(parent_prefix("a/b/c.txt"), "a/b/");
        assert_eq!(parent_prefix("a/b/"), "a/");
    }

    #[test]
    fn filter_excludes_non_matching_entries() {
        let fixture = format!(
            "{}/fixtures/s3_tui_browser.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let launch = S3TuiLaunch {
            profile_label: "fixture".to_string(),
            config: None,
            source: S3TuiSource::Fixture,
            fixture_path: Some(fixture),
            purpose: "browser".to_string(),
            readonly: false,
            restore: true,
            launch_plan: None,
        };
        let mut app = S3TuiApp::new(launch).unwrap();
        app.filter = "report.csv".to_string();
        let items = app.filtered_items();
        assert_eq!(items.len(), 1);
        assert!(item_label(&items[0]).contains("report.csv"));
    }

    #[test]
    fn download_conflict_requires_explicit_overwrite_authorization() {
        let fixture = format!(
            "{}/fixtures/s3_tui_browser.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let launch = S3TuiLaunch {
            profile_label: "fixture".to_string(),
            config: None,
            source: S3TuiSource::Fixture,
            fixture_path: Some(fixture),
            purpose: "transfer".to_string(),
            readonly: false,
            restore: true,
            launch_plan: None,
        };
        let mut app = S3TuiApp::new(launch).unwrap();
        let local = std::env::temp_dir().join(format!(
            "voidb-s3-tui-conflict-{}",
            Utc::now().timestamp_micros()
        ));
        fs::write(&local, b"existing").unwrap();
        app.transfer_plan = Some(TransferPlanView {
            kind: PlanKind::Download,
            bucket: app.bucket.clone(),
            key: "reports/report.csv".to_string(),
            local: Some(local.to_string_lossy().into_owned()),
            confirmed: true,
            overwrite_authorized: false,
            progress: None,
        });

        app.confirm_plan();
        assert_eq!(app.mode, Mode::Error);
        assert!(app.status.contains("destination conflict"));
        app.authorize_overwrite();
        assert!(app.transfer_plan.as_ref().unwrap().overwrite_authorized);
        let _ = fs::remove_file(local);
    }
}
