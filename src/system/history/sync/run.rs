//! One synchronization: fetch the setup branch and other machines'
//! recovery refs, run the transition table, publish this machine's
//! changes (leased; on a rejection fetch again and retry), upload eligible
//! checkpoints, record the durable state, and derive `sync.json` with what
//! is pending: incoming changes to apply, conflicts to decide, uploads
//! still to do. Captures never wait on the network; this never changes a
//! live file (application is its own operation).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use eyre::{Result, bail};
use serde::{Deserialize, Serialize};

use super::SyncMode;
use super::format::{self, RepoState};
use super::layout::{Located, Roots, is_configuration};
use super::network::{PushOutcome, Remote, UPSTREAM_REF};
use super::reconcile::{self, Conflict, Object, PathPlan};
use super::{backup, publish, share, state};
use crate::file::display_path;
use crate::system::history::checkpoint::Store;
use crate::system::history::config::OriginTomlConfig;
use crate::system::history::store::{self as hstore, Entry};
use crate::system::history::tracked::TrackedSet;

const PUSH_RETRIES: usize = 5;

/// An incoming change waiting for `mise bootstrap dotfiles pull` (or automatic
/// application in `sync` mode).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PendingApplication {
    pub branch_path: String,
    /// `None` deletes the local file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<Object>,
    /// Configuration, whose change may alter declarations.
    pub configuration: bool,
    /// The state to record once the write succeeds.
    pub next: state::SyncRecord,
}

/// Derived from the repository after every sync; rebuilt when it
/// disagrees.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct SyncStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_publish: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fetch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_apply: Option<String>,
    #[serde(default)]
    pub uploaded: BTreeSet<String>,
    #[serde(default)]
    pub conflicts: Vec<Conflict>,
    #[serde(default)]
    pub pending_applications: Vec<PendingApplication>,
    /// Incoming configuration changed declarations: run `mise bootstrap`.
    #[serde(default)]
    pub declarations_changed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_until: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_commit: Option<String>,
    /// The user confirmed adopting an unmarked repository.
    #[serde(default)]
    pub adopted: bool,
    /// Checkpoints recorded before the origin was connected are not
    /// uploaded unless `--include-existing` was given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_since: Option<String>,
    /// The repository this state belongs to; another one starts afresh.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_branch: Option<String>,
    /// Conflicts a desktop notification was shown for, so a retry of the
    /// same conflict never notifies again.
    #[serde(default)]
    pub notified_conflicts: Vec<String>,
    /// `origin --remove` was run: the recorded repository no longer stands
    /// in for a declaration.
    #[serde(default)]
    pub disconnected: bool,
}

pub(crate) fn status_path(state_dir: &Path) -> PathBuf {
    hstore::store_dir_in(state_dir).join("sync.json")
}

pub(crate) fn read_status(state_dir: &Path) -> SyncStatus {
    std::fs::read_to_string(status_path(state_dir))
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

pub(crate) fn write_status(state_dir: &Path, status: &SyncStatus) -> Result<()> {
    hstore::write_json(&status_path(state_dir), status)
}

#[derive(Debug, Default)]
pub(crate) struct SyncOutcome {
    pub published: Option<String>,
    pub uploaded: usize,
    pub pruned_remote: usize,
    pub pending: usize,
    pub conflicts: usize,
    pub fetched_upstream: Option<String>,
}

pub(crate) struct SyncRequest {
    pub fetch_only: bool,
    /// Save the tracked set first, so what is published is what is on
    /// disk. The watcher passes `false`: it saves on its own schedule, and a
    /// throttled file's held version or a manual-save entry's unsaved edits
    /// must not reach the repository through a sync.
    pub capture: bool,
    /// The repository to use instead of `[history.origin]` (onboarding,
    /// before the configuration that declares it is in place).
    pub origin: Option<OriginTomlConfig>,
    /// No network: reconcile against the branch as last fetched and record
    /// what is pending (after an incoming configuration declared more).
    pub offline: bool,
}

impl SyncRequest {
    pub(crate) fn new(fetch_only: bool) -> Self {
        Self {
            fetch_only,
            capture: true,
            origin: None,
            offline: false,
        }
    }
}

/// The connected origin, or why there is none.
pub(crate) fn origin() -> Result<OriginTomlConfig> {
    if let Some((_, origin)) = crate::system::history::config::origin()? {
        return Ok(origin);
    }
    // recorded when it was connected: a fresh machine's declaration may
    // still be on its way in the configuration being pulled
    let status = read_status(&crate::dirs::STATE);
    if let (Some(url), Some(branch), false) =
        (status.origin_url, status.origin_branch, status.disconnected)
    {
        return Ok(OriginTomlConfig {
            url,
            branch,
            encrypt_backups: false,
        });
    }
    bail!(
        "no setup repository is connected; `mise bootstrap dotfiles origin set <url>` connects one"
    )
}

/// Runs one synchronization.
pub(crate) fn sync(
    store: &Store,
    tracked: &TrackedSet,
    request: &SyncRequest,
) -> Result<SyncOutcome> {
    let origin = match &request.origin {
        Some(origin) => origin.clone(),
        None => origin()?,
    };
    if origin.encrypt_backups {
        bail!("[history.origin] encrypt_backups is not supported yet; set it to false");
    }
    let repo = store
        .repo()
        .ok_or_else(|| eyre::eyre!("synchronizing requires git"))?;
    let mode = SyncMode::current()?;
    let state_dir = store.state_dir();
    let mut status = read_status(state_dir);
    let machine = store.machine().clone();
    let remote = Remote::new(repo, &origin.url);
    let mut outcome = SyncOutcome::default();

    let result = (|| -> Result<()> {
        if !request.offline {
            // a branch that vanished from a repository this machine had
            // synced with is not an empty upstream: reading it as one would
            // queue the deletion of every file it held
            let found = remote.fetch_pruning(&origin.branch)?;
            if !found && status.upstream_commit.is_some() {
                bail!(
                    "the setup branch `{}` is not at {} any more (renamed, or deleted?); nothing was changed. `mise bootstrap dotfiles origin set {} --branch <name>` follows a renamed branch",
                    origin.branch,
                    origin.url,
                    origin.url
                );
            }
            if !found && repo.ref_oid(UPSTREAM_REF)?.is_some() {
                repo.delete_ref(UPSTREAM_REF)?;
            }
            status.last_fetch = Some(hstore::now_rfc3339());
        }
        let mut upstream_commit = repo.ref_oid(UPSTREAM_REF)?;
        outcome.fetched_upstream = upstream_commit.clone();
        let repo_state = format::detect(repo, upstream_commit.as_deref())?;
        repo_state.check()?;
        if repo_state == RepoState::Unmarked && !status.adopted {
            bail!(
                "{} is an existing repository without the mise marker; `mise bootstrap dotfiles origin set {}` previews how it would be adopted",
                origin.url,
                origin.url
            );
        }
        // ours is the saved version: save what is live first, so a fresh
        // machine's existing files take part in adoption
        if request.capture {
            capture_now(store, tracked);
        }
        let entries = store.list()?;
        let shared = share::current(repo, store, tracked)?;
        let unsaved = unsaved_paths(repo, tracked, &shared)?;
        let publish = mode.publishes() && !request.fetch_only && !request.offline;
        let mut plans;
        let mut attempts = 0;
        loop {
            attempts += 1;
            let mut upstream = reconcile::upstream(repo, upstream_commit.as_deref())?;
            upstream
                .files
                .retain(|branch_path, _| eligible(&Roots::current(), tracked, branch_path));
            let sync_state = state::load(repo)?;
            plans =
                reconcile::reconcile(repo, &shared.objects(), &upstream, &sync_state, &unsaved)?;
            let changes: BTreeMap<String, Option<Object>> = plans
                .iter()
                .filter_map(|plan| {
                    plan.publish
                        .clone()
                        .map(|object| (plan.branch_path.clone(), object))
                })
                .collect();
            if !publish {
                break;
            }
            let add_marker = matches!(repo_state, RepoState::Empty | RepoState::Unmarked);
            let publication = publish::Publication {
                upstream_commit: upstream_commit.as_deref(),
                changes: changes.clone(),
                add_marker,
                message: publish::message(&machine.name, &changes),
            };
            let Some(commit) = publish::build(repo, &publication)? else {
                break;
            };
            match publish::push(&remote, &origin.branch, &commit, upstream_commit.as_deref())? {
                PushOutcome::Done => {
                    outcome.published = Some(commit.clone());
                    status.last_publish = Some(hstore::now_rfc3339());
                    repo.update_ref(UPSTREAM_REF, &commit, upstream_commit.as_deref())?;
                    upstream_commit = Some(commit);
                    // the published plans are acknowledged now
                    let mut next_state = sync_state.clone();
                    for plan in &plans {
                        if plan.publish.is_some() {
                            next_state.insert(plan.branch_path.clone(), plan.next.clone());
                        }
                    }
                    for plan in &plans {
                        if plan.publish.is_none() && plan.apply.is_none() && plan.conflict.is_none()
                        {
                            next_state.insert(plan.branch_path.clone(), plan.next.clone());
                        }
                    }
                    state::save(repo, &next_state, "published")?;
                    // the applications and conflicts are relative to the new head
                    let mut upstream = reconcile::upstream(repo, upstream_commit.as_deref())?;
                    upstream
                        .files
                        .retain(|branch_path, _| eligible(&Roots::current(), tracked, branch_path));
                    plans = reconcile::reconcile(
                        repo,
                        &shared.objects(),
                        &upstream,
                        &next_state,
                        &unsaved,
                    )?;
                    break;
                }
                PushOutcome::Rejected(reason) if attempts < PUSH_RETRIES => {
                    debug!("history sync: publication rejected, fetching again: {reason}");
                    remote.fetch(&origin.branch)?;
                    upstream_commit = repo.ref_oid(UPSTREAM_REF)?;
                }
                PushOutcome::Rejected(reason) => {
                    bail!("publication kept being rejected after {attempts} attempts: {reason}")
                }
            }
        }
        status.upstream_commit = upstream_commit.clone();
        record_pending(&mut status, &plans, &Roots::current());
        if publish {
            let since = status.upload_since.clone();
            let uploadable: Vec<Entry> = entries
                .iter()
                .filter(|entry| {
                    since
                        .as_deref()
                        .is_none_or(|s| entry.checkpoint.created_at.as_str() >= s)
                })
                .cloned()
                .collect();
            outcome.uploaded = backup::upload(
                &remote,
                repo,
                &uploadable,
                &machine.id,
                &mut status.uploaded,
            )?;
            outcome.pruned_remote =
                backup::prune_remote(&remote, &entries, &machine.id, &mut status.uploaded)?;
        }
        outcome.pending = status.pending_applications.len();
        outcome.conflicts = status.conflicts.len();
        status.last_error = None;
        status.backoff_until = None;
        notify_new_conflicts(&mut status);
        Ok(())
    })();
    if let Err(err) = &result {
        status.last_error = Some(format!("{err:#}"));
    }
    write_status(state_dir, &status)?;
    result.map(|()| outcome)
}

/// Whether an upstream path belongs on this machine: configuration and
/// sources always; a tracked entry's stream only when it is the one this
/// machine selects (its variant, or the base stream when it has none), so
/// another platform's version is never applied here and never read as a
/// change. A stream with no tracked entry yet (a fresh machine before its
/// configuration arrived) waits for the next sync.
fn eligible(roots: &Roots, tracked: &TrackedSet, branch_path: &str) -> bool {
    match roots.locate(branch_path) {
        Located::Tracked { path, variant } => tracked
            .entry_for(&path)
            .is_some_and(|entry| entry.variant == variant),
        Located::Config(_) | Located::Source(_) | Located::Marker => true,
        Located::Unmapped => false,
    }
}

/// A desktop notification for conflicts that newly need a decision, when
/// `history.notify` is on. Each conflict notifies once: a retry of the same
/// sync is silent, and a resolved conflict is forgotten so it notifies
/// again should it come back. Never blocks; a failure is only logged.
fn notify_new_conflicts(status: &mut SyncStatus) {
    let current: BTreeSet<String> = status
        .conflicts
        .iter()
        .map(|conflict| conflict.branch_path.clone())
        .collect();
    let notified: BTreeSet<String> = status.notified_conflicts.iter().cloned().collect();
    let new: Vec<&Conflict> = status
        .conflicts
        .iter()
        .filter(|conflict| !notified.contains(&conflict.branch_path))
        .collect();
    if !new.is_empty() && crate::config::Settings::get().history.notify {
        let roots = Roots::current();
        let lines: Vec<String> = new
            .iter()
            .take(3)
            .map(|conflict| {
                let path = roots
                    .locate(&conflict.branch_path)
                    .path()
                    .map(display_path)
                    .unwrap_or_else(|| conflict.branch_path.clone());
                format!("{path}: {}", conflict.kind.describe())
            })
            .collect();
        let more = new.len().saturating_sub(3);
        let body = if more > 0 {
            format!("{}\n+{more} more", lines.join("\n"))
        } else {
            lines.join("\n")
        };
        crate::system::history::notify::send(
            &format!(
                "{} sync conflict{} need{} a decision",
                new.len(),
                if new.len() == 1 { "" } else { "s" },
                if new.len() == 1 { "s" } else { "" }
            ),
            &format!("{body}\nInspect with: mise bootstrap dotfiles status"),
        );
    }
    status.notified_conflicts = current.into_iter().collect();
}

/// Saves the tracked set now (deduplicated against the newest checkpoint),
/// so the versions this sync publishes are what is on disk.
pub(crate) fn capture_now(store: &Store, tracked: &TrackedSet) {
    use crate::system::history::checkpoint::Draft;
    use crate::system::history::store::Trigger;
    match store.attempt(tracked, Draft::new(Trigger::Edit)) {
        Ok(_) => {}
        Err(err) => warn!("history sync: could not save the current state first: {err:#}"),
    }
}

/// Records what is left to apply or decide, keeping the newest upstream
/// version for a path whose application was already pending.
fn record_pending(status: &mut SyncStatus, plans: &[PathPlan], roots: &Roots) {
    status.conflicts = plans
        .iter()
        .filter_map(|plan| plan.conflict.clone())
        .collect();
    status.pending_applications = plans
        .iter()
        .filter(|plan| plan.conflict.is_none())
        .filter_map(|plan| {
            let object = plan.apply.clone()?;
            roots.locate(&plan.branch_path).path()?;
            Some(PendingApplication {
                branch_path: plan.branch_path.clone(),
                object,
                configuration: is_configuration(&plan.branch_path),
                next: plan.next.clone(),
            })
        })
        .collect();
    // said until `mise bootstrap` ran, even once the configuration is written
    status.declarations_changed = status.declarations_changed
        || status
            .pending_applications
            .iter()
            .any(|pending| pending.configuration);
}

/// Whether an upstream path belongs on this machine: configuration and
/// sources always; a tracked entry's stream only when it is the one this
/// machine selects (its variant, or the base stream when it has none), so
/// another platform's version is never applied here and never read as a
/// change. A path no entry covers yet (a fresh machine before its
/// configuration arrived) takes the base stream, what a declaration
/// without variants selects; a variant stream waits for the declaration.
pub(super) fn eligible(roots: &Roots, tracked: &TrackedSet, branch_path: &str) -> bool {
    match roots.locate(branch_path) {
        Located::Tracked { path, variant } => match tracked.entry_for(&path) {
            Some(entry) => entry.variant == variant,
            None => variant.is_none(),
        },
        Located::Config(_) | Located::Source(_) | Located::Marker => true,
        Located::Unmapped => false,
    }
}

/// A bootstrap finished: the declarations that arrived through sync are
/// applied now, so `status` stops asking for one.
pub(crate) fn bootstrap_completed() {
    let state_dir: &Path = &crate::dirs::STATE;
    let mut status = read_status(state_dir);
    if status.declarations_changed {
        status.declarations_changed = false;
        if let Err(err) = write_status(state_dir, &status) {
            debug!("history: could not record that the bootstrap ran: {err}");
        }
    }
}

/// Manual-save entries whose live file differs from the saved version:
/// an incoming change there is held instead of applied.
fn unsaved_paths(
    repo: &crate::system::history::shadow::HistoryRepo,
    tracked: &TrackedSet,
    shared: &share::ShareReport,
) -> Result<BTreeSet<String>> {
    let Some(checkpoint) = shared.checkpoint.as_deref() else {
        return Ok(BTreeSet::new());
    };
    let walk = tracked.walk()?;
    let mut unsaved = BTreeSet::new();
    for (branch_path, file) in &shared.files {
        let Some((_, policy)) = walk.files.get(&file.local) else {
            continue;
        };
        if policy.autosave {
            continue;
        }
        let live = match std::fs::symlink_metadata(&file.local) {
            Ok(meta) if meta.file_type().is_symlink() => repo.hash_blob(
                std::fs::read_link(&file.local)?
                    .to_string_lossy()
                    .as_bytes(),
            )?,
            Ok(meta) if meta.is_file() => repo.hash_blob(&std::fs::read(&file.local)?)?,
            _ => continue,
        };
        if live != file.oid {
            debug!(
                "history sync: {} has unsaved edits (checkpoint {})",
                display_path(&file.local),
                checkpoint
            );
            unsaved.insert(branch_path.clone());
        }
    }
    Ok(unsaved)
}
