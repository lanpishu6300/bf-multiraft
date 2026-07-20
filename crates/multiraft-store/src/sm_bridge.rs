//! OpenRaft state-machine store that bridges to [`multiraft_fsm::StateMachine`].
//!
//! Adapted from openraft `examples/sm-mem` at tag `v0.10.0-alpha.30`.

use std::io;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::Stream;
use futures::TryStreamExt;
use futures::lock::Mutex;
use multiraft_core::Request;
use multiraft_core::Response;
use multiraft_core::TypeConfig;
use multiraft_core::is_standby_snapshot_trigger;
use multiraft_fsm::GroupId;
use multiraft_fsm::StateMachine;
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::alias::DefaultEntryOf;
use openraft::alias::LeaderIdOf;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::StoredMembershipOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use openraft::vote::RaftLeaderIdExt;

use crate::snapshot_catalog::CatalogEntry;
use crate::snapshot_catalog::SnapshotCatalog;

/// Callback when a standby snapshot trigger log is applied: `(group, index, term)`.
pub type TriggerCb = Arc<dyn Fn(GroupId, u64, u64) + Send + Sync>;

/// Options for [`StateMachineStore::with_options`].
#[derive(Clone, Default)]
pub struct SmOptions {
    /// When false (StandbyOffload), `build_snapshot` never dumps the live FSM.
    pub allow_hot_build: bool,
    pub catalog: Option<Arc<SnapshotCatalog>>,
    pub on_standby_trigger: Option<TriggerCb>,
}

#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMetaOf<TypeConfig>,
    pub data: Vec<u8>,
}

#[derive(Debug)]
struct StateMachineStoreInner<S: StateMachine> {
    group_id: GroupId,
    last_applied_log: Option<LogIdOf<TypeConfig>>,
    last_membership: StoredMembershipOf<TypeConfig>,
    fsm: S,
    snapshot_idx: AtomicU64,
    current_snapshot: Option<StoredSnapshot>,
}

impl<S: StateMachine> StateMachineStoreInner<S> {
    fn new(group_id: GroupId, fsm: S) -> Self {
        Self {
            group_id,
            last_applied_log: None,
            last_membership: StoredMembershipOf::<TypeConfig>::default(),
            fsm,
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: None,
        }
    }

    fn next_snapshot_idx(&self) -> u64 {
        self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// OpenRaft [`RaftStateMachine`] that applies log payloads into a [`StateMachine`].
pub struct StateMachineStore<S: StateMachine> {
    group_id: GroupId,
    inner: Arc<Mutex<StateMachineStoreInner<S>>>,
    allow_hot_build: bool,
    catalog: Option<Arc<SnapshotCatalog>>,
    on_standby_trigger: Option<TriggerCb>,
}

impl<S: StateMachine> std::fmt::Debug for StateMachineStore<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateMachineStore")
            .field("group_id", &self.group_id)
            .field("allow_hot_build", &self.allow_hot_build)
            .field("has_catalog", &self.catalog.is_some())
            .field("has_trigger", &self.on_standby_trigger.is_some())
            .finish()
    }
}

impl<S: StateMachine> Clone for StateMachineStore<S> {
    fn clone(&self) -> Self {
        Self {
            group_id: self.group_id,
            inner: self.inner.clone(),
            allow_hot_build: self.allow_hot_build,
            catalog: self.catalog.clone(),
            on_standby_trigger: self.on_standby_trigger.clone(),
        }
    }
}

impl<S: StateMachine> StateMachineStore<S> {
    pub fn new(group_id: GroupId, fsm: S) -> Self {
        Self::with_options(
            group_id,
            fsm,
            SmOptions {
                allow_hot_build: true,
                catalog: None,
                on_standby_trigger: None,
            },
        )
    }

    pub fn with_options(group_id: GroupId, fsm: S, opts: SmOptions) -> Self {
        Self {
            group_id,
            inner: Arc::new(Mutex::new(StateMachineStoreInner::new(group_id, fsm))),
            allow_hot_build: opts.allow_hot_build,
            catalog: opts.catalog,
            on_standby_trigger: opts.on_standby_trigger,
        }
    }

    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    pub fn catalog(&self) -> Option<&Arc<SnapshotCatalog>> {
        self.catalog.as_ref()
    }

    /// Inspect the underlying FSM (for tests / local reads).
    pub async fn with_fsm<R>(&self, f: impl FnOnce(&S) -> R) -> R {
        let inner = self.inner.lock().await;
        f(&inner.fsm)
    }

    /// Last applied `(index, term)` from the SM store (source of truth for FSM watermark).
    ///
    /// Prefer this over Raft metrics after out-of-band [`Self::install_durable_snapshot`].
    pub async fn last_applied(&self) -> Option<(u64, u64)> {
        let inner = self.inner.lock().await;
        inner.last_applied_log.as_ref().map(|id| {
            (
                id.index(),
                id.committed_leader_id().term,
            )
        })
    }

    /// Restore FSM + last_applied from durable snapshot bytes (recovery / pull).
    ///
    /// Preserves existing `last_membership` so an out-of-band install does not wipe
    /// openraft membership metadata exposed via `get_current_snapshot`.
    pub async fn install_durable_snapshot(
        &self,
        last_index: u64,
        last_term: u64,
        _membership_default: bool,
        data: Vec<u8>,
    ) -> Result<(), io::Error> {
        let leader_id = LeaderIdOf::<TypeConfig>::new_committed(last_term, 0);
        let log_id = LogIdOf::<TypeConfig>::new(leader_id, last_index);
        let snapshot_id = format!("{last_index}-{last_term}");

        let mut inner = self.inner.lock().await;
        let group_id = inner.group_id;
        // Keep prior membership; durable snapshot payloads are FSM-only.
        let last_membership = inner.last_membership.clone();
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: Some(log_id),
            last_membership,
            snapshot_id: snapshot_id.clone(),
        };
        inner
            .fsm
            .restore(group_id, &data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        inner.last_applied_log = meta.last_log_id.clone();
        // membership already preserved in meta / unchanged on inner
        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        drop(inner);

        if let Some(catalog) = &self.catalog {
            catalog.write(group_id, last_index, last_term, snapshot_id, &data)?;
        }
        Ok(())
    }

    /// Freeze FSM under a brief lock, then serialize/fsync on a blocking thread.
    pub async fn build_standby_snapshot_async(
        &self,
        catalog: &SnapshotCatalog,
        group: GroupId,
        index: u64,
        term: u64,
        serialize_delay: Option<Duration>,
    ) -> Result<CatalogEntry, io::Error> {
        let data = {
            let inner = self.inner.lock().await;
            inner
                .fsm
                .freeze_for_snapshot(group)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        };

        let catalog = catalog.clone();
        let snapshot_id = format!("{index}-{term}");
        let data_for_write = data.clone();
        let entry = tokio::task::spawn_blocking(move || {
            if let Some(d) = serialize_delay {
                std::thread::sleep(d);
            }
            catalog.write(group, index, term, snapshot_id, &data_for_write)
        })
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("spawn_blocking: {e}")))??;

        // Advertise locally via current_snapshot for openraft get_current_snapshot.
        let leader_id = LeaderIdOf::<TypeConfig>::new_committed(term, 0);
        let log_id = LogIdOf::<TypeConfig>::new(leader_id, index);
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: Some(log_id),
            last_membership: {
                let inner = self.inner.lock().await;
                inner.last_membership.clone()
            },
            snapshot_id: entry.snapshot_id.clone(),
        };
        {
            let mut inner = self.inner.lock().await;
            inner.current_snapshot = Some(StoredSnapshot {
                meta,
                data,
            });
        }

        Ok(entry)
    }

    fn snapshot_from_catalog(&self) -> Result<Option<SnapshotOf<TypeConfig, Cursor<Vec<u8>>>>, io::Error> {
        let Some(catalog) = &self.catalog else {
            return Ok(None);
        };
        let Some(entry) = catalog.latest(self.group_id)? else {
            return Ok(None);
        };
        let Some(data) = catalog.read(self.group_id, &entry.snapshot_id)? else {
            return Ok(None);
        };
        let leader_id = LeaderIdOf::<TypeConfig>::new_committed(entry.last_term, 0);
        let log_id = LogIdOf::<TypeConfig>::new(leader_id, entry.last_index);
        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: Some(log_id),
            last_membership: StoredMembershipOf::<TypeConfig>::default(),
            snapshot_id: entry.snapshot_id,
        };
        Ok(Some(SnapshotOf::<TypeConfig, Cursor<Vec<u8>>> {
            meta,
            snapshot: Cursor::new(data),
        }))
    }
}

/// Free function alias matching the design sketch.
pub async fn build_standby_snapshot_async<S: StateMachine>(
    sm: &StateMachineStore<S>,
    catalog: &SnapshotCatalog,
    group: GroupId,
    index: u64,
    term: u64,
    serialize_delay: Option<Duration>,
) -> Result<CatalogEntry, io::Error> {
    sm.build_standby_snapshot_async(catalog, group, index, term, serialize_delay)
        .await
}

impl<S> RaftSnapshotBuilder<TypeConfig> for StateMachineStore<S>
where
    S: StateMachine,
    TypeConfig: openraft::RaftTypeConfig<D = Request, R = Response, Entry = DefaultEntryOf<TypeConfig>>,
{
    type SnapshotData = Cursor<Vec<u8>>;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig, Cursor<Vec<u8>>>, io::Error> {
        if !self.allow_hot_build {
            // StandbyOffload: never sync-dump FSM; serve installed / catalog only.
            {
                let inner = self.inner.lock().await;
                if let Some(snapshot) = &inner.current_snapshot {
                    return Ok(SnapshotOf::<TypeConfig, Cursor<Vec<u8>>> {
                        meta: snapshot.meta.clone(),
                        snapshot: Cursor::new(snapshot.data.clone()),
                    });
                }
            }
            if let Some(snap) = self.snapshot_from_catalog()? {
                return Ok(snap);
            }
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no catalog/installed snapshot available (StandbyOffload; hot build disabled)",
            ));
        }

        let mut inner = self.inner.lock().await;

        let data = inner
            .fsm
            .freeze_for_snapshot(inner.group_id)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let snapshot_idx = inner.next_snapshot_idx();
        let snapshot_id = if let Some(last) = inner.last_applied_log.clone() {
            format!("{}-{}-{}", last.committed_leader_id(), last.index(), snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMetaOf::<TypeConfig> {
            last_log_id: inner.last_applied_log.clone(),
            last_membership: inner.last_membership.clone(),
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        inner.current_snapshot = Some(snapshot);
        drop(inner);

        Ok(SnapshotOf::<TypeConfig, Cursor<Vec<u8>>> {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

impl<S> RaftStateMachine<TypeConfig> for StateMachineStore<S>
where
    S: StateMachine,
    TypeConfig: openraft::RaftTypeConfig<D = Request, R = Response, Entry = DefaultEntryOf<TypeConfig>>,
{
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>), io::Error> {
        let inner = self.inner.lock().await;
        Ok((inner.last_applied_log.clone(), inner.last_membership.clone()))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        let mut pending_triggers: Vec<(GroupId, u64, u64)> = Vec::new();

        {
            let mut inner = self.inner.lock().await;

            while let Some((entry, responder)) = entries.try_next().await? {
                tracing::debug!(%entry.log_id, "replicate to sm");

                inner.last_applied_log = Some(entry.log_id.clone());

                let response = match &entry.payload {
                    EntryPayload::Blank => Response::none(),
                    EntryPayload::Normal(req) => {
                        if is_standby_snapshot_trigger(&req.data) {
                            let group_id = inner.group_id;
                            let index = entry.log_id.index();
                            let term = entry.log_id.committed_leader_id().term;
                            if self.on_standby_trigger.is_some() {
                                pending_triggers.push((group_id, index, term));
                            }
                            Response::none()
                        } else {
                            let group_id = inner.group_id;
                            let index = entry.log_id.index();
                            let out = inner
                                .fsm
                                .apply(group_id, index, &req.data)
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                            Response::new(out.effects)
                        }
                    }
                    EntryPayload::Membership(mem) => {
                        inner.last_membership = StoredMembershipOf::<TypeConfig>::new(
                            Some(entry.log_id.clone()),
                            mem.clone(),
                        );
                        Response::none()
                    }
                };

                if let Some(responder) = responder {
                    responder.send(response);
                }
            }
        }

        if let Some(cb) = &self.on_standby_trigger {
            for (group, index, term) in pending_triggers {
                cb(group, index, term);
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(&mut self) -> Result<Self::SnapshotData, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<TypeConfig>,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let data = snapshot.into_inner();
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        let mut inner = self.inner.lock().await;
        let group_id = inner.group_id;
        inner
            .fsm
            .restore(group_id, &data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        inner.last_applied_log = meta.last_log_id.clone();
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot = Some(new_snapshot);
        drop(inner);

        if let Some(catalog) = &self.catalog {
            let (last_index, last_term) = match &meta.last_log_id {
                Some(id) => (id.index(), id.committed_leader_id().term),
                None => (0, 0),
            };
            catalog.write(
                group_id,
                last_index,
                last_term,
                meta.snapshot_id.clone(),
                &data,
            )?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<TypeConfig, Self::SnapshotData>>, io::Error> {
        let inner = self.inner.lock().await;
        match &inner.current_snapshot {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(SnapshotOf::<TypeConfig, Self::SnapshotData> {
                    meta: snapshot.meta.clone(),
                    snapshot: Cursor::new(data),
                }))
            }
            None => {
                drop(inner);
                self.snapshot_from_catalog()
            }
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
