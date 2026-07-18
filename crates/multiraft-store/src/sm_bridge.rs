//! OpenRaft state-machine store that bridges to [`multiraft_fsm::StateMachine`].
//!
//! Adapted from openraft `examples/sm-mem` at tag `v0.10.0-alpha.30`.

use std::io;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use futures::Stream;
use futures::TryStreamExt;
use futures::lock::Mutex;
use multiraft_core::Request;
use multiraft_core::Response;
use multiraft_core::TypeConfig;
use multiraft_fsm::GroupId;
use multiraft_fsm::StateMachine;
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::RaftSnapshotBuilder;
use openraft::alias::DefaultEntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::StoredMembershipOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;

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
#[derive(Debug)]
pub struct StateMachineStore<S: StateMachine> {
    group_id: GroupId,
    inner: Arc<Mutex<StateMachineStoreInner<S>>>,
}

impl<S: StateMachine> Clone for StateMachineStore<S> {
    fn clone(&self) -> Self {
        Self {
            group_id: self.group_id,
            inner: self.inner.clone(),
        }
    }
}

impl<S: StateMachine> StateMachineStore<S> {
    pub fn new(group_id: GroupId, fsm: S) -> Self {
        Self {
            group_id,
            inner: Arc::new(Mutex::new(StateMachineStoreInner::new(group_id, fsm))),
        }
    }

    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Inspect the underlying FSM (for tests / local reads).
    pub async fn with_fsm<R>(&self, f: impl FnOnce(&S) -> R) -> R {
        let inner = self.inner.lock().await;
        f(&inner.fsm)
    }
}

impl<S> RaftSnapshotBuilder<TypeConfig> for StateMachineStore<S>
where
    S: StateMachine,
    TypeConfig: openraft::RaftTypeConfig<D = Request, R = Response, Entry = DefaultEntryOf<TypeConfig>>,
{
    type SnapshotData = Cursor<Vec<u8>>;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<TypeConfig, Cursor<Vec<u8>>>, io::Error> {
        let mut inner = self.inner.lock().await;

        let data = inner
            .fsm
            .snapshot(inner.group_id)
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
        let mut inner = self.inner.lock().await;

        while let Some((entry, responder)) = entries.try_next().await? {
            tracing::debug!(%entry.log_id, "replicate to sm");

            inner.last_applied_log = Some(entry.log_id.clone());

            let response = match &entry.payload {
                EntryPayload::Blank => Response::none(),
                EntryPayload::Normal(req) => {
                    let group_id = inner.group_id;
                    let index = entry.log_id.index();
                    let out = inner
                        .fsm
                        .apply(group_id, index, &req.data)
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                    Response::new(out.effects)
                }
                EntryPayload::Membership(mem) => {
                    inner.last_membership =
                        StoredMembershipOf::<TypeConfig>::new(Some(entry.log_id.clone()), mem.clone());
                    Response::none()
                }
            };

            if let Some(responder) = responder {
                responder.send(response);
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
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
