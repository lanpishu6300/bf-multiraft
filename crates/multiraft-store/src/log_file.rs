//! File-backed `RaftLogStorage` under `{data_dir}/`.
//!
//! Log entries use append-only length-prefixed bincode (`log.bin`) so each Raft
//! append is O(new entries) with compact serialization. Truncate / purge rewrite
//! the file. Hard state stays in `hard_state.json`. Legacy `log.json` /
//! `log.ndjson` are loaded once and migrated on open.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::ops::RangeBounds;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use futures::lock::Mutex;
use openraft::LogState;
use openraft::RaftTypeConfig;
use openraft::alias::EntryOf;
use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

const HARD_STATE_FILE: &str = "hard_state.json";
const LOG_FILE_LEGACY: &str = "log.json";
const LOG_NDJSON: &str = "log.ndjson";
const LOG_BIN: &str = "log.bin";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(bound = "")]
struct HardState<C: RaftTypeConfig>
where
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    last_purged_log_id: Option<LogIdOf<C>>,
    committed: Option<LogIdOf<C>>,
    vote: Option<VoteOf<C>>,
}

#[derive(Debug)]
struct FileLogInner<C: RaftTypeConfig> {
    dir: PathBuf,
    last_purged_log_id: Option<LogIdOf<C>>,
    log: BTreeMap<u64, C::Entry>,
    committed: Option<LogIdOf<C>>,
    vote: Option<VoteOf<C>>,
}

/// Raft log store that mirrors the memory store and flushes to disk.
#[derive(Debug, Clone)]
pub struct FileLogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<FileLogInner<C>>>,
}

impl<C> FileLogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    /// Open (or create) a durable log directory.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let hard = load_hard_state::<C>(&dir)?;
        let log = load_log::<C>(&dir)?;

        Ok(Self {
            inner: Arc::new(Mutex::new(FileLogInner {
                dir,
                last_purged_log_id: hard.last_purged_log_id,
                log,
                committed: hard.committed,
                vote: hard.vote,
            })),
        })
    }
}

impl<C> FileLogInner<C>
where
    C: RaftTypeConfig,
    C::Entry: Clone + Serialize + DeserializeOwned,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    fn persist_hard_state(&self) -> io::Result<()> {
        let hs = HardState::<C> {
            last_purged_log_id: self.last_purged_log_id.clone(),
            committed: self.committed.clone(),
            vote: self.vote.clone(),
        };
        atomic_write_json(self.dir.join(HARD_STATE_FILE), &hs)
    }

    /// Append-only write of newly inserted entries (steady-state propose path).
    fn append_log_entries(&self, entries: &[C::Entry]) -> io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for ent in entries {
            let raw = bincode::serialize(ent)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let len = u32::try_from(raw.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "entry too large"))?;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(&raw);
        }
        let path = self.dir.join(LOG_BIN);
        let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
        f.write_all(&buf)?;
        Ok(())
    }

    /// Full rewrite used after truncate / purge (and legacy migration).
    fn rewrite_log(&self) -> io::Result<()> {
        let path = self.dir.join(LOG_BIN);
        let tmp = path.with_extension("bin.tmp");
        {
            let mut buf = Vec::new();
            for ent in self.log.values() {
                let raw = bincode::serialize(ent)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let len = u32::try_from(raw.len())
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "entry too large"))?;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(&raw);
            }
            fs::write(&tmp, &buf)?;
        }
        fs::rename(&tmp, &path)?;
        let _ = fs::remove_file(self.dir.join(LOG_FILE_LEGACY));
        let _ = fs::remove_file(self.dir.join(LOG_NDJSON));
        Ok(())
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, io::Error> {
        Ok(self
            .log
            .range(range)
            .map(|(_, val)| val.clone())
            .collect())
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
        let last = self.log.iter().next_back().map(|(_, ent)| ent.log_id());
        let last_purged = self.last_purged_log_id.clone();
        let last = match last {
            None => last_purged.clone(),
            Some(x) => Some(x),
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        self.committed = committed;
        self.persist_hard_state()
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
        Ok(self.committed.clone())
    }

    async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
        self.vote = Some(vote.clone());
        self.persist_hard_state()
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
        Ok(self.vote.clone())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        let mut newly = Vec::new();
        for entry in entries {
            self.log.insert(entry.index(), entry.clone());
            newly.push(entry);
        }
        let res = self.append_log_entries(&newly);
        callback.io_completed(res.as_ref().map(|_| ()).map_err(|e| io::Error::new(e.kind(), e.to_string())));
        res
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C>>) -> Result<(), io::Error> {
        let start_index = match last_log_id {
            Some(log_id) => log_id.index() + 1,
            None => 0,
        };
        let keys: Vec<u64> = self.log.range(start_index..).map(|(k, _)| *k).collect();
        for key in keys {
            self.log.remove(&key);
        }
        self.rewrite_log()
    }

    async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
        {
            let ld = &mut self.last_purged_log_id;
            assert!(ld.as_ref() <= Some(&log_id));
            *ld = Some(log_id.clone());
        }
        let keys: Vec<u64> = self.log.range(..=log_id.index()).map(|(k, _)| *k).collect();
        for key in keys {
            self.log.remove(&key);
        }
        self.persist_hard_state()?;
        self.rewrite_log()
    }
}

fn load_hard_state<C>(dir: &Path) -> io::Result<HardState<C>>
where
    C: RaftTypeConfig,
    LogIdOf<C>: Serialize + DeserializeOwned,
    VoteOf<C>: Serialize + DeserializeOwned,
{
    let path = dir.join(HARD_STATE_FILE);
    if !path.exists() {
        return Ok(HardState::default());
    }
    let bytes = fs::read(&path)?;
    serde_json::from_slice(&bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn load_log<C>(dir: &Path) -> io::Result<BTreeMap<u64, C::Entry>>
where
    C: RaftTypeConfig,
    C::Entry: Clone + DeserializeOwned + Serialize,
{
    let bin = dir.join(LOG_BIN);
    if bin.exists() {
        return load_bin::<C>(&bin);
    }

    // Migrate older formats once into log.bin.
    let mut map = BTreeMap::new();
    let ndjson = dir.join(LOG_NDJSON);
    if ndjson.exists() {
        map = load_ndjson::<C>(&ndjson)?;
    } else {
        let legacy = dir.join(LOG_FILE_LEGACY);
        if legacy.exists() {
            let bytes = fs::read(&legacy)?;
            let entries: Vec<EntryOf<C>> = serde_json::from_slice(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            for ent in entries {
                map.insert(ent.index(), ent);
            }
        }
    }
    if !map.is_empty() {
        let tmp_store = FileLogInner::<C> {
            dir: dir.to_path_buf(),
            last_purged_log_id: None,
            log: map.clone(),
            committed: None,
            vote: None,
        };
        tmp_store.rewrite_log()?;
    }
    Ok(map)
}

fn load_bin<C>(path: &Path) -> io::Result<BTreeMap<u64, C::Entry>>
where
    C: RaftTypeConfig,
    C::Entry: DeserializeOwned,
{
    let bytes = fs::read(path)?;
    let mut map = BTreeMap::new();
    let mut off = 0usize;
    while off + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        if off + len > bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: truncated frame at {}", path.display(), off),
            ));
        }
        let ent: EntryOf<C> = bincode::deserialize(&bytes[off..off + len])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        off += len;
        map.insert(ent.index(), ent);
    }
    if off != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{}: trailing {} bytes", path.display(), bytes.len() - off),
        ));
    }
    Ok(map)
}

fn load_ndjson<C>(path: &Path) -> io::Result<BTreeMap<u64, C::Entry>>
where
    C: RaftTypeConfig,
    C::Entry: DeserializeOwned,
{
    let f = fs::File::open(path)?;
    let reader = BufReader::new(f);
    let mut map = BTreeMap::new();
    for (lineno, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let ent: EntryOf<C> = serde_json::from_str(&line).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}:{}: {e}", path.display(), lineno + 1),
            )
        })?;
        map.insert(ent.index(), ent);
    }
    Ok(map)
}

fn atomic_write_json<T: Serialize>(path: PathBuf, value: &T) -> io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, &bytes)?;
    fs::rename(&tmp, &path)?;
    Ok(())
}

mod impl_log_store {
    use std::fmt::Debug;
    use std::io;
    use std::ops::RangeBounds;

    use openraft::LogState;
    use openraft::RaftLogReader;
    use openraft::RaftTypeConfig;
    use openraft::alias::LogIdOf;
    use openraft::alias::VoteOf;
    use openraft::storage::IOFlushed;
    use openraft::storage::RaftLogStorage;
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use crate::log_file::FileLogStore;

    impl<C> RaftLogReader<C> for FileLogStore<C>
    where
        C: RaftTypeConfig,
        C::Entry: Clone + Serialize + DeserializeOwned,
        LogIdOf<C>: Serialize + DeserializeOwned,
        VoteOf<C>: Serialize + DeserializeOwned,
    {
        async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
            &mut self,
            range: RB,
        ) -> Result<Vec<C::Entry>, io::Error> {
            let mut inner = self.inner.lock().await;
            inner.try_get_log_entries(range).await
        }

        async fn read_vote(&mut self) -> Result<Option<VoteOf<C>>, io::Error> {
            let mut inner = self.inner.lock().await;
            inner.read_vote().await
        }
    }

    impl<C> RaftLogStorage<C> for FileLogStore<C>
    where
        C: RaftTypeConfig,
        C::Entry: Clone + Serialize + DeserializeOwned,
        LogIdOf<C>: Serialize + DeserializeOwned,
        VoteOf<C>: Serialize + DeserializeOwned,
    {
        type LogReader = Self;

        async fn get_log_state(&mut self) -> Result<LogState<C>, io::Error> {
            let mut inner = self.inner.lock().await;
            inner.get_log_state().await
        }

        async fn save_committed(&mut self, committed: Option<LogIdOf<C>>) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            inner.save_committed(committed).await
        }

        async fn read_committed(&mut self) -> Result<Option<LogIdOf<C>>, io::Error> {
            let mut inner = self.inner.lock().await;
            inner.read_committed().await
        }

        async fn save_vote(&mut self, vote: &VoteOf<C>) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            inner.save_vote(vote).await
        }

        async fn append<I>(&mut self, entries: I, callback: IOFlushed<C>) -> Result<(), io::Error>
        where
            I: IntoIterator<Item = C::Entry>,
        {
            let mut inner = self.inner.lock().await;
            inner.append(entries, callback).await
        }

        async fn truncate_after(
            &mut self,
            last_log_id: Option<LogIdOf<C>>,
        ) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            inner.truncate_after(last_log_id).await
        }

        async fn purge(&mut self, log_id: LogIdOf<C>) -> Result<(), io::Error> {
            let mut inner = self.inner.lock().await;
            inner.purge(log_id).await
        }

        async fn get_log_reader(&mut self) -> Self::LogReader {
            self.clone()
        }
    }
}
