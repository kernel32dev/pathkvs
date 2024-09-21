use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    io::{Error, ErrorKind, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{
        atomic::{AtomicPtr, Ordering},
        Mutex,
    },
    time::{Duration, SystemTime},
};

use error::TransactionError;

pub mod error;

pub struct Database {
    resolved_master: AtomicPtr<Commit>,
    persistence: Option<Persistence>,
}

pub struct Persistence {
    serialized_master: AtomicPtr<Commit>,
    history_sink: Mutex<HistorySink>,
    sync: DatabaseWriteSyncMode,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseWriteSyncMode {
    #[default]
    Sync,
    Flush,
    Cached,
}

struct _AssertDatabaseSend
where
    Database: Send;

struct _AssertDatabaseSync
where
    Database: Sync;

struct HistorySink {
    output_stream: File,
    cursor: u64,
}

#[derive(Clone)]
struct Commit {
    prev: *const Commit,
    time: Duration,
    changes: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Clone)]
pub struct Transaction<'a> {
    database: &'a Database,
    commit: Commit,
    reads: HashSet<Vec<u8>>,
    scans: HashSet<(Vec<u8>, usize)>,
}

#[derive(Clone)]
pub struct Snapshot<'a> {
    commit: Option<&'a Commit>,
}

impl Database {
    pub fn memory() -> Self {
        Self {
            resolved_master: AtomicPtr::new(std::ptr::null_mut()),
            persistence: None,
        }
    }
    pub fn create(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)?;
        Ok(Self {
            resolved_master: AtomicPtr::new(std::ptr::null_mut()),
            persistence: Some(Persistence {
                serialized_master: AtomicPtr::new(std::ptr::null_mut()),
                history_sink: Mutex::new(HistorySink {
                    output_stream: file,
                    cursor: 0,
                }),
                sync: DatabaseWriteSyncMode::default(),
            }),
        })
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let mut file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut cursor = 0u64;
        let mut commit_ptr = std::ptr::null_mut();

        let result: Result<(), Error> = (|| loop {
            let mut commit_cursor = 0u64;

            let mut seconds = [0; 8];
            let mut nanoseconds = [0; 4];
            file.read_exact(&mut seconds)?;
            file.read_exact(&mut nanoseconds)?;
            commit_cursor += 12;
            let seconds = u64::from_le_bytes(seconds);
            let nanoseconds = u32::from_le_bytes(nanoseconds);

            if nanoseconds >= 1_000_000_000 {
                return Err(Error::new(ErrorKind::UnexpectedEof, "bad nanosecond field"));
            }

            let mut kv_len = [0; 4];
            file.read_exact(&mut kv_len)?;
            commit_cursor += 4;
            let kv_len = u32::from_le_bytes(kv_len);

            let time = Duration::new(seconds, nanoseconds);

            let mut changes = HashMap::new();

            for _ in 0..kv_len {
                let mut k_len = [0; 4];
                file.read_exact(&mut k_len)?;
                commit_cursor += 4;
                let k_len = u32::from_le_bytes(k_len);

                let mut k = Vec::<u8>::new();
                k.reserve_exact(k_len as usize);
                unsafe {
                    k.set_len(k_len as usize);
                }
                file.read_exact(&mut k)?;
                commit_cursor += k_len as u64;

                let mut v_len = [0; 4];
                file.read_exact(&mut v_len)?;
                commit_cursor += 4;
                let v_len = u32::from_le_bytes(v_len);

                let mut v = Vec::<u8>::new();
                v.reserve_exact(v_len as usize);
                unsafe {
                    v.set_len(v_len as usize);
                }
                file.read_exact(&mut v)?;
                commit_cursor += v_len as u64;

                changes.insert(k, v);
            }

            commit_ptr = Box::into_raw(Box::new(Commit {
                prev: commit_ptr,
                time,
                changes,
            }));

            cursor += commit_cursor;
        })();

        match result {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {}
            Err(error) => return Err(error),
        }

        file.set_len(cursor)?;

        Ok(Self {
            resolved_master: AtomicPtr::new(commit_ptr),
            persistence: Some(Persistence {
                serialized_master: AtomicPtr::new(commit_ptr),
                history_sink: Mutex::new(HistorySink {
                    output_stream: file,
                    cursor,
                }),
                sync: DatabaseWriteSyncMode::default(),
            }),
        })
    }
    pub fn write_sync_mode(mut self, sync_mode: DatabaseWriteSyncMode) -> Self {
        if let Some(persitence) = &mut self.persistence {
            persitence.sync = sync_mode;
        }
        self
    }
    fn load_master(&self) -> *const Commit {
        if let Some(persistence) = &self.persistence {
            persistence.serialized_master.load(Ordering::SeqCst)
        } else {
            self.resolved_master.load(Ordering::SeqCst)
        }
    }
    pub fn start_writes<'a>(&'a self) -> Transaction<'a> {
        Transaction {
            database: self,
            commit: Commit {
                prev: self.load_master(),
                time: Duration::default(),
                changes: HashMap::new(),
            },
            reads: HashSet::new(),
            scans: HashSet::new(),
        }
    }
    pub fn write(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if key.is_empty() {
            return Ok(());
        }
        let mut ts = self.start_writes();
        ts.write(key, value);
        match ts.commit() {
            Ok(_) => Ok(()),
            Err(TransactionError::Conflict) => {
                unreachable!("a write only transaction cannot conflict")
            }
            Err(TransactionError::Io(error)) => Err(error),
        }
    }
    pub fn snapshot<'a>(&'a self) -> Snapshot<'a> {
        unsafe {
            Snapshot {
                commit: self.load_master().as_ref(),
            }
        }
    }
    pub fn past_unix_time_snapshot_with<'a>(&'a self, time: Duration) -> Snapshot<'a> {
        let mut commit = self.load_master();
        unsafe {
            while let Some(reference) = commit.as_ref() {
                if reference.time <= time {
                    return Snapshot {
                        commit: Some(reference),
                    };
                }
                commit = reference.prev;
            }
        }
        Snapshot { commit: None }
    }
    pub fn past_sys_time_snapshot<'a>(&'a self, time: SystemTime) -> Snapshot<'a> {
        let Ok(time) = time.duration_since(SystemTime::UNIX_EPOCH) else {
            return Snapshot { commit: None };
        };
        self.past_unix_time_snapshot_with(time)
    }
    pub fn len<'b>(&'b self, key: &[u8]) -> u32 {
        if key.is_empty() {
            return 0;
        }
        self.snapshot().len(key)
    }
    pub fn read<'b>(&'b self, key: &[u8]) -> &'b [u8] {
        if key.is_empty() {
            return &[];
        }
        self.snapshot().read(key)
    }
    pub fn count<'b>(&'b self, start: &[u8], end: &[u8]) -> u32 {
        self.snapshot().count(start, end)
    }
    pub fn list<'b>(&'b self, start: &[u8], end: &[u8]) -> Vec<&'b [u8]> {
        self.snapshot().list(start, end)
    }
    pub fn scan<'b>(&'b self, start: &[u8], end: &[u8]) -> Vec<(&'b [u8], &'b [u8])> {
        self.snapshot().scan(start, end)
    }

    fn persist(&self) -> Result<(), Error> {
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };
        loop {
            let mut resolved_master = self.resolved_master.load(Ordering::SeqCst) as *const Commit;
            let mut workbench = persistence.history_sink.lock().unwrap();
            let serialized_master = persistence.serialized_master.load(Ordering::SeqCst) as *const Commit;
            let mut stack = Vec::new();
            while resolved_master != serialized_master {
                stack.push(resolved_master);
                unsafe {
                    resolved_master = resolved_master
                        .as_ref()
                        .expect("snapshot cannot be unwound without first hiting the last_commit")
                        .prev;
                }
            }
            if stack.is_empty() {
                return Ok(());
            }
            for commit in stack.into_iter().rev() {
                let commit_ref = unsafe { commit.as_ref().unwrap_unchecked() };
                let mut new_cursor = workbench.cursor;
                workbench.output_stream.seek(SeekFrom::Start(new_cursor))?;
                workbench.output_stream.set_len(new_cursor)?;

                let seconds = commit_ref.time.as_secs().to_le_bytes();
                let nanoseconds = commit_ref.time.subsec_nanos().to_le_bytes();
                workbench.output_stream.write_all(&seconds)?;
                workbench.output_stream.write_all(&nanoseconds)?;
                new_cursor += 12;

                let kv_len_bytes = (commit_ref.changes.len() as u32).to_le_bytes();
                workbench.output_stream.write_all(&kv_len_bytes)?;
                new_cursor += 4;

                for (k, v) in &commit_ref.changes {
                    let k_len_bytes = (k.len() as u32).to_le_bytes();
                    workbench.output_stream.write_all(&k_len_bytes)?;
                    workbench.output_stream.write_all(&k)?;
                    let v_len_bytes = (v.len() as u32).to_le_bytes();
                    workbench.output_stream.write_all(&v_len_bytes)?;
                    workbench.output_stream.write_all(&v)?;
                    new_cursor += 8 + k.len() as u64 + v.len() as u64;
                }
                match persistence.sync {
                    DatabaseWriteSyncMode::Sync => {
                        workbench.output_stream.flush()?;
                        workbench.output_stream.sync_all()?;
                    }
                    DatabaseWriteSyncMode::Flush => {
                        workbench.output_stream.flush()?;
                    }
                    DatabaseWriteSyncMode::Cached => {}
                }
                persistence.serialized_master
                    .store(commit as *mut _, Ordering::SeqCst);
                workbench.cursor = new_cursor;
            }
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let mut commit_ptr = *self.resolved_master.get_mut();
        unsafe {
            while let Some(commit) = commit_ptr.as_mut() {
                std::ptr::drop_in_place(&mut commit.changes);
                commit_ptr = commit.prev as *mut Commit;
            }
        }
    }
}

impl Commit {
    unsafe fn ptr_len(commit: *const Commit, key: &[u8]) -> u32 {
        Commit::ptr_read(commit, key).len() as u32
    }
    unsafe fn ptr_read<'a>(mut commit: *const Commit, key: &[u8]) -> &'a [u8] {
        if key.len() > u32::MAX as usize {
            return &[];
        }
        while let Some(reference) = commit.as_ref() {
            if let Some(value) = reference.changes.get(key) {
                return value;
            }
            commit = reference.prev;
        }
        &[]
    }
    unsafe fn ptr_count(commit: *const Commit, start: &[u8], end: &[u8]) -> u32 {
        let mut count = 0;
        let mut keys = HashMap::new();
        Commit::ptr_historic_scan(commit, start, end, |key, value| {
            keys.entry(key).or_insert_with(|| {
                if !value.is_empty() {
                    count += 1;
                }
            });
        });
        count
    }
    unsafe fn ptr_list<'a>(commit: *const Commit, start: &[u8], end: &[u8]) -> Vec<&'a [u8]> {
        let mut count = 0;
        let mut keys = BTreeMap::new();
        Commit::ptr_historic_scan(commit, start, end, |key, value| {
            keys.entry(key).or_insert_with(|| {
                if !value.is_empty() {
                    count += 1;
                }
                !value.is_empty()
            });
        });
        let mut vec = Vec::new();
        vec.reserve_exact(count);
        vec.extend(keys.into_iter().filter_map(|(k, v)| v.then_some(k)));
        vec
    }
    unsafe fn ptr_scan<'a>(
        commit: *const Commit,
        start: &[u8],
        end: &[u8],
    ) -> Vec<(&'a [u8], &'a [u8])> {
        let mut count = 0;
        let mut keys = BTreeMap::new();
        Commit::ptr_historic_scan(commit, start, end, |key, value| {
            keys.entry(key).or_insert_with(|| {
                if !value.is_empty() {
                    count += 1;
                }
                value
            });
        });
        let mut vec = Vec::new();
        vec.reserve_exact(count);
        vec.extend(keys.into_iter().filter(|(_, v)| !v.is_empty()));
        vec
    }

    pub fn len(&self, key: &[u8]) -> u32 {
        unsafe { Commit::ptr_len(self, key) }
    }
    pub fn read<'a>(&'a self, key: &[u8]) -> &'a [u8] {
        unsafe { Commit::ptr_read(self, key) }
    }
    pub fn count(&self, start: &[u8], end: &[u8]) -> u32 {
        unsafe { Commit::ptr_count(self, start, end) }
    }
    pub fn list<'a>(&'a self, start: &[u8], end: &[u8]) -> Vec<&'a [u8]> {
        unsafe { Commit::ptr_list(self, start, end) }
    }
    pub fn scan<'a>(&'a self, start: &[u8], end: &[u8]) -> Vec<(&'a [u8], &'a [u8])> {
        unsafe { Commit::ptr_scan(self, start, end) }
    }

    /// callback may be called with multiple values for a same key
    ///
    /// consider only the first one
    ///
    /// callback may also be called with empty value, which means the key is not present, beware
    unsafe fn ptr_historic_scan<'a>(
        mut commit: *const Commit,
        start: &[u8],
        end: &[u8],
        mut callback: impl FnMut(&'a [u8], &'a [u8]),
    ) {
        if !start
            .len()
            .checked_add(end.len())
            .is_some_and(|x| x < u32::MAX as usize)
        {
            return;
        }
        while let Some(reference) = commit.as_ref() {
            for (k, v) in &reference.changes {
                if k.len() >= start.len() + end.len() && k.starts_with(start) && k.ends_with(end) {
                    callback(k, v);
                }
            }
            commit = reference.prev;
        }
    }
}

impl<'a> Snapshot<'a> {
    pub fn len(&self, key: &[u8]) -> u32 {
        self.commit.map(|x| x.len(key)).unwrap_or(0)
    }
    pub fn read(&self, key: &[u8]) -> &'a [u8] {
        self.commit.map(|x| x.read(key)).unwrap_or(&[])
    }
    pub fn count(&self, start: &[u8], end: &[u8]) -> u32 {
        self.commit.map(|x| x.count(start, end)).unwrap_or(0)
    }
    pub fn list(&self, start: &[u8], end: &[u8]) -> Vec<&'a [u8]> {
        self.commit
            .map(|x| x.list(start, end))
            .unwrap_or_else(Vec::new)
    }
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Vec<(&'a [u8], &'a [u8])> {
        self.commit
            .map(|x| x.scan(start, end))
            .unwrap_or_else(Vec::new)
    }
}

impl<'a> Transaction<'a> {
    pub fn len(&mut self, key: &[u8]) -> u32 {
        if key.is_empty() {
            return 0;
        }
        self.read(key).len() as u32
    }
    pub fn read<'b>(&'b mut self, key: &[u8]) -> &'b [u8] {
        if key.is_empty() {
            return &[];
        }
        if let Some(value) = self.commit.changes.get(key) {
            return value;
        }
        self.reads.insert(key.to_vec());
        unsafe { Commit::ptr_read(self.commit.prev, key) }
    }

    pub fn count(&mut self, start: &[u8], end: &[u8]) -> u32 {
        self.register_scan(start, end);
        unsafe { Commit::ptr_count(&self.commit, start, end) }
    }
    pub fn list<'b>(&'b mut self, start: &[u8], end: &[u8]) -> Vec<&'b [u8]> {
        self.register_scan(start, end);
        unsafe { Commit::ptr_list(&self.commit, start, end) }
    }
    pub fn scan<'b>(&'b mut self, start: &[u8], end: &[u8]) -> Vec<(&'b [u8], &'b [u8])> {
        self.register_scan(start, end);
        unsafe { Commit::ptr_scan(&self.commit, start, end) }
    }
    fn register_scan(&mut self, start: &[u8], end: &[u8]) {
        if start
            .len()
            .checked_add(end.len())
            .is_some_and(|x| x <= u32::MAX as usize)
        {
            let bytes = start
                .iter()
                .copied()
                .chain(end.iter().copied())
                .collect::<Box<[u8]>>();
            self.scans.insert((bytes.into_vec(), start.len()));
        }
    }

    pub fn write(&mut self, key: &[u8], value: &[u8]) {
        if key.is_empty() {
            return;
        }
        assert!(key.len() <= u32::MAX as usize);
        assert!(value.len() <= u32::MAX as usize);
        self.commit.changes.insert(key.to_vec(), value.to_vec());
    }
    pub fn commit(self) -> Result<Duration, TransactionError> {
        // TODO! don't commit empty commits
        let Transaction {
            database,
            commit:
                Commit {
                    prev: mut known_master,
                    time: _,
                    changes,
                },
            reads,
            scans,
        } = self;
        let mut time = now_since_epoch();
        let commit_ptr = Box::into_raw(Box::new(Commit {
            prev: known_master,
            time,
            changes,
        }));
        loop {
            match database.resolved_master.compare_exchange(
                known_master as *mut _,
                commit_ptr,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(old_master) => {
                    assert_eq!(old_master, known_master as *mut _);
                    break;
                }
                Err(new_master) => {
                    let commit = unsafe { commit_ptr.as_mut().unwrap_unchecked() };
                    let mut new_changes = new_master as *const Commit;
                    while let Some(reference) = unsafe { new_changes.as_ref() } {
                        for key in &reads {
                            if reference.changes.get(key).is_some() {
                                return Err(TransactionError::Conflict);
                            }
                        }
                        for (key, _) in &reference.changes {
                            for (start_end, start_len) in &scans {
                                if key.len() >= start_end.len()
                                    && key.starts_with(&start_end[..*start_len])
                                    && key.ends_with(&start_end[*start_len..])
                                {
                                    return Err(TransactionError::Conflict);
                                }
                            }
                        }
                        new_changes = reference.prev;
                        if new_changes == known_master {
                            break;
                        }
                    }
                    time = now_since_epoch();
                    known_master = new_master;
                    commit.time = time;
                    commit.prev = new_master;
                }
            }
        }
        self.database.persist().map_err(TransactionError::Io)?;
        Ok(time)
    }
    pub fn rollback(self) {
        drop(self)
    }
}

fn now_since_epoch() -> Duration {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("system time must be after UNIX_EPOCH")
}
