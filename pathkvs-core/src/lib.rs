use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    io::{Error, ErrorKind, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{
        atomic::{AtomicPtr, Ordering},
        Mutex,
    },
};

use error::TransactionError;

pub mod error;

pub struct Database {
    resolved_master: AtomicPtr<Commit>,
    serialized_master: AtomicPtr<Commit>,
    history_sink: Mutex<HistorySink>,
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
    pub fn create(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file = std::fs::File::options()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)?;
        Ok(Self {
            resolved_master: AtomicPtr::new(std::ptr::null_mut()),
            serialized_master: AtomicPtr::new(std::ptr::null_mut()),
            history_sink: Mutex::new(HistorySink {
                output_stream: file,
                cursor: 0,
            }),
        })
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let mut file = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut changes = HashMap::new();
        let mut cursor = 0u64;

        let result: Result<(), Error> = (|| loop {
            let mut commit_changes = Vec::new();
            let mut commit_cursor = 0u64;

            let mut kv_len = [0; 4];
            file.read_exact(&mut kv_len)?;
            commit_cursor += 4;
            let kv_len = u32::from_le_bytes(kv_len);

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

                commit_changes.push((k, v));
            }

            changes.extend(commit_changes);
            cursor += commit_cursor;
        })();

        match result {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {}
            Err(error) => return Err(error),
        }

        file.set_len(cursor)?;

        let commit_ptr = Box::into_raw(Box::new(Commit {
            prev: std::ptr::null(),
            changes,
        }));

        Ok(Self {
            resolved_master: AtomicPtr::new(commit_ptr),
            serialized_master: AtomicPtr::new(commit_ptr),
            history_sink: Mutex::new(HistorySink {
                output_stream: file,
                cursor,
            }),
        })
    }
    pub fn start_writes<'a>(&'a self) -> Transaction<'a> {
        Transaction {
            database: self,
            commit: Commit {
                prev: self.serialized_master.load(Ordering::SeqCst),
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
            Ok(()) => Ok(()),
            Err(TransactionError::Conflict) => {
                unreachable!("a write only transaction cannot conflict")
            }
            Err(TransactionError::Io(error)) => Err(error),
        }
    }
    pub fn snapshot<'a>(&'a self) -> Snapshot<'a> {
        unsafe {
            Snapshot {
                commit: self.serialized_master.load(Ordering::SeqCst).as_ref(),
            }
        }
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
        loop {
            let mut resolved_master = self.resolved_master.load(Ordering::SeqCst) as *const Commit;
            let mut workbench = self.history_sink.lock().unwrap();
            let serialized_master = self.serialized_master.load(Ordering::SeqCst) as *const Commit;
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
                workbench.output_stream.flush()?;
                workbench.output_stream.sync_all()?;
                self.serialized_master
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
    pub fn commit(self) -> Result<(), TransactionError> {
        let Transaction {
            database,
            commit:
                Commit {
                    prev: mut known_master,
                    changes,
                },
            reads,
            scans,
        } = self;
        let commit_ptr = Box::into_raw(Box::new(Commit {
            prev: known_master,
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
                    known_master = new_master;
                    commit.prev = new_master;
                }
            }
        }
        self.database.persist().map_err(TransactionError::Io)
    }
    pub fn rollback(self) {
        drop(self)
    }
}
