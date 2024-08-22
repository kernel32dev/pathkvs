use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{Error, ErrorKind, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
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

struct Commit {
    prev: *const Commit,
    changes: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Clone)]
pub struct Transaction<'a> {
    database: &'a Database,
    snapshot: *const Commit,
    reads: HashSet<Vec<u8>>,
    changes: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Clone, Copy)]
pub struct ReadOnlyTransaction<'a> {
    snapshot: *const Commit,
    _marker: PhantomData<&'a Database>,
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
            snapshot: self.serialized_master.load(Ordering::SeqCst),
            reads: HashSet::new(),
            changes: HashMap::new(),
        }
    }
    pub fn write(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
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
    pub fn start_reads<'a>(&'a self) -> ReadOnlyTransaction<'a> {
        ReadOnlyTransaction {
            snapshot: self.serialized_master.load(Ordering::SeqCst),
            _marker: PhantomData,
        }
    }
    pub fn len<'b>(&'b self, key: &[u8]) -> u32 {
        self.start_reads().len(key)
    }
    pub fn read<'b>(&'b self, key: &[u8]) -> &'b [u8] {
        self.start_reads().read(key)
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
    unsafe fn read_key<'a>(mut commit: *const Commit, key: &[u8]) -> &'a [u8] {
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
}

impl<'a> Transaction<'a> {
    pub fn len<'b>(&'b mut self, key: &[u8]) -> u32 {
        self.read(key).len() as u32
    }
    pub fn read<'b>(&'b mut self, key: &[u8]) -> &'b [u8] {
        if let Some(value) = self.changes.get(key) {
            return value;
        }
        self.reads.insert(key.to_vec());
        unsafe { Commit::read_key(self.snapshot, key) }
    }
    pub fn write(&mut self, key: &[u8], value: &[u8]) {
        assert!(key.len() <= u32::MAX as usize);
        assert!(value.len() <= u32::MAX as usize);
        self.changes.insert(key.to_vec(), value.to_vec());
    }
    pub fn commit(self) -> Result<(), TransactionError> {
        let Transaction {
            database,
            snapshot: mut known_master,
            reads,
            changes,
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
                    unsafe {
                        while let Some(reference) = new_changes.as_ref() {
                            for key in &reads {
                                if reference.changes.get(key).is_some() {
                                    return Err(TransactionError::Conflict);
                                }
                            }
                            new_changes = reference.prev;
                            if new_changes == known_master {
                                break;
                            }
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

impl<'a> ReadOnlyTransaction<'a> {
    pub fn len(&self, key: &[u8]) -> u32 {
        self.read(key).len() as u32
    }
    pub fn read(&self, key: &[u8]) -> &'a [u8] {
        unsafe { Commit::read_key(self.snapshot, key) }
    }
}
