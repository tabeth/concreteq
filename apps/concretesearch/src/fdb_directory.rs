use foundationdb::{Database, RangeOption};
use foundationdb::tuple::Subspace;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError, LockError};
use tantivy::directory::{
    AntiCallToken, Directory, FileHandle, MmapDirectory, TerminatingWrite, WatchCallback,
    WatchHandle, WritePtr, DirectoryLock, Lock,
};
use tempfile::NamedTempFile;
use tokio::task::JoinHandle;

// Constants for file chunking
const CHUNK_SIZE: usize = 80_000; // 80KB chunks. FoundationDB has a 100KB value limit, so we keep enough headroom.
const BUFFER_SIZE: usize = 90_000; // Buffer size triggering a flush. slightly larger than chunk size to batch writes.

/// Metadata stored in FDB for each file (similar to an inode in a filesystem).
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileRegistry {
    pub len: u64,
    pub created_at: u64,
}

/// Helper Function: Safe Blocking
/// 
/// Tantivy's `Directory` trait is synchronous (blocking calls like `read`, `write`), 
/// but FoundationDB and our networking logic are asynchronous (async/await).
/// 
/// This helper bridges the two worlds:
/// 1. If we are already running inside a Tokio runtime (e.g., in a request handler), we MUST use `block_in_place` 
///    to avoid "blocking the thread" which would freeze the async scheduler.
/// 2. If we are not in a runtime (e.g., standard tests or thread), we can just use `futures::executor::block_on`.
fn safe_block_on<F: std::future::Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(_) => tokio::task::block_in_place(|| futures::executor::block_on(future)),
        Err(_) => futures::executor::block_on(future),
    }
}

/// **FdbDirectory**: Custom Storage Backend for Tantivy
///
/// Instead of storing index files on the local disk (like `MmapDirectory`), this implementation
/// stores them as Key-Value pairs in FoundationDB.
/// 
/// **Key Concept**:
/// - Writes are sent to FDB in chunks.
/// - Reads verify if the file is in the local cache (`/tmp/concrete_cache`).
/// - If cached: Read from disk (fast).
/// - If not cached: Download from FDB -> Save to Cache -> Read from disk.
/// This "Lazy Loading" strategy balances network usage and performance.
#[derive(Clone)]
pub struct FdbDirectory {
    db: Arc<Database>,
    subspace: Subspace,
    cache_path: PathBuf,
    // We reuse Tantivy's optimized `MmapDirectory` to handle the actual reading of cached files.
    // This gives us memory-mapped IO performance for free once the file is downloaded.
    local_cache: MmapDirectory,
    rt: tokio::runtime::Handle,
}

impl Debug for FdbDirectory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FdbDirectory {{ cache_path: {:?} }}", self.cache_path)
    }
}

impl FdbDirectory {
    /// Creates a new FdbDirectory connected to the given Database and Subspace prefix.
    pub fn new(db: Arc<Database>, prefix: &[u8], cache_path: PathBuf) -> tantivy::Result<Self> {
        let subspace = Subspace::from(prefix);
        // Ensure the cache directory exists
        std::fs::create_dir_all(&cache_path)?;
        let local_cache = MmapDirectory::open(&cache_path)?;
        let rt = tokio::runtime::Handle::current();
        Ok(Self {
            db,
            subspace,
            cache_path,
            local_cache,
            rt,
        })
    }

    /// Converts a Path object to a string safe for FDB keys.
    fn get_key_str(path: &Path) -> String {
        path.file_name()
            .and_then(|os_str| os_str.to_str())
            .unwrap_or_else(|| path.to_str().unwrap())
            .to_string()
    }

    /// Fetches the file metadata (`FileRegistry`) from FDB.
    async fn fetch_file_registry(&self, path: &Path) -> Result<Option<FileRegistry>, OpenReadError> {
        let path_str = Self::get_key_str(path);
        let trx = self.db.create_trx().map_err(|e| OpenReadError::IoError {
            io_error: io::Error::new(io::ErrorKind::Other, e.to_string()).into(),
            filepath: path.to_path_buf(),
        })?;
        
        // Key format: (prefix, "meta", filename)
        let key = self.subspace.pack(&("meta", path_str));
        match trx.get(&key, false).await {
            Ok(Some(val)) => {
                let registry: FileRegistry = serde_json::from_slice(&val).map_err(|e| OpenReadError::IoError {
                    io_error: io::Error::new(io::ErrorKind::InvalidData, e.to_string()).into(),
                    filepath: path.to_path_buf(),
                })?;
                Ok(Some(registry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(OpenReadError::IoError {
                io_error: io::Error::new(io::ErrorKind::Other, e.to_string()).into(),
                filepath: path.to_path_buf(),
            }),
        }
    }

    /// Ensures the file exists in the local cache. If not, it downloads it from FDB.
    async fn ensure_cached(&self, path: &Path) -> Result<(), OpenReadError> {
        let local_path = self.cache_path.join(path);
        if local_path.exists() {
             return Ok(());
        }

        // Check if file exists in FDB
        let registry = self.fetch_file_registry(path).await?;
        let _registry = registry.ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;

        // Create a temporary file to download chunks into
        let mut temp_file = NamedTempFile::new_in(&self.cache_path).map_err(|e| OpenReadError::IoError {
            io_error: e.into(),
            filepath: path.to_path_buf(),
        })?;

        let trx = self.db.create_trx().map_err(|e| OpenReadError::IoError {
             io_error: io::Error::new(io::ErrorKind::Other, e.to_string()).into(),
             filepath: path.to_path_buf()
        })?;

        let path_str = Self::get_key_str(path);
        // Define the range of keys for this file's blobs.
        // FDB keys refer to chunks: (prefix, "blob", filename, chunk_id 0..N)
        let range_start = self.subspace.pack(&("blob", path_str.clone(), 0 as u64));
        let range_end = self.subspace.pack(&("blob", path_str, 0xFFFF_FFFF_FFFF_FFFF as u64));

        let range = RangeOption {
            mode: foundationdb::options::StreamingMode::WantAll,
            ..RangeOption::from(range_start..range_end)
        };
        
        // Stream all chunks from FDB
        let chunks = trx.get_range(&range, 1_000, false).await.map_err(|e| OpenReadError::IoError {
             io_error: io::Error::new(io::ErrorKind::Other, e.to_string()).into(),
             filepath: path.to_path_buf()
        })?;

        // Write chunks to the temp file
        for chunk_kv in chunks {
            temp_file.write_all(&chunk_kv.value()).map_err(|e| OpenReadError::IoError {
                io_error: e.into(),
                filepath: path.to_path_buf(),
            })?;
        }
        
        temp_file.flush().map_err(|e| OpenReadError::IoError {
             io_error: e.into(),
             filepath: path.to_path_buf()
        })?;
        
        // Atomically move the temp file to the final destination in the cache
        let dest_path = self.cache_path.join(path);
        temp_file.persist(&dest_path).map_err(|e| OpenReadError::IoError {
             io_error: e.error.into(),
             filepath: path.to_path_buf()
        })?;

        Ok(())
    }
}

/// **Directory Trait Implementation**
/// This tells Tantivy how to interact with our custom storage.
impl Directory for FdbDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        // Before giving a handle, ensure the file is downloaded to cache.
        safe_block_on(async {
            self.ensure_cached(path).await
        })?;
        
        // Delegate reading to the local MmapDirectory cache
        self.local_cache.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        // Try deleting from local cache first (best effort)
        let _ = self.local_cache.delete(path); 

        // Delete from FDB (Transactional)
        safe_block_on(async {
             let trx = self.db.create_trx().map_err(|e| DeleteError::IoError {
                io_error: io::Error::new(io::ErrorKind::Other, e.to_string()).into(),
                filepath: path.to_path_buf(),
            })?;
            
            let path_str = Self::get_key_str(path);
            
            // Delete Metadata
            let meta_key = self.subspace.pack(&("meta", path_str.clone()));
            trx.clear(&meta_key);
            
            // Delete All Blobs (Chunks)
            let range_start = self.subspace.pack(&("blob", path_str.clone(), 0 as u64));
            let range_end = self.subspace.pack(&("blob", path_str, 0xFFFF_FFFF_FFFF_FFFF as u64));
            trx.clear_range(&range_start, &range_end);

            trx.commit().await.map_err(|e| DeleteError::IoError {
                 io_error: io::Error::new(io::ErrorKind::Other, e.to_string()).into(),
                 filepath: path.to_path_buf(),
            })?;
            Ok::<(), DeleteError>(())
        })
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        // Check existence by looking for metadata in FDB
         let res = safe_block_on(async {
             Ok::<bool, OpenReadError>(self.fetch_file_registry(path).await?.is_some())
        });
        res
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
         // Return a custom Writer that intercepts writes and sends them to FDB
         Ok(WritePtr::new(Box::new(FdbWriter::new(
             self.db.clone(),
             self.subspace.clone(),
             path.to_path_buf(),
             self.cache_path.clone(),
             self.rt.clone(),
         ))))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        // Utility for reading small config files entirely into memory
        let handle = self.get_file_handle(path)?;
        let len = handle.len();
        let bytes = handle.read_bytes(0..len).map_err(|e| OpenReadError::IoError {
            io_error: e.into(),
            filepath: path.to_path_buf(),
        })?;
        Ok(bytes.as_slice().to_vec())
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        // Utility for writing small files in one go
        let mut writer = self.open_write(path).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        writer.write_all(data)?;
        
        // Ensure data is properly committed
        writer.terminate()
    }

    fn sync_directory(&self) -> io::Result<()> {
        // FDB handles durability, but we might want to sync local filesystem cache
        self.local_cache.sync_directory()
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        // Watch functionality (not needed for this prototype)
        Ok(WatchHandle::empty())
    }
    
    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        // We defer locking to the local filesystem for simplicity in this prototype.
        // Distributed locking would use FDB directly.
        let guard = self.local_cache.acquire_lock(_lock)?;
        Ok(guard)
    }
}

/// **Custom Writer**
/// 
/// Intercepts data written by Tantivy and:
/// 1. Buffers it in memory.
/// 2. Slices it into 80KB chunks.
/// 3. Spawns async tasks to write chunks to FDB.
/// 4. Also maintains a local copy for immediate cache availability.
struct FdbWriter {
    db: Arc<Database>,
    subspace: Subspace,
    path: PathBuf,
    buffer: Vec<u8>,
    chunk_counter: u64,
    write_tasks: Vec<JoinHandle<io::Result<()>>>,
    // Also write to local temp file to avoid re-downloading immediately
    local_file: Option<NamedTempFile>, 
    local_path: PathBuf, // The final destination in cache
    total_len: u64,
    rt: tokio::runtime::Handle,
}

impl FdbWriter {
    fn new(db: Arc<Database>, subspace: Subspace, path: PathBuf, cache_dir: PathBuf, rt: tokio::runtime::Handle) -> Self {
        // Create a temp file in the cache directory
        let local_file = NamedTempFile::new_in(&cache_dir).ok();

        Self {
            db,
            subspace,
            path: path.clone(),
            buffer: Vec::with_capacity(BUFFER_SIZE),
            chunk_counter: 0,
            write_tasks: Vec::new(),
            local_file,
            local_path: cache_dir.join(path),
            total_len: 0,
            rt,
        }
    }

    /// Flushes the current buffer as a single chunk to FDB.
    fn flush_chunk(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let chunk_data = self.buffer.clone();
        let chunk_id = self.chunk_counter;
        self.chunk_counter += 1;
        self.buffer.clear();

        let db = self.db.clone();
        let subspace = self.subspace.clone();
        let path_str = FdbDirectory::get_key_str(&self.path);

        // Spawn a background task to write to FDB.
        // We don't await here because `write` calls must be fast and non-blocking.
        // We collect the task handle to `await` on all of them later in `terminate()`.
        let handle = self.rt.spawn(async move {
            let trx = db.create_trx().map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            let key = subspace.pack(&("blob", path_str.clone(), chunk_id));
            trx.set(&key, &chunk_data);
            trx.commit().await.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            Ok(())
        });

        self.write_tasks.push(handle);
    }
}

impl Write for FdbWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        self.total_len += buf.len() as u64;
        
        // Mirror write to local temp file
        if let Some(f) = &mut self.local_file {
            f.write_all(buf)?;
        }

        if self.buffer.len() >= CHUNK_SIZE {
            self.flush_chunk();
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_chunk();
        if let Some(f) = &mut self.local_file {
            f.flush()?;
        }
        Ok(())
    }
}

impl TerminatingWrite for FdbWriter {
    /// Called when Tantivy is done writing the file.
    /// This is where we ensure durability.
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.flush_chunk();
        
        let tasks = std::mem::take(&mut self.write_tasks);
        
        // Wait for all async write tasks to complete.
        safe_block_on(async {
             // Wait for all chunks to be written
             let results = join_all(tasks).await;
             for res in results {
                 res.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))??;
             }
             
             // After all chunks are safely written, we write the "metadata" file.
             // This marks the file as successfully completed/valid.
             let reg = FileRegistry {
                 len: self.total_len,
                 created_at: 0, // In real app, use timestamp
             };
             let val = serde_json::to_vec(&reg)?;
             
             let path_str = FdbDirectory::get_key_str(&self.path);
             let key = self.subspace.pack(&("meta", path_str.clone()));

             self.db.run(|trx, _maybe_committed| {
                 let key = key.clone();
                 let val = val.clone();
                 async move {
                    trx.set(&key, &val);
                    Ok(())
                 }
             }).await.map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
             
             Ok::<(), io::Error>(())
        })?;

        // Move the fully written temp file to the final cache location.
        // This makes it immediately available for reading without re-downloading.
        if let Some(f) = self.local_file.take() {
            if let Err(e) = f.persist(&self.local_path) {
                // If persist fails, we just return error. The file is in FDB anyway, so valid.
                // But it's good to know.
                return Err(e.error);
            }
        }

        Ok(())
    }
}

// ------------------------------------------------------------------------------------------------
// TESTS
// ------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_fdb_directory_write_read() {
        // This test requires a running FDB instance locally.
        crate::test_utils::init_fdb();
        
        let db = match Database::new(None) {
            Ok(db) => Arc::new(db),
            Err(_) => return, // Skip if no FDB
        };
        crate::test_utils::clear_subspace(&db, b"test_prefix").await;

        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();
        let directory = FdbDirectory::new(db.clone(), b"test_prefix", cache_path.clone()).unwrap();

        let path = Path::new("test_file");
        let payload = b"Hello FoundationDB!";
        
        // Test Atomic Write (Write entire file at once)
        directory.atomic_write(path, payload).unwrap();
        
        // Assert file exists in local cache after write
        assert!(cache_path.join(path).exists());
        
        // Test Read from Cache
        let data = directory.atomic_read(path).unwrap();
        assert_eq!(data, payload);
        
        // Simulate Cache Miss by manually deleting local file
        std::fs::remove_file(cache_path.join(path)).unwrap();
        assert!(!cache_path.join(path).exists());
        
        // Re-open directory to ensure handle cache doesn't lie to us
        let directory2 = FdbDirectory::new(db.clone(), b"test_prefix", cache_path.clone()).unwrap();

        // Test Read Lazy (should trigger download from FDB)
        let data_lazy = directory2.atomic_read(path).unwrap();
        assert_eq!(data_lazy, payload);
        assert!(cache_path.join(path).exists()); // Should be restored in cache
        
        // Test Delete
        directory.delete(path).unwrap();
        assert!(!directory.exists(path).unwrap());
    }
}
