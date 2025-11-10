use std::collections::HashMap;
use std::io::{self, IoSlice, SeekFrom};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use fs4::tokio::AsyncFileExt;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

lazy_static::lazy_static! {
    static ref LOCKS: Arc<Mutex<HashMap<PathBuf, Arc<RwLock<()>>>>> = Arc::new(Mutex::new(HashMap::new()));
}

async fn rw_lock(path: PathBuf) -> Arc<RwLock<()>> {
    LOCKS.lock().await.entry(path).or_default().clone()
}

struct PathLock<T> {
    key: PathBuf,
    lock: Option<T>,
}

pub type SharedLock = OwnedRwLockReadGuard<()>;
pub type ExclusiveLock = OwnedRwLockWriteGuard<()>;

impl<T> PathLock<T> {
    fn new(key: PathBuf, lock: T) -> Self {
        Self {
            key,
            lock: Some(lock),
        }
    }
}

impl PathLock<ExclusiveLock> {
    async fn exclusive(path: PathBuf) -> PathLock<ExclusiveLock> {
        PathLock::new(path.clone(), rw_lock(path).await.write_owned().await)
    }
}

impl PathLock<SharedLock> {
    async fn shared(path: PathBuf) -> Self {
        PathLock::new(path.clone(), rw_lock(path).await.read_owned().await)
    }
}

impl<T> Drop for PathLock<T> {
    fn drop(&mut self) {
        drop(self.lock.take());
        let key = self.key.clone();
        tokio::spawn(async move {
            let mut locks = LOCKS.lock().await;
            if locks.get(&key).is_some_and(|lock| lock.try_write().is_ok()) {
                locks.remove(&key);
            }
        });
    }
}

pub struct LockedFile<T> {
    file: File,
    _guard: PathLock<T>,
}

impl<T> Deref for LockedFile<T> {
    type Target = File;
    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl<T> DerefMut for LockedFile<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl<T> Drop for LockedFile<T> {
    fn drop(&mut self) {
        if let Err(err) = self.file.unlock() {
            tracing::error!("Failed to unlock file: {}", err);
        }
    }
}

impl<T> LockedFile<T>
where
    T: Unpin,
{
    fn file(self: Pin<&mut Self>) -> Pin<&mut File> {
        Pin::new(&mut self.get_mut().file)
    }
}

impl LockedFile<SharedLock> {
    pub async fn shared(path: impl AsRef<Path>, options: &OpenOptions) -> io::Result<Self> {
        let _guard = PathLock::shared(path.as_ref().to_path_buf()).await;
        let file = options.open(path).await?;
        let file = tokio::task::spawn_blocking(move || {
            file.lock_shared()?;
            io::Result::Ok(file)
        })
        .await??;
        Ok(LockedFile { file, _guard })
    }
}

impl LockedFile<ExclusiveLock> {
    pub async fn exclusive(path: impl AsRef<Path>, options: &OpenOptions) -> io::Result<Self> {
        let _guard = PathLock::exclusive(path.as_ref().to_path_buf()).await;
        let file = options.open(path).await?;
        let file = tokio::task::spawn_blocking(move || {
            file.lock_exclusive()?;
            io::Result::Ok(file)
        })
        .await??;
        Ok(LockedFile { file, _guard })
    }
}

impl<T> AsyncRead for LockedFile<T>
where
    T: Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.file().poll_read(cx, buf)
    }
}

impl<T> AsyncWrite for LockedFile<T>
where
    T: Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.file().poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.file().poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.file().poll_shutdown(cx)
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.file().poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.file.is_write_vectored()
    }
}

impl<T> AsyncSeek for LockedFile<T>
where
    T: Unpin,
{
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        self.file().start_seek(position)
    }
    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        self.file().poll_complete(cx)
    }
}
