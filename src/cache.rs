use anyhow::Result;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs as tfs,
    io::AsyncWriteExt,
    sync::{Mutex, mpsc},
};
use tracing::debug;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn walk_cache_dir<F>(root: &Path, mut visit: F) -> Result<()>
where
    F: FnMut(&Path) + Send,
{
    let mut stack = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(dir) = stack.pop() {
        let Ok(mut rd) = tfs::read_dir(&dir).await else {
            continue;
        };
        while let Ok(Some(entry)) = rd.next_entry().await {
            let Ok(ty) = entry.file_type().await else {
                continue;
            };
            if ty.is_dir() {
                stack.push(entry.path());
            } else {
                files.push(entry.path());
            }
        }
    }
    for path in &files {
        visit(path);
    }
    Ok(())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Meta {
    expires_at: u64,
    created_at: u64,
    size: u64,
    pub content_type: Option<String>,
    swr_expires_at: Option<u64>,
    last_access_at: u64,
    pub etag: Option<String>,
    #[serde(default)]
    pub last_modified: Option<String>, // Last-Modified 헤더값 (RFC 7232 조건부 요청용)
}

#[derive(Debug, Clone)]
pub struct CacheFileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub content_type: Option<String>,
    pub is_fresh: bool,
    pub etag: Option<String>,
    pub created_at: u64,
    pub last_modified: Option<String>,
}

pub struct CacheStoreOptions {
    pub content_type: Option<String>,
    pub ttl: Option<Duration>,
    pub swr: Option<Duration>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

#[derive(Clone)]
pub struct DiskCache {
    root: PathBuf,
    max_size: u64,
    inner: Arc<Mutex<CacheInner>>,
    default_ttl: Duration,
    policy: crate::config::EvictionPolicy,
    touch_tx: mpsc::Sender<PathBuf>,
    evict_tx: mpsc::Sender<()>,
}

struct CacheInner {
    meta_cache: LruCache<String, Meta>,
    index: Vec<IndexEntry>,
    total_size: u64,
}

#[derive(Clone)]
struct IndexEntry {
    key: String,
    base_path: PathBuf,
    created_at: u64,
    size: u64,
    last_access_at: u64,
}

impl DiskCache {
    pub async fn new<P: AsRef<Path>>(
        root: P,
        max_size: u64,
        default_ttl: Duration,
        policy: crate::config::EvictionPolicy,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        tfs::create_dir_all(&root).await?;
        let (touch_tx, mut touch_rx) = mpsc::channel::<PathBuf>(1024);
        let (evict_tx, mut evict_rx) = mpsc::channel::<()>(1);

        tokio::spawn(async move {
            let mut batch: Vec<PathBuf> = Vec::new();
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    Some(meta_path) = touch_rx.recv() => {
                        batch.push(meta_path);
                        if batch.len() >= 100 {
                            Self::flush_touch_batch(&mut batch).await;
                        }
                    }
                    _ = ticker.tick() => {
                        if !batch.is_empty() {
                            Self::flush_touch_batch(&mut batch).await;
                        }
                    }
                }
            }
        });

        let cache_capacity = NonZeroUsize::new(10000).unwrap();
        let cache = Self {
            root: root.clone(),
            max_size,
            inner: Arc::new(Mutex::new(CacheInner {
                meta_cache: LruCache::new(cache_capacity),
                index: Vec::new(),
                total_size: 0,
            })),
            default_ttl,
            policy,
            touch_tx,
            evict_tx,
        };

        let cache_for_evict = cache.clone();
        tokio::spawn(async move {
            while evict_rx.recv().await.is_some() {
                if let Err(e) = cache_for_evict.enforce_size_limit().await {
                    debug!(target: "cache", error=?e, "eviction failed");
                }
            }
        });

        cache.rebuild_index().await?;

        Ok(cache)
    }

    async fn flush_touch_batch(batch: &mut Vec<PathBuf>) {
        for meta_path in batch.drain(..) {
            if let Ok(bytes) = tfs::read(&meta_path).await {
                if let Ok(mut meta) = serde_json::from_slice::<Meta>(&bytes) {
                    let bin_path = meta_path.with_extension("bin");
                    if tfs::metadata(&bin_path)
                        .await
                        .map(|m| m.len() != meta.size)
                        .unwrap_or(true)
                    {
                        continue;
                    }
                    meta.last_access_at = now_secs();
                    if let Ok(new_bytes) = serde_json::to_vec(&meta)
                        && let Err(e) = Self::write_file(&meta_path, &new_bytes).await
                    {
                        debug!(target: "cache", error=?e, path=?meta_path, "touch write failed");
                    }
                } else {
                    let base = meta_path.with_extension("");
                    let _ = tfs::remove_file(base.with_extension("bin")).await;
                    let _ = tfs::remove_file(&meta_path).await;
                    debug!(target: "cache", path=?meta_path, "removed corrupt meta");
                }
            }
        }
    }

    async fn rebuild_index(&self) -> Result<()> {
        let now = now_secs();
        let mut meta_paths = Vec::new();
        walk_cache_dir(&self.root, |path| {
            if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                meta_paths.push(path.to_path_buf());
            }
        })
        .await?;

        let mut entries = Vec::new();
        let mut total = 0u64;
        for meta_path in meta_paths {
            let base = meta_path.with_extension("");
            let Ok(bytes) = tfs::read(&meta_path).await else {
                continue;
            };
            let Ok(meta) = serde_json::from_slice::<Meta>(&bytes) else {
                continue;
            };
            if tfs::metadata(base.with_extension("bin")).await.is_err() {
                continue;
            }
            let fully_expired = now > meta.expires_at
                && meta.swr_expires_at.is_none_or(|swr_end| now > swr_end);
            if fully_expired {
                continue;
            }
            let key = base
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            total += meta.size;
            entries.push(IndexEntry {
                key,
                base_path: base,
                created_at: meta.created_at,
                size: meta.size,
                last_access_at: meta.last_access_at,
            });
        }

        let mut inner = self.inner.lock().await;
        inner.index = entries;
        inner.total_size = total;
        Ok(())
    }

    /// 현재 캐시 사용량 (bytes) 과 항목 수 반환 (best-effort, 만료 항목 포함)
    pub async fn size_info(&self) -> (u64, u64) {
        let mut total_bytes = 0u64;
        let mut entries = 0u64;
        let _ = walk_cache_dir(&self.root, |path| {
            if path.extension().and_then(|s| s.to_str()) == Some("bin") {
                total_bytes += std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                entries += 1;
            }
        })
        .await;
        (total_bytes, entries)
    }

    /// 전체 캐시 비우기 (디렉토리 내 .bin / .meta / .vary 파일 삭제)
    pub async fn clear_all(&self) -> Result<()> {
        let mut paths = Vec::new();
        walk_cache_dir(&self.root, |path| {
            paths.push(path.to_path_buf());
        })
        .await?;
        for path in paths {
            let _ = tfs::remove_file(&path).await;
        }
        Ok(())
    }

    fn key_path(&self, key: &str) -> PathBuf {
        let mut hasher = blake3::Hasher::new();
        hasher.update(key.as_bytes());
        let hash = hasher.finalize().to_hex().to_string();
        let (a, b) = hash.split_at(2);
        self.root.join(a).join(b)
    }

    // Vary 인덱스 파일 경로 (.vary 확장자)
    fn vary_index_path(&self, base_key: &str) -> PathBuf {
        self.key_path(base_key).with_extension("vary")
    }

    async fn write_file(path: &Path, data: &[u8]) -> Result<()> {
        if let Some(parent) = path.parent() {
            tfs::create_dir_all(parent).await?;
        }
        let temp_path = Self::temp_path_for(path);
        let mut f = tfs::File::create(&temp_path).await?;
        f.write_all(data).await?;
        f.sync_all().await?;
        tfs::rename(&temp_path, path).await?;
        Ok(())
    }

    fn temp_path_for(path: &Path) -> PathBuf {
        let id = format!(
            "{}.{}",
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let extension = path
            .extension()
            .and_then(|s| s.to_str())
            .map(|s| format!("{s}.{id}.tmp"))
            .unwrap_or_else(|| format!("{id}.tmp"));
        path.with_extension(extension)
    }

    pub async fn temp_data_path(&self, key: &str) -> Result<PathBuf> {
        let path = Self::temp_path_for(&self.key_path(key).with_extension("bin"));
        if let Some(parent) = path.parent() {
            tfs::create_dir_all(parent).await?;
        }
        Ok(path)
    }

    /// Validate (size match, freshness) and convert Meta+data_path → CacheFileEntry.
    /// Returns Ok(None) and cleans up files if validation fails.
    async fn validate_and_build_entry(
        &self,
        key: &str,
        meta: Meta,
        data_path: PathBuf,
        meta_path: PathBuf,
    ) -> Result<Option<CacheFileEntry>> {
        let data_meta = match tfs::metadata(&data_path).await {
            Ok(m) if m.is_file() => m,
            _ => return Ok(None),
        };

        if data_meta.len() != meta.size {
            let _ = tfs::remove_file(&data_path).await;
            let _ = tfs::remove_file(&meta_path).await;
            debug!(target: "cache", key=%key, expected=meta.size, actual=data_meta.len(), "removed size-mismatched entry");
            return Ok(None);
        }

        let now = now_secs();
        let is_fresh = if now > meta.expires_at {
            if meta.swr_expires_at.is_some_and(|swr_end| now <= swr_end) {
                false
            } else {
                let _ = tfs::remove_file(&data_path).await;
                let _ = tfs::remove_file(&meta_path).await;
                debug!(target: "cache", key=%key, "expired entry removed");
                return Ok(None);
            }
        } else {
            true
        };

        let _ = self.touch_tx.try_send(meta_path);
        Ok(Some(CacheFileEntry {
            path: data_path,
            size: meta.size,
            content_type: meta.content_type,
            is_fresh,
            etag: meta.etag,
            created_at: meta.created_at,
            last_modified: meta.last_modified,
        }))
    }

    pub async fn get_file(&self, key: &str) -> Result<Option<CacheFileEntry>> {
        let path = self.key_path(key);
        let meta_path = path.with_extension("meta");
        let data_path = path.with_extension("bin");

        // Fast path: meta already in LRU cache
        {
            let mut inner = self.inner.lock().await;
            if let Some(meta) = inner.meta_cache.get(key).cloned() {
                drop(inner);
                return self
                    .validate_and_build_entry(key, meta, data_path, meta_path)
                    .await;
            }
        }

        // Slow path: read from disk
        let (data_meta, meta_bytes) =
            tokio::join!(tfs::metadata(&data_path), tfs::read(&meta_path));
        let meta_bytes = match (data_meta, meta_bytes) {
            (Ok(m), Ok(bytes)) if m.is_file() => bytes,
            _ => return Ok(None),
        };

        let meta = match serde_json::from_slice::<Meta>(&meta_bytes) {
            Ok(meta) => meta,
            Err(_) => {
                let _ = tfs::remove_file(&data_path).await;
                let _ = tfs::remove_file(&meta_path).await;
                debug!(target: "cache", key=%key, "removed corrupt meta");
                return Ok(None);
            }
        };

        let result = self
            .validate_and_build_entry(key, meta.clone(), data_path, meta_path)
            .await;

        if matches!(&result, Ok(Some(_))) {
            let mut inner = self.inner.lock().await;
            inner.meta_cache.put(key.to_string(), meta);
        }
        result
    }

    pub async fn put_file(
        &self,
        key: &str,
        temp_path: &Path,
        size: u64,
        options: CacheStoreOptions,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let path = self.key_path(key);
        let meta_path = path.with_extension("meta");
        let data_path = path.with_extension("bin");
        if let Some(parent) = data_path.parent() {
            tfs::create_dir_all(parent).await?;
        }

        let now = now_secs();
        let ttl_dur = options.ttl.unwrap_or(self.default_ttl);
        let meta = Meta {
            expires_at: now + ttl_dur.as_secs(),
            created_at: now,
            size,
            content_type: options.content_type,
            swr_expires_at: options.swr.map(|d| now + ttl_dur.as_secs() + d.as_secs()),
            last_access_at: now,
            etag: options.etag,
            last_modified: options.last_modified,
        };
        let meta_json = serde_json::to_vec(&meta)?;
        let meta_temp_path = Self::temp_path_for(&meta_path);
        let mut meta_file = tfs::File::create(&meta_temp_path).await?;
        meta_file.write_all(&meta_json).await?;
        meta_file.sync_all().await?;
        drop(meta_file);

        let _ = tfs::remove_file(&meta_path).await;
        if let Err(e) = tfs::rename(temp_path, &data_path).await {
            let _ = tfs::remove_file(&meta_temp_path).await;
            return Err(e.into());
        }
        if let Err(e) = tfs::rename(&meta_temp_path, &meta_path).await {
            let _ = tfs::remove_file(&meta_temp_path).await;
            let _ = tfs::remove_file(&data_path).await;
            return Err(e.into());
        }

        inner.meta_cache.put(key.to_string(), meta.clone());
        inner.index.push(IndexEntry {
            key: key.to_string(),
            base_path: path.clone(),
            created_at: meta.created_at,
            size: meta.size,
            last_access_at: meta.last_access_at,
        });
        inner.total_size += size;
        drop(inner);

        let _ = self.evict_tx.try_send(());

        Ok(())
    }

    async fn enforce_size_limit(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;

        if inner.total_size <= self.max_size {
            return Ok(());
        }

        debug!(target: "cache", current_bytes=inner.total_size, max_bytes=self.max_size, entries=inner.index.len(), "starting eviction");

        match self.policy {
            crate::config::EvictionPolicy::Fifo => {
                inner.index.sort_by_key(|e| e.created_at);
            }
            crate::config::EvictionPolicy::Lru => {
                inner.index.sort_by_key(|e| e.last_access_at);
            }
            crate::config::EvictionPolicy::Size => {
                inner.index.sort_by_key(|e| std::cmp::Reverse(e.size));
            }
            crate::config::EvictionPolicy::LruSize => {
                inner.index.sort_by(|a, b| {
                    let la = a.last_access_at.cmp(&b.last_access_at);
                    if la == std::cmp::Ordering::Equal {
                        b.size.cmp(&a.size)
                    } else {
                        la
                    }
                });
            }
        }

        let mut to_remove = Vec::new();
        let mut total = inner.total_size;

        for entry in &inner.index {
            if total <= self.max_size {
                break;
            }
            to_remove.push(entry.clone());
            if total >= entry.size {
                total -= entry.size;
            } else {
                total = 0;
            }
        }

        for entry in &to_remove {
            let _ = tfs::remove_file(entry.base_path.with_extension("bin")).await;
            let _ = tfs::remove_file(entry.base_path.with_extension("meta")).await;
            inner.meta_cache.pop(&entry.key);
        }

        inner
            .index
            .retain(|e| !to_remove.iter().any(|r| r.key == e.key));
        inner.total_size = total;

        debug!(target: "cache", final_bytes=total, "eviction complete");
        Ok(())
    }

    // base_key 에 대한 Vary 헤더 이름 목록 조회 (없으면 None)
    pub async fn get_vary_header_names(&self, base_key: &str) -> Result<Option<Vec<String>>> {
        let path = self.vary_index_path(base_key);
        if tfs::metadata(&path).await.is_err() {
            return Ok(None);
        }
        let bytes = match tfs::read(&path).await {
            Ok(b) => b,
            Err(_) => return Ok(None),
        };
        let names: Vec<String> = serde_json::from_slice(&bytes).unwrap_or_default();
        if names.is_empty() || names.iter().any(|name| name == "*") {
            return Ok(None);
        }
        Ok(Some(names))
    }

    // base_key 에 대해 Vary 헤더 이름 목록 저장 (이전 값 덮어쓰기)
    pub async fn set_vary_header_names(&self, base_key: &str, names: &[String]) -> Result<()> {
        if names.is_empty() {
            return Ok(());
        }
        let path = self.vary_index_path(base_key);
        let data = serde_json::to_vec(names)?;
        Self::write_file(&path, &data).await?;
        Ok(())
    }
}
