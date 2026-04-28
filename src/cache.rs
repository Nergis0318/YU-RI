use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use tokio::{
    fs as tfs,
    io::AsyncWriteExt,
    sync::{Mutex, mpsc},
};
use tracing::debug;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Meta {
    expires_at: u64,
    created_at: u64,
    size: u64,
    pub content_type: Option<String>,
    swr_expires_at: Option<u64>, // stale-while-revalidate 만료 시각 (expires_at 이후 추가 허용 구간)
    last_access_at: u64,         // LRU 용 (get 시 갱신)
    pub etag: Option<String>,    // ETag 헤더 (조건부 요청 지원용)
}

#[derive(Debug, Clone)]
pub struct CacheFileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub content_type: Option<String>,
    pub is_fresh: bool,
    pub etag: Option<String>,
    pub created_at: u64,
}

pub struct CacheStoreOptions {
    pub content_type: Option<String>,
    pub ttl: Option<Duration>,
    pub swr: Option<Duration>,
    pub etag: Option<String>,
}

#[derive(Clone)]
pub struct DiskCache {
    root: PathBuf,
    max_size: u64,
    inner: Arc<Mutex<()>>, // simple global lock for size maintenance (can improve with sharded locks)
    default_ttl: Duration,
    // 정책: 구성 단계에서 선택
    policy: crate::config::EvictionPolicy,
    touch_tx: mpsc::Sender<PathBuf>, // 비동기 last_access_at 갱신 큐
    evict_tx: mpsc::Sender<()>,      // 비동기 eviction 트리거
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

        let root_clone = root.clone();
        // 비동기 last_access_at 터치 워커
        tokio::spawn(async move {
            while let Some(meta_path) = touch_rx.recv().await {
                // meta 파일 읽고 last_access_at 갱신 (best-effort)
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
                        meta.last_access_at = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        if let Ok(new_bytes) = serde_json::to_vec(&meta) {
                            // write back (ignore errors)
                            if let Err(e) = Self::write_file(&meta_path, &new_bytes).await {
                                debug!(target: "cache", error=?e, path=?meta_path, "touch write failed");
                            }
                        }
                    } else {
                        // 손상된 meta → 관련 bin 제거
                        let base = meta_path.with_extension("");
                        let _ = tfs::remove_file(base.with_extension("bin")).await;
                        let _ = tfs::remove_file(&meta_path).await;
                        debug!(target: "cache", path=?meta_path, "removed corrupt meta");
                    }
                }
            }
            debug!(target: "cache", root=?root_clone, "touch worker stopped");
        });

        let cache = Self {
            root,
            max_size,
            inner: Arc::new(Mutex::new(())),
            default_ttl,
            policy,
            touch_tx,
            evict_tx,
        };

        // 비동기 eviction 워커
        let cache_for_evict = cache.clone();
        tokio::spawn(async move {
            while evict_rx.recv().await.is_some() {
                // 단순화: 신호 받으면 실행. (debouncing 추가 가능)
                if let Err(e) = cache_for_evict.enforce_size_limit().await {
                    debug!(target: "cache", error=?e, "eviction failed");
                }
            }
        });

        Ok(cache)
    }

    /// 전체 캐시 비우기 (디렉토리 내 .bin / .meta / .vary 파일 삭제)
    pub async fn clear_all(&self) -> Result<()> {
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let mut rd = tfs::read_dir(&dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let ty = entry.file_type().await?;
                if ty.is_dir() {
                    stack.push(entry.path());
                } else {
                    // 확장자 제한 없이 모두 제거 (캐시 용도로만 사용되는 디렉토리라 가정)
                    let _ = tfs::remove_file(entry.path()).await;
                }
            }
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

    pub async fn get_file(&self, key: &str) -> Result<Option<CacheFileEntry>> {
        let path = self.key_path(key);
        let meta_path = path.with_extension("meta");
        let data_path = path.with_extension("bin");

        let (data_meta, meta_bytes) =
            tokio::join!(tfs::metadata(&data_path), tfs::read(&meta_path));
        let (data_meta, meta_bytes) = match (data_meta, meta_bytes) {
            (Ok(data_meta), Ok(meta_bytes)) if data_meta.is_file() => (data_meta, meta_bytes),
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

        if data_meta.len() != meta.size {
            let _ = tfs::remove_file(&data_path).await;
            let _ = tfs::remove_file(&meta_path).await;
            debug!(target: "cache", key=%key, expected=meta.size, actual=data_meta.len(), "removed size-mismatched entry");
            return Ok(None);
        }

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let is_fresh = if now > meta.expires_at {
            if let Some(swr_end) = meta.swr_expires_at
                && now <= swr_end
            {
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
        }))
    }

    pub async fn put_file(
        &self,
        key: &str,
        temp_path: &Path,
        size: u64,
        options: CacheStoreOptions,
    ) -> Result<()> {
        let _g = self.inner.lock().await;
        let path = self.key_path(key);
        let meta_path = path.with_extension("meta");
        let data_path = path.with_extension("bin");
        if let Some(parent) = data_path.parent() {
            tfs::create_dir_all(parent).await?;
        }

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let ttl_dur = options.ttl.unwrap_or(self.default_ttl);
        let meta = Meta {
            expires_at: now + ttl_dur.as_secs(),
            created_at: now,
            size,
            content_type: options.content_type,
            swr_expires_at: options.swr.map(|d| now + ttl_dur.as_secs() + d.as_secs()),
            last_access_at: now,
            etag: options.etag,
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

        let _ = self.evict_tx.try_send(());

        Ok(())
    }

    async fn enforce_size_limit(&self) -> Result<()> {
        // _g lock removed here because we want this to be background and not block put?
        // But if multiple evictions run, it might be weird.
        // Also self.inner is Mutex<()>. If we lock it here, we block 'put' if 'put' locks it.
        // Wait, 'put' locks it to write file. Writing file is fast.
        // enforce_size_limit walks dir, which is slow.
        // If we lock here, we block 'put' while walking dir. That defeats the purpose.
        // So we should NOT lock self.inner for the whole duration.
        // Maybe we don't need to lock at all for reading?
        // Deletion might race with new writes?
        // If we delete a file that is being written... temporary files?
        // 'put' writes to temp then rename? No, it writes directly.
        // Ideally 'put' writes to temp.
        // Current impl writes directly.
        // If eviction deletes a file just as it's written?
        // 'put' overwrites.
        // Eviction finds files.
        // If we list files, then delete oldest.
        // It's acceptable race for cache.

        let mut entries: Vec<(PathBuf, u64, u64, u64)> = Vec::new(); // (base_path, created_at, size, last_access_at)
        let mut total = 0u64;
        let mut stack = vec![self.root.clone()];
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        while let Some(dir) = stack.pop() {
            let mut rd = tfs::read_dir(&dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let ty = entry.file_type().await?;
                if ty.is_dir() {
                    stack.push(entry.path());
                    continue;
                }
                if entry.path().extension().and_then(|s| s.to_str()) == Some("meta") {
                    let meta_bytes = tfs::read(entry.path()).await.ok();
                    if let Some(mb) = meta_bytes
                        && let Ok(meta) = serde_json::from_slice::<Meta>(&mb)
                    {
                        let base = entry.path().with_extension("");
                        let bin = base.with_extension("bin");
                        if tfs::metadata(&bin).await.is_ok() {
                            // 만료( + swr 종료) 된 항목은 즉시 삭제 후 스킵
                            let fully_expired = if now > meta.expires_at {
                                if let Some(swr_end) = meta.swr_expires_at {
                                    now > swr_end
                                } else {
                                    true
                                }
                            } else {
                                false
                            };
                            if fully_expired {
                                let _ = tfs::remove_file(bin).await;
                                let _ = tfs::remove_file(entry.path()).await;
                                continue;
                            }
                            total += meta.size;
                            entries.push((base, meta.created_at, meta.size, meta.last_access_at));
                        }
                    }
                }
            }
        }
        if total <= self.max_size {
            return Ok(());
        }
        debug!(target: "cache", current_bytes=total, max_bytes=self.max_size, entries=entries.len(), "starting eviction");
        // 정책별 정렬
        match self.policy {
            crate::config::EvictionPolicy::Fifo => {
                entries.sort_by_key(|(_, created, _, _)| *created);
            }
            crate::config::EvictionPolicy::Lru => {
                entries.sort_by_key(|(_, _created, _size, last_access)| *last_access);
            }
            crate::config::EvictionPolicy::Size => {
                entries.sort_by_key(|b| std::cmp::Reverse(b.2)); // 큰 것 먼저 제거
            }
            crate::config::EvictionPolicy::LruSize => {
                // last_access 오래된 것 우선, 동일 last_access 내에서는 큰 size 우선 제거
                entries.sort_by(|a, b| {
                    let la = a.3.cmp(&b.3); // last_access_at asc
                    if la == std::cmp::Ordering::Equal {
                        b.2.cmp(&a.2)
                    } else {
                        la
                    }
                });
            }
        }
        for (base, _created, size, _last_access) in entries {
            if total <= self.max_size {
                break;
            }
            let _ = tfs::remove_file(base.with_extension("bin")).await;
            let _ = tfs::remove_file(base.with_extension("meta")).await;
            // 관련 vary index 는 유지 (다른 variant 가 존재할 수 있음)
            if total >= size {
                total -= size;
            } else {
                total = 0;
            }
        }
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
