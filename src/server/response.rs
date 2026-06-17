use crate::cache::CacheFileEntry;
use bytes::{Bytes, BytesMut};
use http::{HeaderMap, Response, StatusCode, header};
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use std::{io::SeekFrom, path::PathBuf};
use tokio::{
    fs as tfs,
    io::{AsyncReadExt, AsyncSeekExt},
};
use tracing::warn;

use super::headers::{CacheStatus, add_cache_headers, copy_entry_headers};

pub type BoxedBody = BoxBody<Bytes, hyper::Error>;

pub fn full<T: Into<Bytes>>(chunk: T) -> BoxedBody {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

pub fn empty() -> BoxedBody {
    http_body_util::Empty::new()
        .map_err(|never| match never {})
        .boxed()
}

pub fn simple(code: StatusCode, msg: &str) -> Response<BoxedBody> {
    let mut r = Response::new(full(msg.to_string()));
    *r.status_mut() = code;
    r.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    r
}

pub fn not_modified_response(
    entry: &CacheFileEntry,
    cache_status: CacheStatus,
) -> Response<BoxedBody> {
    let mut resp = Response::new(empty());
    *resp.status_mut() = StatusCode::NOT_MODIFIED;
    copy_entry_headers(resp.headers_mut(), entry);
    add_cache_headers(resp.headers_mut(), cache_status, entry.created_at);
    resp
}

pub fn parse_range_header(headers: &HeaderMap) -> Option<(u64, Option<u64>)> {
    let range_val = headers.get(header::RANGE)?.to_str().ok()?;
    let rest = range_val.strip_prefix("bytes=")?;
    if rest.contains(',') {
        return None;
    }
    let (s, e) = rest.split_once('-')?;
    if !s.is_empty() {
        let start = s.parse::<u64>().ok()?;
        if e.is_empty() {
            return Some((start, None));
        }
        let end = e.parse::<u64>().ok()?;
        (end >= start).then_some((start, Some(end)))
    } else {
        let suffix = e.parse::<u64>().ok()?;
        Some((u64::MAX, Some(suffix)))
    }
}

pub fn build_cached_response(
    entry: &CacheFileEntry,
    range_request: Option<(u64, Option<u64>)>,
    cache_status: CacheStatus,
    is_head: bool,
) -> Response<BoxedBody> {
    let range = resolve_range(entry.size, range_request);
    let extra_headers = range_extra_headers(&range);
    let response_body = if is_head || range.len == 0 {
        empty()
    } else {
        stream_file(entry.path.clone(), range.start, range.len)
    };

    let mut resp = Response::new(response_body);
    *resp.status_mut() = range.status;
    if let Ok(hv) = header::HeaderValue::from_str(&range.len.to_string()) {
        resp.headers_mut().insert(header::CONTENT_LENGTH, hv);
    }
    copy_entry_headers(resp.headers_mut(), entry);
    add_cache_headers(resp.headers_mut(), cache_status, entry.created_at);
    for (k, v) in extra_headers {
        resp.headers_mut().insert(k, v);
    }
    resp
}

struct ResolvedRange {
    start: u64,
    len: u64,
    status: StatusCode,
    content_range: Option<String>,
}

fn resolve_range(total_len: u64, range: Option<(u64, Option<u64>)>) -> ResolvedRange {
    let Some((start, end_opt)) = range else {
        return ResolvedRange {
            start: 0,
            len: total_len,
            status: StatusCode::OK,
            content_range: None,
        };
    };
    if total_len == 0 {
        return ResolvedRange {
            start: 0,
            len: 0,
            status: StatusCode::RANGE_NOT_SATISFIABLE,
            content_range: Some(format!("bytes */{total_len}")),
        };
    }
    let (range_start, range_end) = if start == u64::MAX {
        let suffix = end_opt.unwrap_or(0).min(total_len);
        (total_len.saturating_sub(suffix), total_len - 1)
    } else {
        let end = end_opt
            .unwrap_or_else(|| total_len.saturating_sub(1))
            .min(total_len.saturating_sub(1));
        (start.min(total_len), end)
    };
    if range_start <= range_end && range_start < total_len {
        ResolvedRange {
            start: range_start,
            len: range_end - range_start + 1,
            status: StatusCode::PARTIAL_CONTENT,
            content_range: Some(format!("bytes {range_start}-{range_end}/{total_len}")),
        }
    } else {
        ResolvedRange {
            start: 0,
            len: 0,
            status: StatusCode::RANGE_NOT_SATISFIABLE,
            content_range: Some(format!("bytes */{total_len}")),
        }
    }
}

fn range_extra_headers(range: &ResolvedRange) -> Vec<(header::HeaderName, header::HeaderValue)> {
    let mut extra = Vec::with_capacity(2);
    extra.push((
        header::ACCEPT_RANGES,
        header::HeaderValue::from_static("bytes"),
    ));
    if let Some(cr) = &range.content_range
        && let Ok(hv) = header::HeaderValue::from_str(cr)
    {
        extra.push((header::CONTENT_RANGE, hv));
    }
    extra
}

fn stream_file(path: PathBuf, start: u64, len: u64) -> BoxedBody {
    const BUF_SIZE: usize = 128 * 1024;
    let (tx, body_stream) =
        tokio::sync::mpsc::channel::<Result<hyper::body::Frame<Bytes>, hyper::Error>>(64);
    tokio::spawn(async move {
        let result = async {
            let mut file = tfs::File::open(&path).await?;
            file.seek(SeekFrom::Start(start)).await?;
            let mut reader = file.take(len);
            let mut buf = BytesMut::with_capacity(BUF_SIZE);
            loop {
                buf.reserve(BUF_SIZE);
                let n = reader.read_buf(&mut buf).await?;
                if n == 0 {
                    break;
                }
                let chunk = buf.split().freeze();
                if tx
                    .send(Ok(hyper::body::Frame::data(chunk)))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Ok::<(), std::io::Error>(())
        }
        .await;
        if let Err(e) = result {
            warn!(error=?e, path=?path, "cached file stream failed");
        }
    });
    http_body_util::StreamBody::new(tokio_stream::wrappers::ReceiverStream::new(body_stream))
        .boxed()
}
