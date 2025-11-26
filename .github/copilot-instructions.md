# YU-RI Copilot Instructions

## 프로젝트 개요

**YU-RI**는 리눅스 미러용 고성능 캐싱 프록시 서버입니다. Rust로 작성되었으며, async/await 기반 `tokio` 런타임과 `hyper` HTTP 서버를 사용합니다.

## 아키텍처

```
요청 → server.rs (handle 함수) → cache.rs (DiskCache) → 업스트림
                               ↓
                          http_cache.rs (TTL 계산)
```

### 핵심 모듈

| 파일 | 역할 |
|------|------|
| `src/main.rs` | 진입점, 환경 설정 로드 및 서버 시작 |
| `src/config.rs` | 환경 변수 기반 설정 (`Config`, `EvictionPolicy`) |
| `src/server.rs` | HTTP 요청 처리, 캐시 조회/저장, Range 요청, SWR 백그라운드 재검증 |
| `src/cache.rs` | 디스크 기반 캐시 (`DiskCache`), blake3 해시 키, 퇴출 정책 |
| `src/http_cache.rs` | `Cache-Control`/`Expires` 헤더 파싱, TTL 결정 |

### 주요 패턴

- **캐시 키**: URL을 `blake3` 해시로 변환, `cache/{hash[0:2]}/{hash[2:4]}.{bin,meta,vary}` 구조
- **Vary 헤더 지원**: `base_key||Header1=val&Header2=val` 형식의 variant 키
- **Stale-While-Revalidate**: 만료된 캐시 즉시 반환 + 백그라운드 갱신 (`background_refresh`)
- **비동기 LRU 터치**: `mpsc` 채널을 통한 `last_access_at` 업데이트 워커

## 빌드 & 실행

```bash
# 개발 빌드
cargo build

# 릴리스 빌드 (최적화: LTO, single codegen-unit)
cargo build --release

# 실행 전 환경 설정 (.env 또는 시스템 환경 변수)
cp .env.example .env
# UPSTREAM_BASE 필수 설정

# 실행
cargo run

# Docker 빌드 (musl 정적 바이너리)
docker build -t yu-ri .
```

## 환경 변수 (필수/선택)

| 변수 | 필수 | 기본값 | 설명 |
|------|------|--------|------|
| `UPSTREAM_BASE` | ✅ | - | 원본 서버 URL |
| `LISTEN_ADDR` | ❌ | `127.0.0.1:8080` | 바인딩 주소 |
| `CACHE_DIR` | ❌ | `cache` | 캐시 디렉토리 |
| `MAX_CACHE_SIZE_BYTES` | ❌ | 5GB | 최대 캐시 크기 |
| `DEFAULT_TTL_SECS` | ❌ | 300 | 기본 TTL(초) |
| `EVICTION_POLICY` | ❌ | `LRU` | 퇴출 정책: `LRU`, `FIFO`, `SIZE`, `LRU_SIZE` |
| `RUST_LOG` | ❌ | - | 로그 레벨 (`debug`, `info`, `warn`) |

## 코드 작성 규칙

- **에러 처리**: `anyhow::Result` 사용, `?` 연산자로 전파
- **로깅**: `tracing` 매크로 (`info!`, `warn!`, `debug!`) 사용, `target: "모듈명"` 패턴
- **비동기**: 모든 I/O는 `tokio::fs`, `tokio::sync` 사용
- **바이트 처리**: `bytes::Bytes`, `BytesMut` 사용 (zero-copy 슬라이싱)

## 테스트

```bash
# 유닛 테스트 실행
cargo test

# http_cache.rs에 TTL 파싱 테스트 존재
```

## 주의사항

- `edition = "2024"` 사용 (Rust 2024 에디션)
- GET 요청만 지원, 다른 메서드는 405 반환
- Range 요청: 단일 범위만 지원, 멀티 범위는 패스스루
- User-Agent 헤더: 업스트림 요청 시 `RFMP/1.0` 고정
