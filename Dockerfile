FROM rust:latest AS builder

ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

RUN apt update -y && apt upgrade -y && apt install -y musl-tools

RUN case "${TARGETARCH}" in \
        amd64) RUST_TARGET=x86_64-unknown-linux-musl ;; \
        arm64) RUST_TARGET=aarch64-unknown-linux-musl ;; \
        *) echo "Unsupported TARGETARCH: ${TARGETARCH}" >&2 && exit 1 ;; \
    esac && \
    rustup target add "${RUST_TARGET}" && \
    echo "${RUST_TARGET}" > /rust-target

WORKDIR /app

COPY . .

RUN RUST_TARGET=$(cat /rust-target) && \
    cargo build --release --target "${RUST_TARGET}" && \
    cp "target/${RUST_TARGET}/release/yu-ri" /yu-ri

FROM scratch

COPY --from=builder /yu-ri /yu-ri

CMD ["/yu-ri"]
