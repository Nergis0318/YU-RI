FROM rust:latest AS builder

ARG DEBIAN_FRONTEND=noninteractive

RUN apt update -y && apt upgrade -y && apt install -y musl-tools && rustup target add x86_64-unknown-linux-musl

WORKDIR /app

COPY . .

RUN cargo build --release --target x86_64-unknown-linux-musl

FROM scratch

COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/yu-ri /yu-ri

CMD ["/yu-ri"]
