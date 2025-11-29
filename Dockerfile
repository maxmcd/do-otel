# syntax=docker/dockerfile:1
FROM rust:1.91-trixie AS build

WORKDIR /app

COPY container_src/Cargo.lock container_src/Cargo.toml ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release

COPY container_src/src src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --locked --release

FROM debian:trixie
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /app/target/release/sqlotel /sqlotel
EXPOSE 8080

# Run
CMD ["/sqlotel"]
