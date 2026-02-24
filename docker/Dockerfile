ARG PG_MAJOR=17

# ==============================================================================
# Build
# ==============================================================================
FROM rust:1-bookworm AS builder

ARG PG_MAJOR=17

RUN apt-get update && apt-get install -y --no-install-recommends \
        gnupg2 curl ca-certificates lsb-release \
    && echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        | gpg --dearmor -o /etc/apt/trusted.gpg.d/pgdg.gpg \
    && apt-get update && apt-get install -y --no-install-recommends \
        postgresql-${PG_MAJOR} \
        postgresql-server-dev-${PG_MAJOR} \
        pkg-config libssl-dev libclang-dev clang \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/usr/lib/postgresql/${PG_MAJOR}/bin:${PATH}"

RUN cargo install cargo-pgrx --version "0.17.0" --locked
RUN cargo pgrx init --pg${PG_MAJOR}=/usr/lib/postgresql/${PG_MAJOR}/bin/pg_config

WORKDIR /build/pg_cocoon

# Cache dependency builds: copy only manifests first
COPY Cargo.toml Cargo.lock* ./
COPY src/bin/pgrx_embed.rs src/bin/pgrx_embed.rs
RUN mkdir -p src && echo '#[allow(unused)] fn main() {}' > src/lib.rs \
    && cargo fetch

# Copy full source and build
COPY . .
RUN cargo pgrx package --pg-config /usr/lib/postgresql/${PG_MAJOR}/bin/pg_config \
        --features pg${PG_MAJOR} --no-default-features

# ==============================================================================
# Runtime
# ==============================================================================
FROM postgres:${PG_MAJOR}-bookworm

ARG PG_MAJOR=17

COPY --from=builder /build/pg_cocoon/target/release/pg_cocoon-pg${PG_MAJOR}/ /
