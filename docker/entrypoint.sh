#!/bin/sh
set -e

# Fix ownership of volumes (Docker creates named volumes as root)
chown builder:builder /build/pg_seaturtle/target 2>/dev/null || true
chown builder:builder /usr/local/cargo/registry 2>/dev/null || true

exec gosu builder "$@"
