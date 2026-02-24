#!/bin/sh
set -e

# Fix ownership of the target/ volume (Docker creates named volumes as root)
chown builder:builder /build/pg_cocoon/target 2>/dev/null || true

exec gosu builder "$@"
