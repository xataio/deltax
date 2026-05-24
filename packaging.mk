# packaging.mk — build .deb packages for pg_deltax
#
# Builds a CNPG-compatible (Debian Bookworm, glibc) .deb containing the
# pgrx-packaged extension files. Runs inside a `rust:1-bookworm` Docker
# container so the host only needs Docker.
#
# Usage:
#   make -f packaging.mk build ARCH=amd64 PGVER=17 TAG=v0.1.0
#   make -f packaging.mk build ARCH=arm64 PGVER=18 TAG=v0.1.0
#   make -f packaging.mk verify DEB=pkgout/pg-deltax-pg17_0.1.0_amd64.deb
#
# Output naming: pg-deltax-pg<PGVER>_<version>_<arch>.deb

EXT_NAME       ?= pg_deltax
PKG_NAME_BASE  ?= pg-deltax
PGVER          ?= 17
TAG            ?= v0.0.1-test
PKGOUT         ?= pkgout
PGRX_VERSION   ?= 0.17.0
BUILDER_IMAGE  ?= rust:1-bookworm
CNPG_BASE      ?= ghcr.io/cloudnative-pg/postgresql:$(PGVER)-minimal-bookworm

.PHONY: build verify clean

build:
	@set -e; \
	: $${ARCH:?ARCH required (amd64|arm64)}; \
	mkdir -p "$(PKGOUT)"; \
	echo "==> Build $(EXT_NAME) PG $(PGVER) on $$ARCH (tag $(TAG))"; \
	docker run --rm --platform=linux/$$ARCH -u 0:0 \
	  -e ARCH="$$ARCH" \
	  -e PGVER="$(PGVER)" -e TAG="$(TAG)" -e EXT_NAME="$(EXT_NAME)" \
	  -e PKG_NAME_BASE="$(PKG_NAME_BASE)" \
	  -e PGRX_VERSION="$(PGRX_VERSION)" -e PKGOUT="$(PKGOUT)" \
	  -e CARGO_HOME=/tmp/cargo -e CARGO_TARGET_DIR=/tmp/target \
	  -v "$$PWD:/work" -w /work \
	  "$(BUILDER_IMAGE)" \
	  bash -euo pipefail -c '\
	    apt-get update; \
	    apt-get install -y --no-install-recommends \
	      gnupg2 curl ca-certificates lsb-release \
	      build-essential pkg-config libssl-dev libclang-dev clang \
	      dpkg-dev file binutils; \
	    echo "deb http://apt.postgresql.org/pub/repos/apt $$(lsb_release -cs)-pgdg main" \
	      >/etc/apt/sources.list.d/pgdg.list; \
	    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
	      | gpg --dearmor -o /etc/apt/trusted.gpg.d/pgdg.gpg; \
	    apt-get update; \
	    apt-get install -y --no-install-recommends \
	      postgresql-server-dev-$$PGVER; \
	    \
	    cargo_version="$$(grep -E "^version = " Cargo.toml | head -1 | sed -E "s/version = \"(.*)\"/\\1/")"; \
	    deb_version="$${TAG#v}"; \
	    if [ "$$cargo_version" != "$$deb_version" ]; then \
	      echo "ERROR: Cargo.toml version ($$cargo_version) does not match tag ($$deb_version)"; \
	      exit 1; \
	    fi; \
	    \
	    cargo install cargo-pgrx --version "$$PGRX_VERSION" --locked; \
	    PG_CONFIG=/usr/lib/postgresql/$$PGVER/bin/pg_config; \
	    cargo pgrx init "--pg$$PGVER=$$PG_CONFIG"; \
	    cargo pgrx package --pg-config "$$PG_CONFIG" \
	      --features "pg$$PGVER" --no-default-features; \
	    \
	    PKG_FULL_NAME="$${PKG_NAME_BASE}-pg$${PGVER}_$${deb_version}_$${ARCH}"; \
	    STAGE="$$PKGOUT/$$PKG_FULL_NAME"; \
	    rm -rf "$$STAGE"; \
	    mkdir -p "$$STAGE/DEBIAN"; \
	    cp -r /tmp/target/release/$${EXT_NAME}-pg$${PGVER}/. "$$STAGE/"; \
	    \
	    printf "Package: %s-pg%s\nVersion: %s\nArchitecture: %s\nSection: database\nPriority: optional\nMaintainer: Xata <info@xata.io>\nHomepage: https://github.com/xataio/pg_deltax\nDescription: PostgreSQL time-series extension with Gorilla compression (PG %s)\n Adds time-series partitioning, columnar storage, and Gorilla\n compression to PostgreSQL %s. Requires shared_preload_libraries=pg_deltax.\n" \
	      "$$PKG_NAME_BASE" "$$PGVER" "$$deb_version" "$$ARCH" "$$PGVER" "$$PGVER" \
	      > "$$STAGE/DEBIAN/control"; \
	    \
	    dpkg-deb --build --root-owner-group "$$STAGE"; \
	    \
	    echo "== Built deb =="; \
	    dpkg-deb -I "$$STAGE.deb" | grep -E "Package|Version|Architecture"; \
	    echo "== .so info =="; \
	    file "$$STAGE/usr/lib/postgresql/$$PGVER/lib/$${EXT_NAME}.so"; \
	    readelf -h "$$STAGE/usr/lib/postgresql/$$PGVER/lib/$${EXT_NAME}.so" | grep Machine; \
	  '

verify:
	@set -e; \
	: $${DEB:?DEB= path required}; \
	case "$(DEB)" in *_arm64.deb) P=arm64 ;; *_amd64.deb) P=amd64 ;; *) echo "Cannot infer arch from $(DEB)"; exit 1;; esac; \
	echo "Using: $(DEB) (arch: $$P, PG: $(PGVER))"; \
	docker run --rm --platform=linux/$$P -u 0:0 \
	  -v "$$PWD:/payload" "$(CNPG_BASE)" \
	  bash -euo pipefail -c '\
	    export DEBIAN_FRONTEND=noninteractive; \
	    apt-get update; \
	    apt-get install -y --no-install-recommends "/payload/$(DEB)"; \
	    pg_createcluster $(PGVER) main; \
	    pg_ctlcluster $(PGVER) main start -o "-c shared_preload_libraries=$(EXT_NAME)"; \
	    su postgres -c "psql -c \"CREATE EXTENSION $(EXT_NAME);\""; \
	    su postgres -c "psql -c \"SELECT extname, extversion FROM pg_extension WHERE extname = '\''$(EXT_NAME)'\'';\""; \
	    pg_ctlcluster $(PGVER) main stop; \
	  '

clean:
	rm -rf "$(PKGOUT)"
