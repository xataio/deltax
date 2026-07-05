PG_MAJOR ?= 17
DEV_IMAGE  = pg_deltax-dev:pg$(PG_MAJOR)
IMAGE      = pg_deltax:pg$(PG_MAJOR)
WORKTREE_ID = $(shell printf '%s' '$(CURDIR)' | shasum | cut -c1-12)
CORRECTNESS_IMAGE = pg_deltax-correctness:pg$(PG_MAJOR)-$(WORKTREE_ID)
COV_IMAGE  = pg_deltax-cov:pg$(PG_MAJOR)
TARGET_VOL      = pg_deltax_target_pg$(PG_MAJOR)
CARGO_VOL       = pg_deltax_cargo
QUERY_CONTAINER = pg_deltax_query

PG_VERSIONS ?= 17 18
VENV         = .venv

.PHONY: dev-image image image-fresh correctness-image test build clippy fmt fmt-check coverage coverage-all run psql cargo clean \
       integration-test \
       correctness-smoke correctness correctness-fuzz correctness-clean \
       bench-clickbench bench-clickbench-keep bench-clickbench-full bench-clean \
       bench-rtabench bench-rtabench-bless bench-rtabench-keep bench-rtabench-full \
       bench-rtabench-clean bench-rtabench-distclean \
       bench-timescaledb bench-compare bench-all \
       run-sql run-sql-file logs logs-all logs-follow release

# Build the dev toolchain image (rebuilds only when Dockerfile.dev changes)
dev-image:
	docker build -f docker/Dockerfile.dev --build-arg PG_MAJOR=$(PG_MAJOR) -t $(DEV_IMAGE) docker/

# Generic: run any cargo command. Usage: make cargo CMD="check"
cargo: dev-image
	docker run --rm -v $(CURDIR):/build/pg_deltax -v $(TARGET_VOL):/build/pg_deltax/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) cargo $(CMD)

test: dev-image
	docker run --rm -v $(CURDIR):/build/pg_deltax -v $(TARGET_VOL):/build/pg_deltax/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) sh -c "cargo pgrx test pg$(PG_MAJOR)"

build: dev-image
	docker run --rm -v $(CURDIR):/build/pg_deltax -v $(TARGET_VOL):/build/pg_deltax/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) cargo build --features pg$(PG_MAJOR) --no-default-features

clippy: dev-image
	docker run --rm -v $(CURDIR):/build/pg_deltax -v $(TARGET_VOL):/build/pg_deltax/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) cargo clippy --features pg$(PG_MAJOR) --no-default-features --tests

# Format code with rustfmt. Pass FILE=... to scope to a single file (per-session cleanup style).
#   make fmt                       # format the whole tree
#   make fmt FILE=src/foo.rs       # format just that file
#
# `cargo fmt` is whole-workspace and ignores positional file args; and
# `rustfmt <file>` recurses into `mod ...;` children by default. So when
# FILE= is set we invoke `rustfmt` directly with `skip_children=true` so the
# scope really is just that one file. Edition matches Cargo.toml.
fmt: dev-image
	docker run --rm -v $(CURDIR):/build/pg_deltax -v $(TARGET_VOL):/build/pg_deltax/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) \
		$(if $(FILE),rustfmt --edition 2024 --config skip_children=true $(FILE),cargo fmt)

# Verify formatting without modifying files. Whole-tree only.
fmt-check: dev-image
	docker run --rm -v $(CURDIR):/build/pg_deltax -v $(TARGET_VOL):/build/pg_deltax/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) cargo fmt -- --check

coverage: dev-image
	docker run --rm -v $(CURDIR):/build/pg_deltax -v $(TARGET_VOL):/build/pg_deltax/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) bash -c '\
		eval "$$(cargo llvm-cov show-env --export-prefix)" && \
		cargo llvm-cov clean --workspace && \
		cargo pgrx test pg$(PG_MAJOR) && \
		cargo llvm-cov report --html --output-dir /build/pg_deltax/coverage && \
		cargo llvm-cov report && \
		echo "" && \
		echo "HTML report: coverage/html/index.html"'

# Combined unit + integration test coverage report.
# 1. Instrumented build + unit tests in dev container (profraw → target/)
# 2. Package the same instrumented .so into a runtime image
# 3. Run integration tests with profraw mounted to host
# 4. Copy integration profraw back and generate merged report
#
# The key is that unit tests and the runtime image share the same instrumented
# binary, so all profraw files are compatible.
coverage-all: dev-image $(VENV)/.stamp
	@rm -rf $(CURDIR)/coverage/profraw $(CURDIR)/coverage/pkg
	@mkdir -p $(CURDIR)/coverage/profraw $(CURDIR)/coverage/pkg
	# Step 1: instrumented build + unit tests + install extension
	# Uses cargo pgrx install (dev profile) so the .so matches the test profraw.
	docker run --rm -v $(CURDIR):/build/pg_deltax -v $(TARGET_VOL):/build/pg_deltax/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) bash -c '\
		eval "$$(cargo llvm-cov show-env --export-prefix)" && \
		cargo llvm-cov clean --workspace && \
		cargo pgrx test pg$(PG_MAJOR) && \
		cargo pgrx install --pg-config /usr/lib/postgresql/$(PG_MAJOR)/bin/pg_config \
			--features pg$(PG_MAJOR) --no-default-features && \
		mkdir -p /build/pg_deltax/coverage/pkg/usr/lib/postgresql/$(PG_MAJOR)/lib \
			/build/pg_deltax/coverage/pkg/usr/share/postgresql/$(PG_MAJOR)/extension && \
		cp /usr/lib/postgresql/$(PG_MAJOR)/lib/pg_deltax.so \
			/build/pg_deltax/coverage/pkg/usr/lib/postgresql/$(PG_MAJOR)/lib/ && \
		cp /usr/share/postgresql/$(PG_MAJOR)/extension/pg_deltax* \
			/build/pg_deltax/coverage/pkg/usr/share/postgresql/$(PG_MAJOR)/extension/'
	# Step 2: build runtime image with the same instrumented .so
	docker build -f docker/Dockerfile.coverage --build-arg PG_MAJOR=$(PG_MAJOR) -t $(COV_IMAGE) .
	# Step 3: run integration tests against instrumented image
	-docker rm -f pg_deltax_cov 2>/dev/null
	docker run -d --name pg_deltax_cov \
		-p 15433:5432 \
		-e POSTGRES_PASSWORD=postgres \
		-v $(CURDIR)/coverage/profraw:/coverage \
		$(COV_IMAGE) \
		-c shared_preload_libraries=pg_deltax
	@echo "Waiting for coverage PG..."
	@for i in $$(seq 1 30); do \
		docker exec pg_deltax_cov pg_isready -U postgres -q 2>/dev/null && break; \
		sleep 1; \
	done
	PG_DELTAX_IMAGE=__skip__ PG_DELTAX_COV_CONTAINER=pg_deltax_cov PG_DELTAX_PORT=15433 \
		$(VENV)/bin/pytest tests/ -v --ignore=tests/bench_clickbench.py --ignore=tests/bench_clickbench_timescaledb.py || true
	# Graceful shutdown to flush profraw
	docker stop -t 10 pg_deltax_cov
	docker rm pg_deltax_cov
	# Step 4: merge integration profraw and generate combined report
	docker run --rm -v $(CURDIR):/build/pg_deltax -v $(TARGET_VOL):/build/pg_deltax/target \
		-v $(CARGO_VOL):/usr/local/cargo/registry $(DEV_IMAGE) bash -c '\
		eval "$$(cargo llvm-cov show-env --export-prefix)" && \
		PROFDIR=$$(dirname "$$LLVM_PROFILE_FILE") && \
		cp /build/pg_deltax/coverage/profraw/*.profraw "$$PROFDIR"/ 2>/dev/null; \
		cargo llvm-cov report --html --output-dir /build/pg_deltax/coverage && \
		cargo llvm-cov report && \
		echo "" && \
		echo "HTML report: coverage/html/index.html"'

# Build the runtime image (production-like, no build tools)
image: dev-image
	docker build -f docker/Dockerfile --build-arg PG_MAJOR=$(PG_MAJOR) -t $(IMAGE) .

# Force-rebuild the runtime image (no Docker cache)
image-fresh: dev-image
	docker build --no-cache -f docker/Dockerfile --build-arg PG_MAJOR=$(PG_MAJOR) -t $(IMAGE) .

# Build a worktree-scoped runtime image for correctness tests. This avoids
# cross-worktree races with integration tests rebuilding the shared $(IMAGE) tag.
correctness-image: dev-image
	docker build -f docker/Dockerfile --build-arg PG_MAJOR=$(PG_MAJOR) -t $(CORRECTNESS_IMAGE) .

# Run postgres with the extension for manual testing
run: image
	docker run --rm --name pg_deltax -p 5432:5432 -e POSTGRES_PASSWORD=postgres $(IMAGE) \
		-c shared_preload_libraries=pg_deltax \
		-c log_min_messages=log

psql:
	docker exec -it pg_deltax psql -U postgres

# Build, start PG, run SQL, show logs, stop. Usage: make run-sql SQL="SELECT 1"
run-sql: image
	@docker rm -f $(QUERY_CONTAINER) 2>/dev/null || true
	@docker run -d --name $(QUERY_CONTAINER) \
		-e POSTGRES_PASSWORD=postgres \
		$(IMAGE) \
		-c shared_preload_libraries=pg_deltax \
		-c log_min_messages=log >/dev/null
	@echo "Waiting for PostgreSQL..."
	@for i in $$(seq 1 30); do \
		docker exec $(QUERY_CONTAINER) pg_isready -U postgres -q 2>/dev/null && break; \
		sleep 0.3; \
	done
	@echo "--- Query Output ---"
	@docker exec $(QUERY_CONTAINER) psql -U postgres -c "$(SQL)"
	@echo ""
	@echo "--- Server Logs (LOG level) ---"
	@docker logs $(QUERY_CONTAINER) 2>&1 | grep -E 'pg_deltax|LOG:' || true
	@docker rm -f $(QUERY_CONTAINER) >/dev/null

# Same as run-sql but reads SQL from a file. Usage: make run-sql-file FILE="test.sql"
run-sql-file: image
	@docker rm -f $(QUERY_CONTAINER) 2>/dev/null || true
	@docker run -d --name $(QUERY_CONTAINER) \
		-e POSTGRES_PASSWORD=postgres \
		-v $(CURDIR)/$(FILE):/tmp/query.sql:ro \
		$(IMAGE) \
		-c shared_preload_libraries=pg_deltax \
		-c log_min_messages=log >/dev/null
	@echo "Waiting for PostgreSQL..."
	@for i in $$(seq 1 30); do \
		docker exec $(QUERY_CONTAINER) pg_isready -U postgres -q 2>/dev/null && break; \
		sleep 0.3; \
	done
	@echo "--- Query Output ---"
	@docker exec $(QUERY_CONTAINER) psql -U postgres -f /tmp/query.sql
	@echo ""
	@echo "--- Server Logs (LOG level) ---"
	@docker logs $(QUERY_CONTAINER) 2>&1 | grep -E 'pg_deltax|LOG:' || true
	@docker rm -f $(QUERY_CONTAINER) >/dev/null

# Show pg_deltax log lines from the running container
logs:
	@docker logs pg_deltax 2>&1 | grep -E 'pg_deltax' || true

logs-all:
	@docker logs pg_deltax 2>&1

logs-follow:
	@docker logs -f pg_deltax 2>&1

$(VENV)/.stamp: tests/requirements.txt
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r tests/requirements.txt
	@touch $@

integration-test: $(VENV)/.stamp
	@for v in $(PG_VERSIONS); do \
		echo "=== Integration tests: PG $$v ==="; \
		$(MAKE) image PG_MAJOR=$$v; \
		PG_DELTAX_IMAGE=pg_deltax:pg$$v $(VENV)/bin/pytest tests/ -v --ignore=tests/correctness; \
	done

correctness-smoke: $(VENV)/.stamp correctness-image
	PG_DELTAX_IMAGE=$(CORRECTNESS_IMAGE) $(VENV)/bin/pytest tests/correctness/ -m smoke -v -s

correctness: $(VENV)/.stamp correctness-image
	PG_DELTAX_IMAGE=$(CORRECTNESS_IMAGE) $(VENV)/bin/pytest tests/correctness/ -v -s

correctness-fuzz:
	@echo "Seeded generated correctness tests are not implemented yet."

correctness-clean:
	find tests/correctness -type d -name __pycache__ -prune -exec rm -rf {} +
	rm -rf tests/correctness/.pytest_cache tests/.correctness_failures

bench-clickbench: $(VENV)/.stamp image
	PG_DELTAX_IMAGE=pg_deltax:pg$(PG_MAJOR) $(VENV)/bin/pytest tests/bench_clickbench.py -v -s

bench-clickbench-full: $(VENV)/.stamp image
	PG_DELTAX_IMAGE=pg_deltax:pg$(PG_MAJOR) CLICKBENCH_FILES=100 $(VENV)/bin/pytest tests/bench_clickbench.py -v -s

# Same as bench-clickbench but leaves the container running for manual debugging
bench-clickbench-keep: $(VENV)/.stamp image
	PG_DELTAX_IMAGE=pg_deltax:pg$(PG_MAJOR) KEEP_CONTAINER=1 $(VENV)/bin/pytest tests/bench_clickbench.py -v -s

# Remove benchmark containers
bench-clean:
	docker volume rm pg_deltax_bench_pgdata 2>/dev/null || true

# RTABench local (Docker) — plain PG vs pg_deltax head-to-head on a
# sub-GB slice of the real dataset, with per-query correctness checks.
bench-rtabench: $(VENV)/.stamp image
	PG_DELTAX_IMAGE=pg_deltax:pg$(PG_MAJOR) $(VENV)/bin/pytest tests/bench_rtabench.py -v -s

# Record the current warm times as the pinned wall-clock baseline (L5). Run
# this deliberately after a known-good change so the regression gate (Phase F)
# checks against an intentional reference, not a drifting previous run.
bench-rtabench-bless: $(VENV)/.stamp image
	PG_DELTAX_IMAGE=pg_deltax:pg$(PG_MAJOR) RTABENCH_BLESS=1 $(VENV)/bin/pytest tests/bench_rtabench.py -v -s

bench-rtabench-full: $(VENV)/.stamp image
	PG_DELTAX_IMAGE=pg_deltax:pg$(PG_MAJOR) RTABENCH_ORDERS=10010342 $(VENV)/bin/pytest tests/bench_rtabench.py -v -s

# Reload the dataset (drops container + PG data volume, preserves the
# downloaded CSV cache under tests/.data/rtabench) and run the benchmark
# with KEEP_CONTAINER + BENCH_PERSIST so the container stays up after.
# Every invocation re-loads data through the current extension code, so
# compression changes are exercised end-to-end; the first run downloads
# the upstream CSVs (~7 GB, one-time) — subsequent runs reuse them.
bench-rtabench-keep: $(VENV)/.stamp image bench-rtabench-clean
	PG_DELTAX_IMAGE=pg_deltax:pg$(PG_MAJOR) KEEP_CONTAINER=1 BENCH_PERSIST=1 \
		$(VENV)/bin/pytest tests/bench_rtabench.py -v -s

# Wipe the container and persistent PG data volume. The downloaded CSV
# cache (tests/.data/rtabench) is preserved — use `bench-rtabench-distclean`
# to also drop the ~7 GB cache.
bench-rtabench-clean:
	-docker rm -f pg_deltax_inttest 2>/dev/null
	-docker volume rm pg_deltax_bench_pgdata 2>/dev/null

# Full wipe including the downloaded CSV cache (forces ~7 GB redownload).
bench-rtabench-distclean: bench-rtabench-clean
	rm -rf tests/.data/rtabench

bench-timescaledb: $(VENV)/.stamp
	TSDB_VARIANT=tsl $(VENV)/bin/pytest tests/bench_clickbench_timescaledb.py -v -s

bench-compare: $(VENV)/.stamp
	$(VENV)/bin/python tests/bench_compare.py

bench-all: bench-clickbench bench-timescaledb bench-compare

# Cut a release. Bumps the version in Cargo.toml + Cargo.lock, commits, tags
# v$(VERSION), and pushes the branch + tag. The Release workflow reacts to the
# v* tag and builds + publishes the .deb artifacts. Runs entirely on the host
# (git + awk), so no Docker or host cargo toolchain is required.
#   make release VERSION=0.2.1      # commits "Release v0.2.1", tags v0.2.1
release:
	@set -e; \
	: $${VERSION:?VERSION required without leading 'v', e.g. make release VERSION=0.2.1}; \
	case "$(VERSION)" in \
	  v*) echo "ERROR: drop the leading 'v' — use VERSION=$(VERSION:v%=%)"; exit 1 ;; \
	  [0-9]*) : ;; \
	  *) echo "ERROR: VERSION must start with a digit (got '$(VERSION)')"; exit 1 ;; \
	esac; \
	tag="v$(VERSION)"; \
	echo "==> Releasing $$tag"; \
	if [ -n "$$(git status --porcelain)" ]; then \
	  echo "ERROR: working tree is not clean — commit or stash first"; exit 1; \
	fi; \
	branch="$$(git rev-parse --abbrev-ref HEAD)"; \
	if ! git rev-parse --abbrev-ref --symbolic-full-name '@{u}' >/dev/null 2>&1; then \
	  echo "ERROR: branch '$$branch' has no upstream"; exit 1; \
	fi; \
	echo "--> fetching origin"; \
	git fetch --quiet origin; \
	if [ "$$(git rev-parse @)" != "$$(git rev-parse '@{u}')" ]; then \
	  echo "ERROR: '$$branch' is not in sync with its upstream — pull/push first"; exit 1; \
	fi; \
	if git rev-parse -q --verify "refs/tags/$$tag" >/dev/null; then \
	  echo "ERROR: tag $$tag already exists locally"; exit 1; \
	fi; \
	if git ls-remote --exit-code --tags origin "$$tag" >/dev/null 2>&1; then \
	  echo "ERROR: tag $$tag already exists on origin"; exit 1; \
	fi; \
	echo "--> bumping version to $(VERSION)"; \
	awk -v v="$(VERSION)" '!d && /^version = / {sub(/"[^"]*"/, "\"" v "\""); d=1} {print}' Cargo.toml > Cargo.toml.tmp && mv Cargo.toml.tmp Cargo.toml; \
	awk -v v="$(VERSION)" '/^name = "pg_deltax"$$/ {h=1} h && /^version = / {sub(/"[^"]*"/, "\"" v "\""); h=0} {print}' Cargo.lock > Cargo.lock.tmp && mv Cargo.lock.tmp Cargo.lock; \
	got="$$(awk '/^version = / {gsub(/"/, "", $$3); print $$3; exit}' Cargo.toml)"; \
	if [ "$$got" != "$(VERSION)" ]; then \
	  echo "ERROR: version bump failed (Cargo.toml shows '$$got')"; git checkout -- Cargo.toml Cargo.lock; exit 1; \
	fi; \
	echo "--> committing, tagging, pushing"; \
	git add Cargo.toml Cargo.lock; \
	git commit -m "Release $$tag"; \
	git tag -a "$$tag" -m "Release $$tag"; \
	git push origin "$$branch"; \
	git push origin "$$tag"; \
	echo "==> Pushed $$tag — the Release workflow will build and publish artifacts."

clean:
	docker volume rm pg_deltax_target_pg17 pg_deltax_target_pg18 $(CARGO_VOL) 2>/dev/null || true
	docker builder prune -f --filter type=regular 2>/dev/null || true
