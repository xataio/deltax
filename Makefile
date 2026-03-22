PG_MAJOR ?= 17
DEV_IMAGE  = pg_deltax-dev:pg$(PG_MAJOR)
IMAGE      = pg_deltax:pg$(PG_MAJOR)
COV_IMAGE  = pg_deltax-cov:pg$(PG_MAJOR)
TARGET_VOL      = pg_deltax_target_pg$(PG_MAJOR)
CARGO_VOL       = pg_deltax_cargo
QUERY_CONTAINER = pg_deltax_query

PG_VERSIONS ?= 17 18
VENV         = .venv

.PHONY: dev-image image image-fresh test build clippy coverage coverage-all run psql cargo clean \
       integration-test \
       bench-clickbench bench-clickbench-keep bench-clickbench-full bench-clean \
       bench-timescaledb bench-compare bench-all \
       run-sql run-sql-file logs logs-all logs-follow

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
		PG_DELTAX_IMAGE=pg_deltax:pg$$v $(VENV)/bin/pytest tests/ -v; \
	done

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

bench-timescaledb: $(VENV)/.stamp
	TSDB_VARIANT=tsl $(VENV)/bin/pytest tests/bench_clickbench_timescaledb.py -v -s

bench-compare: $(VENV)/.stamp
	$(VENV)/bin/python tests/bench_compare.py

bench-all: bench-clickbench bench-timescaledb bench-compare

clean:
	docker volume rm pg_deltax_target_pg17 pg_deltax_target_pg18 $(CARGO_VOL) 2>/dev/null || true
	docker builder prune -f --filter type=regular 2>/dev/null || true
