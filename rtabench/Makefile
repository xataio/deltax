# EC2 RTABench benchmark workflow
#
# Lifecycle:
#   make launch-ec2              # launch EC2 instance, prints export EC2=<ip>
#   make setup EC2=<ip>          # install PG18, Rust, pgrx, build extension, load data
#   make destroy-ec2             # terminate the instance
#
# Iterating on code changes:
#   make deploy EC2=<ip>         # rsync source, recompile, restart PG
#   make bench EC2=<ip>          # run all 31 queries, download results
#   make query EC2=<ip> Q=10     # EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) a single query (by 4-digit number)
#
# Ad-hoc:
#   make sql EC2=<ip> SQL="SHOW work_mem"
#   make psql EC2=<ip>

EC2 ?=
EC2_KEY ?= ~/.ssh/tsg.pem
EC2_USER ?= ubuntu
EC2_SSH = ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no $(EC2_USER)@$(EC2)
EC2_SCP = scp -i $(EC2_KEY) -o StrictHostKeyChecking=no
EC2_PG_CONFIG = /usr/lib/postgresql/18/bin/pg_config
EC2_DB = test
PG_DELTAX_SRC = $(CURDIR)/..

# Side-by-side TimescaleDB cluster on the same EC2 box (PG16 on port 5433,
# loaded with the same RTABench data). Used for plan-shape comparison via
# `make query-ts` / `make query-both`.
TS_PORT = 5433
TS_PSQL = sudo -u postgres /usr/lib/postgresql/16/bin/psql -p $(TS_PORT)

RESULTS_DIR = results
HISTORY_DIR = $(RESULTS_DIR)/history

# Path of our db subdir inside the public rtabench checkout — same shape
# CLICKBENCH_DIR uses for ClickBench. When this exists, `make bench` copies
# the JSON in, generates an HTML comparison report at rtabench's root
# (reading whatever *other* sibling databases have checked in), and opens it.
RTABENCH_DIR ?= $(CURDIR)/../../../rtabench/pg_deltax

AWS_PROFILE ?= management
AWS_NAME = rtabench-pg-deltax

.PHONY: launch-ec2 destroy-ec2 ssh setup deploy bench report fetch-competitors query query-cold sql psql \
        setup-ts query-ts query-both sql-ts psql-ts

# Launch a new EC2 instance (terminates any existing one with the same name)
launch-ec2:
	@bash launch-ec2.sh

# Terminate the EC2 instance
destroy-ec2:
	@EXISTING=$$(aws ec2 describe-instances --profile "$(AWS_PROFILE)" \
		--filters "Name=tag:Name,Values=$(AWS_NAME)" "Name=instance-state-name,Values=running,stopped,pending" \
		--query 'Reservations[*].Instances[*].[InstanceId,PublicIpAddress]' --output text); \
	if [ -z "$$EXISTING" ]; then \
		echo "No running instance found with name $(AWS_NAME)"; \
	else \
		IDS=$$(echo "$$EXISTING" | awk '{print $$1}'); \
		echo "Terminating: $$EXISTING"; \
		aws ec2 terminate-instances --profile "$(AWS_PROFILE)" --instance-ids $$IDS --output text; \
		aws ec2 wait instance-terminated --profile "$(AWS_PROFILE)" --instance-ids $$IDS; \
		echo "Terminated."; \
	fi

# SSH into the EC2 instance with agent forwarding
ssh:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	$(EC2_SSH) -A -t

# Full setup: sync everything, run benchmark.sh (install deps + build + load + compress)
setup:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	@echo "=== Syncing pg_deltax source ==="
	rsync -az --delete --exclude target --exclude .git --exclude docker --exclude tests --exclude '*.md' \
		-e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(PG_DELTAX_SRC)/ $(EC2_USER)@$(EC2):~/pg_deltax/
	@echo "=== Syncing rtabench scripts ==="
	rsync -az -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(CURDIR)/ $(EC2_USER)@$(EC2):~/rtabench/
	@echo "=== Running benchmark.sh (this takes a while) ==="
	$(EC2_SSH) "cd ~/rtabench && SEGMENT_SIZE='$(SEGMENT_SIZE)' bash benchmark.sh"

# Sync source + rtabench scripts, recompile extension, restart PostgreSQL
deploy:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	@echo "=== Syncing pg_deltax source ==="
	rsync -az --delete --exclude target --exclude .git --exclude docker --exclude tests --exclude '*.md' \
		-e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(PG_DELTAX_SRC)/ $(EC2_USER)@$(EC2):~/pg_deltax/
	@echo "=== Syncing rtabench scripts ==="
	rsync -az -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(CURDIR)/ $(EC2_USER)@$(EC2):~/rtabench/
	@echo "=== Compiling extension (release) ==="
	@START=$$(date +%s); \
	$(EC2_SSH) 'cd ~/pg_deltax && sudo env "PATH=$$HOME/.cargo/bin:$$PATH" "RUSTUP_HOME=$${RUSTUP_HOME:-$$HOME/.rustup}" "CARGO_HOME=$${CARGO_HOME:-$$HOME/.cargo}" "PGRX_HOME=$$HOME/.pgrx" cargo pgrx install --pg-config $(EC2_PG_CONFIG) --release' && \
	END=$$(date +%s) && \
	echo "=== Compilation took $$((END - START))s ==="
	@echo "=== Restarting PostgreSQL ==="
	$(EC2_SSH) 'sudo systemctl restart postgresql'
	@echo "=== Done ==="

# Run all 31 RTABench queries (3x each) and download results
bench:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	@echo "=== Running RTABench ==="
	$(EC2_SSH) 'cd ~/rtabench && bash run.sh | tee /tmp/bench_output.txt'
	@echo "=== Parsing results ==="
	$(EC2_SSH) 'cd ~/rtabench && source load_stats.env 2>/dev/null; LOAD_TIME=$${LOAD_TIME:-0} DATA_SIZE=$${DATA_SIZE:-0} bash parse-results.sh < /tmp/bench_output.txt > /tmp/pg_deltax.json'
	@mkdir -p $(RESULTS_DIR) $(HISTORY_DIR)
	$(EC2_SCP) $(EC2_USER)@$(EC2):/tmp/pg_deltax.json $(RESULTS_DIR)/pg_deltax.json
	$(eval TS := $(shell date +%Y%m%d_%H%M%S))
	$(eval COMMIT := $(shell cd $(PG_DELTAX_SRC) && git rev-parse --short HEAD))
	@mkdir -p $(HISTORY_DIR)/$(TS)_$(COMMIT)
	cp $(RESULTS_DIR)/pg_deltax.json $(HISTORY_DIR)/$(TS)_$(COMMIT)/pg_deltax.json
	@echo "=== Results saved to $(RESULTS_DIR)/pg_deltax.json ==="
	@echo "=== Archived to $(HISTORY_DIR)/$(TS)_$(COMMIT)/ ==="
	@if [ -d "$(RTABENCH_DIR)" ] || [ -d "$(dir $(RTABENCH_DIR))" ]; then \
		echo "=== Generating rtabench HTML report ==="; \
		mkdir -p $(RTABENCH_DIR)/results; \
		cp $(RESULTS_DIR)/pg_deltax.json $(RTABENCH_DIR)/results/c6a.4xlarge.json; \
		python3 $(CURDIR)/generate-report.py $(RTABENCH_DIR)/..; \
		open $(RTABENCH_DIR)/../index.html; \
	else \
		echo "=== $(RTABENCH_DIR) not found — skipping rtabench HTML report ==="; \
	fi

# Regenerate the HTML report from existing results JSON — no bench run.
# Useful after dropping competitor JSONs (e.g. timescaledb/results/*.json)
# into ~/src/rtabench/ for a side-by-side comparison.
report:
	@test -d "$(RTABENCH_DIR)" || test -d "$(dir $(RTABENCH_DIR))" || \
		(echo "$(RTABENCH_DIR) not found — clone https://github.com/timescale/rtabench first"; exit 1)
	@mkdir -p $(RTABENCH_DIR)/results
	@test -f $(RESULTS_DIR)/pg_deltax.json && \
		cp $(RESULTS_DIR)/pg_deltax.json $(RTABENCH_DIR)/results/c6a.4xlarge.json || true
	python3 $(CURDIR)/generate-report.py $(RTABENCH_DIR)/..
	@open $(RTABENCH_DIR)/../index.html

# Download comparison results from rtabench.com and drop one JSON per system
# into ~/src/rtabench/<system>/results/c6a.4xlarge.json. After this runs once,
# `make report` shows every database as a column in the comparison table.
# The competitor JSONs aren't checked into the rtabench repo (unlike ClickBench),
# so this step is needed to bootstrap the comparison.
fetch-competitors:
	@test -d "$(RTABENCH_DIR)/.." || (echo "$(RTABENCH_DIR)/.. not found — clone https://github.com/timescale/rtabench first"; exit 1)
	python3 $(CURDIR)/fetch-competitors.py --rtabench-root $(RTABENCH_DIR)/..

# Run a single query against pg_deltax. First with `\timing on` (no
# instrumentation — accurate wall clock), then `EXPLAIN (ANALYZE, BUFFERS,
# TIMING OFF)` for plan shape and actual row counts. Plain EXPLAIN ANALYZE
# is intentionally avoided: per-tuple `gettimeofday()` instrumentation
# inflates wall time by 100×+ on plans that filter billions of rows
# (Q0030's 4.6B `Rows Removed by Join Filter` measured 408s in EXPLAIN
# ANALYZE vs ~3s real). TIMING OFF removes the timer but row counters
# remain — still adds 20–60s on Q0030-shaped plans, hence the separate
# wall-clock pass.
# Usage: make query EC2=<ip> Q=10 [SET="SET pg_deltax.parallel_workers=4"]
query:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> Q=<NNNN>"; exit 1)
	@test -n "$(Q)"   || (echo "Usage: make $@ EC2=<ip> Q=<NNNN>"; exit 1)
	$(EC2_SSH) "QN=\$$(printf '%04d' $(Q)) && \
		F=\$$(ls ~/rtabench/queries/\$${QN}_*.sql | head -1) && \
		test -n \"\$$F\" || (echo \"No query file matching \$${QN}_*.sql\"; exit 1) && \
		SET='$(SET)' bash ~/rtabench/run-query.sh \"\$$F\" sudo -u postgres psql $(EC2_DB)"

# Cold variant: restart PG + drop OS caches first. Usage: make query-cold EC2=<ip> Q=10
query-cold:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> Q=<NNNN>"; exit 1)
	@test -n "$(Q)"   || (echo "Usage: make $@ EC2=<ip> Q=<NNNN>"; exit 1)
	$(EC2_SSH) "sudo systemctl restart postgresql && \
		sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null && \
		sleep 1 && \
		QN=\$$(printf '%04d' $(Q)) && \
		F=\$$(ls ~/rtabench/queries/\$${QN}_*.sql | head -1) && \
		test -n \"\$$F\" || (echo \"No query file matching \$${QN}_*.sql\"; exit 1) && \
		echo '(cold)' && \
		SET='$(SET)' bash ~/rtabench/run-query.sh \"\$$F\" sudo -u postgres psql $(EC2_DB)"

# Run arbitrary SQL. Usage: make sql EC2=<ip> SQL="SHOW work_mem"
sql:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> SQL=\"...\""; exit 1)
	$(EC2_SSH) "sudo -u postgres psql $(EC2_DB) -c \"$(SQL)\""

# Open interactive psql session
psql:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	$(EC2_SSH) -t "sudo -u postgres psql $(EC2_DB)"

# --- TimescaleDB side-by-side cluster -----------------------------------------
# Install PG16 + TimescaleDB 2.18.1 on port 5433 alongside the existing PG18
# cluster. Same RTABench data, separate cluster so plans can be compared on
# identical hardware.
setup-ts:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	@echo "=== Syncing rtabench scripts ==="
	rsync -az -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(CURDIR)/ $(EC2_USER)@$(EC2):~/rtabench/
	@echo "=== Running timescale-setup.sh (this takes a while) ==="
	$(EC2_SSH) 'cd ~/rtabench && bash timescale-setup.sh'

# Run a single query against the TimescaleDB cluster: wall-clock first,
# then plan with row counts (TIMING OFF, see notes on `query` above).
# Usage: make query-ts EC2=<ip> Q=10 [SET="SET ..."]
query-ts:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> Q=<NNNN>"; exit 1)
	@test -n "$(Q)"   || (echo "Usage: make $@ EC2=<ip> Q=<NNNN>"; exit 1)
	$(EC2_SSH) "QN=\$$(printf '%04d' $(Q)) && \
		F=\$$(ls ~/rtabench/queries/\$${QN}_*.sql | head -1) && \
		test -n \"\$$F\" || (echo \"No query file matching \$${QN}_*.sql\"; exit 1) && \
		echo '(TimescaleDB)' && \
		SET='$(SET)' bash ~/rtabench/run-query.sh \"\$$F\" $(TS_PSQL) $(EC2_DB)"

# Run EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) on both clusters back-to-back. Usage: make query-both EC2=<ip> Q=10
query-both:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> Q=<NNNN>"; exit 1)
	@test -n "$(Q)"   || (echo "Usage: make $@ EC2=<ip> Q=<NNNN>"; exit 1)
	@echo "########## pg_deltax ##########"
	@$(MAKE) -s query EC2=$(EC2) Q=$(Q) SET="$(SET)"
	@echo
	@echo "########## TimescaleDB ##########"
	@$(MAKE) -s query-ts EC2=$(EC2) Q=$(Q) SET="$(SET)"

# Run arbitrary SQL against the TimescaleDB cluster. Usage: make sql-ts EC2=<ip> SQL="..."
sql-ts:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> SQL=\"...\""; exit 1)
	$(EC2_SSH) "$(TS_PSQL) $(EC2_DB) -c \"$(SQL)\""

# Open interactive psql session against the TimescaleDB cluster.
psql-ts:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	$(EC2_SSH) -t "$(TS_PSQL) $(EC2_DB)"
