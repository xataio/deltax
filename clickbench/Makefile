# EC2 ClickBench benchmark workflow
#
# Lifecycle:
#   make launch-ec2              # launch EC2 instance, prints export EC2=<ip>
#   make bench-full EC2=<ip>     # full ClickBench-equivalent run (install + load + queries)
#   make bench EC2=<ip>          # queries-only against an already-loaded DB
#   make bench-concurrent EC2=<ip> # only the concurrent-QPS test; merges into the latest results JSON
#   make destroy-ec2             # terminate the instance
#
# All three bench targets use the shared ClickBench driver
# (lib/benchmark-common.sh from $(CLICKBENCH_REPO), default ../../../ClickBench)
# so timing methodology matches upstream: true-cold cycles between cold tries
# (./stop / wait / drop_caches / ./start / ./check). Differences:
#   * bench-full       ./install + download parquet + ./load + 43 queries +
#                      data-size + stop. NO concurrent-QPS (dashboard doesn't
#                      render it yet — saves ~10 min). ~20+ min. Submission-grade.
#   * bench            only the query phase (43×3) against whatever is loaded.
#                      ~10+ min. Same cold-cycle timing methodology as
#                      bench-full; emits Load time: 0 because no load happened.
#                      Use after `make setup` or `make bench-full` set up the DB.
#   * bench-concurrent only the 10-connection sustained-throughput test.
#                      ~10 min. Merges concurrent_qps / concurrent_error_ratio
#                      into the JSON produced by the most recent `make
#                      bench-full` (so it must run after a bench-full).
#
# Fast iteration on the extension itself:
#   make setup EC2=<ip>          # legacy: install + load data via xataio scripts
#   make setup EC2=<ip> PARQUET=0  # same but load from TSV instead
#   make deploy EC2=<ip>         # rsync source, recompile, restart PG
#   make query EC2=<ip> Q=33     # EXPLAIN ANALYZE a single query
#
# Correctness verification (the bench harness only times queries — it does
# not validate their output). The reference is generated once on a fresh
# vanilla-PG EC2 (separate Name tag, doesn't clobber the bench EC2) and
# committed; verify runs against the live bench EC2.
#   make launch-reference-ec2          # launch the vanilla-PG instance
#   make reference EC2=<vanilla-pg-ip> # one-time: vanilla PG + capture + rebuild JSON
#   make destroy-reference-ec2         # tear down the vanilla-PG instance
#   make verify    EC2=<bench-ip>      # diff current pg_deltax output vs committed JSON
#
# Ad-hoc:
#   make sql EC2=<ip> SQL="SHOW work_mem"
#   make psql EC2=<ip>

EC2 ?=
EC2_KEY ?= ~/.ssh/tsg.pem
EC2_USER ?= ubuntu
PARQUET ?= 1
EC2_SSH = ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no $(EC2_USER)@$(EC2)
EC2_SCP = scp -i $(EC2_KEY) -o StrictHostKeyChecking=no
EC2_PG_CONFIG = /usr/lib/postgresql/18/bin/pg_config
EC2_DB = test
PG_DELTAX_SRC = $(CURDIR)/..

RESULTS_DIR = results
HISTORY_DIR = $(RESULTS_DIR)/history
CLICKBENCH_DIR ?= $(CURDIR)/../../../ClickBench/pg_deltax
CLICKBENCH_REPO ?= $(CLICKBENCH_DIR)/..

AWS_PROFILE ?= management
AWS_NAME = clickbench-pg-deltax
REFERENCE_AWS_NAME = clickbench-pg-deltax-reference

.PHONY: launch-ec2 destroy-ec2 launch-reference-ec2 destroy-reference-ec2 ssh setup deploy bench bench-full bench-concurrent query sql psql reference verify

# Launch a new EC2 instance. Aborts if the name is already taken (never
# terminates implicitly); requires KEY_NAME — see launch-ec2.sh.
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

# Launch a separate EC2 instance for the vanilla-PG correctness reference run.
# Uses a different Name tag so it doesn't clobber the bench EC2.
launch-reference-ec2:
	@bash launch-ec2.sh --name $(REFERENCE_AWS_NAME) --reference

# Terminate the reference EC2 instance.
destroy-reference-ec2:
	@EXISTING=$$(aws ec2 describe-instances --profile "$(AWS_PROFILE)" \
		--filters "Name=tag:Name,Values=$(REFERENCE_AWS_NAME)" "Name=instance-state-name,Values=running,stopped,pending" \
		--query 'Reservations[*].Instances[*].[InstanceId,PublicIpAddress]' --output text); \
	if [ -z "$$EXISTING" ]; then \
		echo "No running instance found with name $(REFERENCE_AWS_NAME)"; \
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
	@echo "=== Syncing clickbench scripts ==="
	rsync -az -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(CURDIR)/ $(EC2_USER)@$(EC2):~/clickbench/
	@echo "=== Running benchmark.sh (this takes a while) ==="
	$(EC2_SSH) 'cd ~/clickbench && PARQUET=$(PARQUET) bash benchmark.sh'

# Sync source + clickbench scripts, recompile extension, restart PostgreSQL
deploy:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	@echo "=== Syncing pg_deltax source ==="
	rsync -az --delete --exclude target --exclude .git --exclude docker --exclude tests --exclude '*.md' \
		-e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(PG_DELTAX_SRC)/ $(EC2_USER)@$(EC2):~/pg_deltax/
	@echo "=== Syncing clickbench scripts ==="
	rsync -az -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(CURDIR)/ $(EC2_USER)@$(EC2):~/clickbench/
	@echo "=== Compiling extension (release) ==="
	@START=$$(date +%s); \
	$(EC2_SSH) 'cd ~/pg_deltax && sudo env "PATH=$$HOME/.cargo/bin:$$PATH" "RUSTUP_HOME=$${RUSTUP_HOME:-$$HOME/.rustup}" "CARGO_HOME=$${CARGO_HOME:-$$HOME/.cargo}" "PGRX_HOME=$$HOME/.pgrx" cargo pgrx install --pg-config $(EC2_PG_CONFIG) --release' && \
	END=$$(date +%s) && \
	echo "=== Compilation took $$((END - START))s ==="
	@echo "=== Restarting PostgreSQL ==="
	$(EC2_SSH) 'sudo systemctl restart postgresql'
	@echo "=== Done ==="

# Run only the query phase + concurrent-QPS test against an already-loaded
# database. Uses the shared driver's true-cold cycle (stop/wait/drop_caches/
# start/check) between cold tries, so timing numbers are directly comparable
# to a full submission run — only load_time is recorded as 0 because no load
# happens. Use this during dev iteration after `make setup` or `make bench-full`
# has set up the DB. Run `make bench-full` instead when you need the full
# end-to-end methodology (install + load + queries) for a submission.
bench: BENCH_DRIVER_CMD = bash ../queries-only.sh
bench: BENCH_KIND = queries-only
bench: _bench-prepare _bench-run _bench-collect _bench-publish

# Full ClickBench-equivalent run minus the concurrent-QPS test: install ->
# start -> download parquet -> load -> 43 queries (3 tries each, true-cold
# cycle between cold tries) -> data-size -> stop. We skip the concurrent
# test because the dashboard UI doesn't render those metrics yet and the
# test adds ~10 min per run — use `make bench-concurrent` separately to
# capture them.
bench-full: BENCH_DRIVER_CMD = bash ../bench-full.sh
bench-full: BENCH_KIND = full
bench-full: _bench-prepare _bench-run _bench-collect _bench-publish

# Run only the concurrent-QPS sustained-throughput test against an already-
# loaded database. Merges concurrent_qps / concurrent_error_ratio into the
# JSON produced by the most recent `make bench-full`. Requires
# $(RESULTS_DIR)/pg_deltax.json to exist (i.e. you must have run bench-full
# at least once already).
bench-concurrent: BENCH_DRIVER_CMD = bash ../bench-concurrent.sh
bench-concurrent: BENCH_KIND = concurrent
bench-concurrent: BENCH_BUILD_RESULT_EXTRA = --base $(RESULTS_DIR)/pg_deltax.json
bench-concurrent: _bench-concurrent-precheck _bench-prepare _bench-run _bench-collect _bench-publish

_bench-concurrent-precheck:
	@test -f "$(RESULTS_DIR)/pg_deltax.json" || (echo "Missing $(RESULTS_DIR)/pg_deltax.json — run 'make bench-full EC2=<ip>' first"; exit 1)

.PHONY: _bench-prepare _bench-run _bench-collect _bench-publish _bench-concurrent-precheck

_bench-prepare:
	@test -n "$(EC2)" || (echo "Usage: make bench[-full] EC2=<ip>"; exit 1)
	@test -d "$(CLICKBENCH_REPO)/lib" || (echo "Missing $(CLICKBENCH_REPO)/lib — set CLICKBENCH_REPO to your ClickBench checkout"; exit 1)
	@test -d "$(CLICKBENCH_DIR)" || (echo "Missing $(CLICKBENCH_DIR) — set CLICKBENCH_DIR to your in-development pg-deltax dir"; exit 1)
	@test -f "$(CLICKBENCH_DIR)/template.json" || (echo "Missing $(CLICKBENCH_DIR)/template.json"; exit 1)
	@test -d "$(PG_DELTAX_SRC)" || (echo "Missing $(PG_DELTAX_SRC) — set PG_DELTAX_SRC to your pg_deltax source checkout"; exit 1)
	@echo "=== Syncing pg_deltax source to EC2 ==="
	# Same rsync as `make deploy` — populates ~/pg_deltax with your local
	# source so ./install can `cargo pgrx install` it instead of falling back
	# to `git clone` (which would fail for the private repo).
	rsync -az --delete --exclude target --exclude .git --exclude docker --exclude tests --exclude '*.md' \
		-e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		"$(PG_DELTAX_SRC)/" $(EC2_USER)@$(EC2):~/pg_deltax/
	@echo "=== Syncing shared driver + pg-deltax scripts + driver shims to EC2 ==="
	$(EC2_SSH) 'mkdir -p ~/clickbench'
	rsync -az --delete -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		"$(CLICKBENCH_REPO)/lib/" $(EC2_USER)@$(EC2):~/clickbench/lib/
	rsync -az --delete -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		"$(CLICKBENCH_DIR)/" $(EC2_USER)@$(EC2):~/clickbench/pg-deltax/
	$(EC2_SCP) $(CURDIR)/queries-only.sh $(CURDIR)/bench-full.sh $(CURDIR)/bench-concurrent.sh \
		$(EC2_USER)@$(EC2):~/clickbench/

_bench-run:
	@echo "=== Running $(BENCH_KIND) benchmark ==="
	# pipefail so a failing benchmark.sh propagates through tee and aborts make,
	# instead of silently producing a bench_output.txt with no triplets.
	$(EC2_SSH) 'set -o pipefail; cd ~/clickbench/pg-deltax && $(BENCH_DRIVER_CMD) 2>&1 | tee /tmp/bench_output.txt'

_bench-collect:
	@echo "=== Downloading log ==="
	@mkdir -p $(RESULTS_DIR) $(HISTORY_DIR)
	$(EC2_SCP) $(EC2_USER)@$(EC2):/tmp/bench_output.txt $(RESULTS_DIR)/bench_output.txt
	@echo "=== Building results JSON ==="
	python3 $(CURDIR)/build-result.py $(RESULTS_DIR)/bench_output.txt \
		--template $(CLICKBENCH_DIR)/template.json \
		--machine c6a.4xlarge \
		$(BENCH_BUILD_RESULT_EXTRA) \
		> $(RESULTS_DIR)/pg_deltax.json.new
	mv $(RESULTS_DIR)/pg_deltax.json.new $(RESULTS_DIR)/pg_deltax.json

_bench-publish:
	$(eval TS := $(shell date -u +%Y%m%d_%H%M%S))
	$(eval COMMIT := $(shell cd $(PG_DELTAX_SRC) && git rev-parse --short HEAD))
	@mkdir -p $(HISTORY_DIR)/$(TS)_$(COMMIT)_$(BENCH_KIND)
	cp $(RESULTS_DIR)/pg_deltax.json $(HISTORY_DIR)/$(TS)_$(COMMIT)_$(BENCH_KIND)/pg_deltax.json
	cp $(RESULTS_DIR)/bench_output.txt $(HISTORY_DIR)/$(TS)_$(COMMIT)_$(BENCH_KIND)/bench_output.txt
	@echo "=== Results saved to $(RESULTS_DIR)/pg_deltax.json ==="
	@echo "=== Archived to $(HISTORY_DIR)/$(TS)_$(COMMIT)_$(BENCH_KIND)/ ==="
	@echo "=== Generating ClickBench HTML report ==="
	@TODAY=$$(date -u +%Y%m%d); \
		mkdir -p $(CLICKBENCH_DIR)/results/$$TODAY; \
		cp $(RESULTS_DIR)/pg_deltax.json $(CLICKBENCH_DIR)/results/$$TODAY/c6a.4xlarge.json
	cd $(CLICKBENCH_REPO) && ./generate-results.sh
	@open $(CLICKBENCH_REPO)/index.html

# One-time: launch a SEPARATE vanilla-PG EC2 first, then run this to install PG,
# load the data with plain COPY, capture each of the 43 queries' result sets,
# pull the captured CSVs back, and (re)build clickbench/reference_results.json.
# Commit the resulting JSON.
reference:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<vanilla-pg-ip>"; exit 1)
	@echo "=== Syncing clickbench scripts ==="
	rsync -az -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(CURDIR)/ $(EC2_USER)@$(EC2):~/clickbench/
	@echo "=== Bootstrapping vanilla PG + loading data (this takes a while) ==="
	$(EC2_SSH) 'cd ~/clickbench && bash reference.sh'
	@echo "=== Capturing query result sets ==="
	$(EC2_SSH) 'cd ~/clickbench && DB=test bash capture_results.sh'
	@echo "=== Downloading results ==="
	rm -rf /tmp/clickbench_reference
	$(EC2_SCP) $(EC2_USER)@$(EC2):/tmp/results.tar.gz /tmp/clickbench_reference.tar.gz
	$(EC2_SCP) $(EC2_USER)@$(EC2):~/clickbench/reference_stats.env /tmp/clickbench_reference_stats.env
	mkdir -p /tmp/clickbench_reference
	tar -xzf /tmp/clickbench_reference.tar.gz -C /tmp/clickbench_reference --strip-components=1
	@echo "=== Building reference_results.json ==="
	python3 $(CURDIR)/verify_results.py build-reference /tmp/clickbench_reference \
		--stats-env /tmp/clickbench_reference_stats.env \
		-o $(CURDIR)/reference_results.json
	@echo "=== Done. Commit $(CURDIR)/reference_results.json. ==="

# Run the 43 ClickBench queries against the CURRENT pg_deltax DB on the bench EC2,
# diff against the committed reference. Exits non-zero on mismatch.
# Cheap: ~2 minutes of cold psql calls, no recompile, no data reload.
verify:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<bench-ip>"; exit 1)
	@test -f $(CURDIR)/reference_results.json || (echo "Missing reference_results.json — run 'make reference' first or pull it from git"; exit 1)
	@echo "=== Syncing capture script ==="
	rsync -az -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(CURDIR)/capture_results.sh $(CURDIR)/queries.sql $(EC2_USER)@$(EC2):~/clickbench/
	@echo "=== Capturing query result sets from pg_deltax DB ==="
	$(EC2_SSH) 'cd ~/clickbench && DB=test bash capture_results.sh'
	@echo "=== Downloading results ==="
	rm -rf /tmp/clickbench_verify
	$(EC2_SCP) $(EC2_USER)@$(EC2):/tmp/results.tar.gz /tmp/clickbench_verify.tar.gz
	mkdir -p /tmp/clickbench_verify
	tar -xzf /tmp/clickbench_verify.tar.gz -C /tmp/clickbench_verify --strip-components=1
	@echo "=== Comparing against $(CURDIR)/reference_results.json ==="
	python3 $(CURDIR)/verify_results.py check /tmp/clickbench_verify \
		--reference $(CURDIR)/reference_results.json

# EXPLAIN ANALYZE a single query. Usage: make query EC2=<ip> Q=33 [SET="SET pg_deltax.parallel_workers=4"]
query:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> Q=<number>"; exit 1)
	@test -n "$(Q)" || (echo "Usage: make $@ EC2=<ip> Q=<number>"; exit 1)
	$(EC2_SSH) "QUERY=\$$(sed -n '$$(($(Q)+1))p' ~/clickbench/queries.sql) && \
		echo \"Q$(Q): \$$QUERY\" && echo '---' && \
		sudo -u postgres psql $(EC2_DB) -c '$(SET)' -c \"EXPLAIN ANALYZE \$$QUERY\""

# Cold EXPLAIN ANALYZE: restart PG + drop OS caches first. Usage: make query-cold EC2=<ip> Q=7
query-cold:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> Q=<number>"; exit 1)
	@test -n "$(Q)" || (echo "Usage: make $@ EC2=<ip> Q=<number>"; exit 1)
	$(EC2_SSH) "sudo systemctl restart postgresql && \
		sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null && \
		sleep 1 && \
		QUERY=\$$(sed -n '$$(($(Q)+1))p' ~/clickbench/queries.sql) && \
		echo \"Q$(Q) (cold): \$$QUERY\" && echo '---' && \
		sudo -u postgres psql $(EC2_DB) -c '$(SET)' -c \"EXPLAIN ANALYZE \$$QUERY\""

# Run arbitrary SQL. Usage: make sql EC2=<ip> SQL="SHOW work_mem"
sql:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> SQL=\"...\""; exit 1)
	$(EC2_SSH) "sudo -u postgres psql $(EC2_DB) -c \"$(SQL)\""

# Open interactive psql session
psql:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	$(EC2_SSH) -t "sudo -u postgres psql $(EC2_DB)"
