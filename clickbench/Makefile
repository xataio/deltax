# EC2 ClickBench benchmark workflow
#
# Lifecycle:
#   make launch-ec2              # launch EC2 instance, prints export EC2=<ip>
#   make setup EC2=<ip>          # install PG18, Rust, pgrx, build extension, load data (parquet)
#   make setup EC2=<ip> PARQUET=0  # same but load from TSV instead
#   make destroy-ec2             # terminate the instance
#
# Iterating on code changes:
#   make deploy EC2=<ip>         # rsync source, recompile, restart PG
#   make bench EC2=<ip>          # run all 43 queries, download results
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
CLICKBENCH_DIR ?= $(CURDIR)/../../../ClickBench/pg-deltax

AWS_PROFILE ?= management
AWS_NAME = clickbench-pg-deltax
REFERENCE_AWS_NAME = clickbench-pg-deltax-reference

.PHONY: launch-ec2 destroy-ec2 launch-reference-ec2 destroy-reference-ec2 ssh setup deploy bench query sql psql reference verify

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

# Run full ClickBench (43×3 queries) and download results
bench:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	@echo "=== Running ClickBench ==="
	$(EC2_SSH) 'cd ~/clickbench && bash run.sh | tee /tmp/bench_output.txt'
	@echo "=== Parsing results ==="
	$(EC2_SSH) 'cd ~/clickbench && source load_stats.env 2>/dev/null; LOAD_TIME=$${LOAD_TIME:-0} DATA_SIZE=$${DATA_SIZE:-0} bash parse-results.sh < /tmp/bench_output.txt > /tmp/pg_deltax.json'
	@mkdir -p $(RESULTS_DIR) $(HISTORY_DIR)
	$(EC2_SCP) $(EC2_USER)@$(EC2):/tmp/pg_deltax.json $(RESULTS_DIR)/pg_deltax.json
	$(eval TS := $(shell date +%Y%m%d_%H%M%S))
	$(eval COMMIT := $(shell cd $(PG_DELTAX_SRC) && git rev-parse --short HEAD))
	@mkdir -p $(HISTORY_DIR)/$(TS)_$(COMMIT)
	cp $(RESULTS_DIR)/pg_deltax.json $(HISTORY_DIR)/$(TS)_$(COMMIT)/pg_deltax.json
	@echo "=== Results saved to $(RESULTS_DIR)/pg_deltax.json ==="
	@echo "=== Archived to $(HISTORY_DIR)/$(TS)_$(COMMIT)/ ==="
	@if [ -d "$(CLICKBENCH_DIR)" ]; then \
		echo "=== Generating ClickBench HTML report ==="; \
		cp $(RESULTS_DIR)/pg_deltax.json $(CLICKBENCH_DIR)/results/c6a.4xlarge.json; \
		cd $(CLICKBENCH_DIR)/.. && ./generate-results.sh; \
		open $(CLICKBENCH_DIR)/../index.html; \
	fi

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
