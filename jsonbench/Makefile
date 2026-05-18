# EC2 JSONBench benchmark workflow
#
# Lifecycle:
#   make launch-ec2                    # launch EC2 m6i.8xlarge, prints export EC2=<ip>
#   make setup EC2=<ip>                # default 100m rows
#   make setup EC2=<ip> SCALE=1        # 1m smoke test
#   make setup EC2=<ip> SCALE=1000     # full 1B rows (slow)
#   make destroy-ec2                   # terminate the instance
#
# Iterating on code changes:
#   make deploy EC2=<ip>               # rsync source, recompile, restart PG
#   make bench EC2=<ip>                # run all 5 queries, download results
#   make query EC2=<ip> Q=2            # EXPLAIN ANALYZE a single query (Q=0..4)
#
# Ad-hoc:
#   make sql EC2=<ip> SQL="SHOW work_mem"
#   make psql EC2=<ip>

EC2 ?=
EC2_KEY ?= ~/.ssh/tsg.pem
EC2_USER ?= ubuntu
SCALE ?= 100
EC2_SSH = ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no $(EC2_USER)@$(EC2)
EC2_SCP = scp -i $(EC2_KEY) -o StrictHostKeyChecking=no
EC2_PG_CONFIG = /usr/lib/postgresql/18/bin/pg_config
EC2_DB = bluesky
PG_DELTAX_SRC = $(CURDIR)/..

RESULTS_DIR = results
HISTORY_DIR = $(RESULTS_DIR)/history
JSONBENCH_DIR ?= /Users/tsg/src/JSONBench

AWS_PROFILE ?= management
AWS_NAME = jsonbench-pg-deltax

.PHONY: launch-ec2 destroy-ec2 ssh setup deploy bench query query-cold sql psql

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
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> [SCALE=<n>]"; exit 1)
	@echo "=== Syncing pg_deltax source ==="
	rsync -az --delete --exclude target --exclude .git --exclude docker --exclude tests --exclude '*.md' \
		-e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(PG_DELTAX_SRC)/ $(EC2_USER)@$(EC2):~/pg_deltax/
	@echo "=== Syncing jsonbench scripts ==="
	rsync -az -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(CURDIR)/ $(EC2_USER)@$(EC2):~/jsonbench/
	@echo "=== Running benchmark.sh (SCALE=$(SCALE), this takes a while) ==="
	$(EC2_SSH) 'cd ~/jsonbench && SCALE=$(SCALE) bash benchmark.sh'

# Sync source + jsonbench scripts, recompile extension, restart PostgreSQL
deploy:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	@echo "=== Syncing pg_deltax source ==="
	rsync -az --delete --exclude target --exclude .git --exclude docker --exclude tests --exclude '*.md' \
		-e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(PG_DELTAX_SRC)/ $(EC2_USER)@$(EC2):~/pg_deltax/
	@echo "=== Syncing jsonbench scripts ==="
	rsync -az -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(CURDIR)/ $(EC2_USER)@$(EC2):~/jsonbench/
	@echo "=== Compiling extension (release) ==="
	@START=$$(date +%s); \
	$(EC2_SSH) 'cd ~/pg_deltax && sudo env "PATH=$$HOME/.cargo/bin:$$PATH" "RUSTUP_HOME=$${RUSTUP_HOME:-$$HOME/.rustup}" "CARGO_HOME=$${CARGO_HOME:-$$HOME/.cargo}" "PGRX_HOME=$$HOME/.pgrx" cargo pgrx install --pg-config $(EC2_PG_CONFIG) --release' && \
	END=$$(date +%s) && \
	echo "=== Compilation took $$((END - START))s ==="
	@echo "=== Restarting PostgreSQL ==="
	$(EC2_SSH) 'sudo systemctl restart postgresql'
	@echo "=== Done ==="

# Run all 5 JSONBench queries (3x each) and download results
bench:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	@echo "=== Running JSONBench ==="
	$(EC2_SSH) 'cd ~/jsonbench && bash run.sh | tee /tmp/bench_output.txt'
	@echo "=== Parsing results ==="
	$(EC2_SSH) 'cd ~/jsonbench && source load_stats.env 2>/dev/null; LOAD_TIME=$${LOAD_TIME:-0} DATA_SIZE=$${DATA_SIZE:-0} NUM_LOADED=$${NUM_LOADED:-0} DATASET_SIZE=$${DATASET_SIZE:-0} bash parse-results.sh < /tmp/bench_output.txt > /tmp/pg_deltax.json'
	@mkdir -p $(RESULTS_DIR) $(HISTORY_DIR)
	$(EC2_SCP) $(EC2_USER)@$(EC2):/tmp/pg_deltax.json $(RESULTS_DIR)/pg_deltax.json
	$(eval TS := $(shell date +%Y%m%d_%H%M%S))
	$(eval COMMIT := $(shell cd $(PG_DELTAX_SRC) && git rev-parse --short HEAD))
	@mkdir -p $(HISTORY_DIR)/$(TS)_$(COMMIT)
	cp $(RESULTS_DIR)/pg_deltax.json $(HISTORY_DIR)/$(TS)_$(COMMIT)/pg_deltax.json
	@echo "=== Results saved to $(RESULTS_DIR)/pg_deltax.json ==="
	@echo "=== Archived to $(HISTORY_DIR)/$(TS)_$(COMMIT)/ ==="
	@if [ -d "$(JSONBENCH_DIR)" ]; then \
		DATASET_NUM=$$(jq -r '.dataset_size' $(RESULTS_DIR)/pg_deltax.json); \
		DATASET_LABEL=$$(awk -v n="$${DATASET_NUM}" 'BEGIN{ printf("%dm", n/1000000); }'); \
		DEST=$(JSONBENCH_DIR)/pg_deltax/results/m6i.8xlarge_bluesky_$${DATASET_LABEL}.json; \
		echo "=== Copying to upstream JSONBench dashboard inputs ==="; \
		mkdir -p $(JSONBENCH_DIR)/pg_deltax/results; \
		cp $(RESULTS_DIR)/pg_deltax.json $$DEST; \
		echo "    -> $$DEST"; \
		echo "=== Generating JSONBench HTML report ==="; \
		cd $(JSONBENCH_DIR) && bash generate-results.sh; \
		open $(JSONBENCH_DIR)/index.html; \
	fi

# EXPLAIN ANALYZE a single query. Usage: make query EC2=<ip> Q=2 [SET="SET pg_deltax.parallel_workers=4"]
query:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> Q=<0-4>"; exit 1)
	@test -n "$(Q)" || (echo "Usage: make $@ EC2=<ip> Q=<0-4>"; exit 1)
	$(EC2_SSH) "QUERY=\$$(sed -n '$$(($(Q)+1))p' ~/jsonbench/queries.sql) && \
		echo \"Q$(Q): \$$QUERY\" && echo '---' && \
		sudo -u postgres psql $(EC2_DB) -c '$(SET)' -c \"EXPLAIN ANALYZE \$$QUERY\""

# Cold EXPLAIN ANALYZE: restart PG + drop OS caches first. Usage: make query-cold EC2=<ip> Q=2
query-cold:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> Q=<0-4>"; exit 1)
	@test -n "$(Q)" || (echo "Usage: make $@ EC2=<ip> Q=<0-4>"; exit 1)
	$(EC2_SSH) "sudo systemctl restart postgresql && \
		sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null && \
		sleep 1 && \
		QUERY=\$$(sed -n '$$(($(Q)+1))p' ~/jsonbench/queries.sql) && \
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
