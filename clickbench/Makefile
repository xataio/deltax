# EC2 ClickBench benchmark workflow
#
# Lifecycle:
#   make launch-ec2              # launch EC2 instance, prints export EC2=<ip>
#   make setup EC2=<ip>          # install PG18, Rust, pgrx, build extension, load data, compress
#   make destroy-ec2             # terminate the instance
#
# Iterating on code changes:
#   make deploy EC2=<ip>         # rsync source, recompile, restart PG
#   make bench EC2=<ip>          # run all 43 queries, download results
#   make query EC2=<ip> Q=33     # EXPLAIN ANALYZE a single query
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

RESULTS_DIR = results
HISTORY_DIR = $(RESULTS_DIR)/history

AWS_PROFILE ?= management
AWS_NAME = clickbench-pg-deltax

.PHONY: launch-ec2 destroy-ec2 ssh setup deploy bench query sql psql

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
	rsync -az --exclude target --exclude .git --exclude docker --exclude tests --exclude '*.md' \
		-e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(PG_DELTAX_SRC)/ $(EC2_USER)@$(EC2):~/pg_deltax/
	@echo "=== Syncing clickbench scripts ==="
	rsync -az -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(CURDIR)/ $(EC2_USER)@$(EC2):~/clickbench/
	@echo "=== Running benchmark.sh (this takes a while) ==="
	$(EC2_SSH) 'cd ~/clickbench && bash benchmark.sh'

# Sync source + clickbench scripts, recompile extension, restart PostgreSQL
deploy:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	@echo "=== Syncing pg_deltax source ==="
	rsync -az --exclude target --exclude .git --exclude docker --exclude tests --exclude '*.md' \
		-e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(PG_DELTAX_SRC)/ $(EC2_USER)@$(EC2):~/pg_deltax/
	@echo "=== Syncing clickbench scripts ==="
	rsync -az -e "ssh -i $(EC2_KEY) -o StrictHostKeyChecking=no" \
		$(CURDIR)/ $(EC2_USER)@$(EC2):~/clickbench/
	@echo "=== Compiling extension (release) ==="
	@START=$$(date +%s); \
	$(EC2_SSH) 'cd ~/pg_deltax && sudo env "PATH=$$PATH" "RUSTUP_HOME=$${RUSTUP_HOME:-$$HOME/.rustup}" "CARGO_HOME=$${CARGO_HOME:-$$HOME/.cargo}" "PGRX_HOME=$$HOME/.pgrx" cargo pgrx install --pg-config $(EC2_PG_CONFIG) --release' && \
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
	$(EC2_SSH) 'cd ~/clickbench && bash parse-results.sh < /tmp/bench_output.txt > /tmp/pg_deltax.json'
	@mkdir -p $(RESULTS_DIR) $(HISTORY_DIR)
	$(EC2_SCP) $(EC2_USER)@$(EC2):/tmp/pg_deltax.json $(RESULTS_DIR)/pg_deltax.json
	$(eval TS := $(shell date +%Y%m%d_%H%M%S))
	$(eval COMMIT := $(shell cd $(PG_DELTAX_SRC) && git rev-parse --short HEAD))
	@mkdir -p $(HISTORY_DIR)/$(TS)_$(COMMIT)
	cp $(RESULTS_DIR)/pg_deltax.json $(HISTORY_DIR)/$(TS)_$(COMMIT)/pg_deltax.json
	@echo "=== Results saved to $(RESULTS_DIR)/pg_deltax.json ==="
	@echo "=== Archived to $(HISTORY_DIR)/$(TS)_$(COMMIT)/ ==="

# EXPLAIN ANALYZE a single query. Usage: make query EC2=<ip> Q=33 [SET="SET pg_deltax.parallel_workers=4"]
query:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> Q=<number>"; exit 1)
	@test -n "$(Q)" || (echo "Usage: make $@ EC2=<ip> Q=<number>"; exit 1)
	$(EC2_SSH) "QUERY=\$$(sed -n '$$(($(Q)+1))p' ~/clickbench/queries.sql) && \
		echo \"Q$(Q): \$$QUERY\" && echo '---' && \
		sudo -u postgres psql $(EC2_DB) -c '$(SET)' -c \"EXPLAIN ANALYZE \$$QUERY\""

# Run arbitrary SQL. Usage: make sql EC2=<ip> SQL="SHOW work_mem"
sql:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip> SQL=\"...\""; exit 1)
	$(EC2_SSH) "sudo -u postgres psql $(EC2_DB) -c \"$(SQL)\""

# Open interactive psql session
psql:
	@test -n "$(EC2)" || (echo "Usage: make $@ EC2=<ip>"; exit 1)
	$(EC2_SSH) -t "sudo -u postgres psql $(EC2_DB)"
