PG_MAJOR ?= 17
DEV_IMAGE  = pg_cocoon-dev:pg$(PG_MAJOR)
IMAGE      = pg_cocoon:pg$(PG_MAJOR)
TARGET_VOL = pg_cocoon_target_pg$(PG_MAJOR)

.PHONY: dev-image image test build clippy run psql cargo clean

# Build the dev toolchain image (rebuilds only when Dockerfile.dev changes)
dev-image:
	docker build -f docker/Dockerfile.dev --build-arg PG_MAJOR=$(PG_MAJOR) -t $(DEV_IMAGE) docker/

# Generic: run any cargo command. Usage: make cargo CMD="check"
cargo: dev-image
	docker run --rm -v $(CURDIR):/build/pg_cocoon -v $(TARGET_VOL):/build/pg_cocoon/target \
		$(DEV_IMAGE) cargo $(CMD)

test: dev-image
	docker run --rm -v $(CURDIR):/build/pg_cocoon -v $(TARGET_VOL):/build/pg_cocoon/target \
		$(DEV_IMAGE) sh -c "cargo pgrx test pg$(PG_MAJOR)"

build: dev-image
	docker run --rm -v $(CURDIR):/build/pg_cocoon -v $(TARGET_VOL):/build/pg_cocoon/target \
		$(DEV_IMAGE) cargo build --features pg$(PG_MAJOR) --no-default-features

clippy: dev-image
	docker run --rm -v $(CURDIR):/build/pg_cocoon -v $(TARGET_VOL):/build/pg_cocoon/target \
		$(DEV_IMAGE) cargo clippy --features pg$(PG_MAJOR) --no-default-features

# Build the runtime image (production-like, no build tools)
image:
	docker build -f docker/Dockerfile --build-arg PG_MAJOR=$(PG_MAJOR) -t $(IMAGE) .

# Run postgres with the extension for manual testing
run: image
	docker run --rm --name pg_cocoon -p 5432:5432 -e POSTGRES_PASSWORD=postgres $(IMAGE)

psql:
	docker exec -it pg_cocoon psql -U postgres

clean:
	docker volume rm $(TARGET_VOL) 2>/dev/null || true
