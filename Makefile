# -------- Metacat Makefile (Quarkus + gRPC) --------
# Quick refs:
#   make / make build           # build all (skip tests)
#   make test                   # run service tests
#   make proto                  # generate + package proto jar
#   make run                    # run Quarkus in dev (foreground)
#   make start                  # start Quarkus in dev (background, writes PID)
#   make stop                   # stop background dev
#   make logs                   # tail background dev logs
#   make docker                 # build container image via Quarkus
#
# Overrides:
#   make MVN=mvnw QUARKUS_PROFILE=dev QUARKUS_DEV_ARGS="-Dquarkus.http.port=8082" start
#   make MVN=mvnw

.SHELLFLAGS := -eo pipefail -c
SHELL       := bash
MAKEFLAGS  += --no-builtin-rules
.ONESHELL:

MVN ?= mvn

# ---------- Quarkus dev settings ----------
QUARKUS_PROFILE  ?= dev
QUARKUS_DEV_ARGS ?=

# ---------- Dev dirs ----------
PID_DIR := .devpids
LOG_DIR := .devlogs
BIN_DIR := .devbin

$(shell mkdir -p $(PID_DIR) $(LOG_DIR) $(BIN_DIR) >/dev/null)

# ---------- Aggregates ----------
.PHONY: all build
all: build

build: proto build-all

.PHONY: proto
proto:
	@echo "==> [PROTO] package generated stubs"
	$(MVN) -q -f proto/pom.xml -DskipTests package

.PHONY: build-all
build-all:
	@echo "==> [BUILD] all modules"
	$(MVN) -q -DskipTests package

# ---------- Tests ----------
.PHONY: test unit-test integration-test verify
test:
	$(MVN) -pl service -am verify

unit-test:
	$(MVN) -pl service -am -DskipITs=true test

integration-test:
	$(MVN) -pl service -am -DskipUTs=true -DfailIfNoTests=false verify

verify:
	$(MVN) -pl service -am verify

# ---------- Clean ----------
.PHONY: clean clean-java clean-dev
clean: clean-java clean-dev
clean-java:
	$(MVN) -q -T 1C clean || true

clean-dev:
	@echo "==> [CLEAN-DEV] removing pid/logs and isolated repos"
	rm -rf $(PID_DIR) $(LOG_DIR) $(BIN_DIR)
	mkdir -p $(PID_DIR) $(LOG_DIR) $(BIN_DIR)

# ---------- Dev (foreground) ----------
# Runs Quarkus dev in the foreground (CTRL-C to stop).
.PHONY: run
run: build
	@echo "==> [DEV] quarkus:dev (profile=$(QUARKUS_PROFILE))"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  $(QUARKUS_DEV_ARGS) \
		-pl service -am \
	  io.quarkus:quarkus-maven-plugin:${quarkus.platform.version}:dev

# ---------- Dev (background) ----------
# Start/stop/logs use PID and LOG files for the 'service' module.
SERVICE_NAME := service
PID_FILE := $(PID_DIR)/$(SERVICE_NAME).pid
LOG_FILE := $(LOG_DIR)/$(SERVICE_NAME).log

define _bg_and_pid
( \
  set -m; \
  nohup bash -lc '$(MVN) -f ./pom.xml -Dquarkus.profile=$(QUARKUS_PROFILE) $(QUARKUS_DEV_ARGS) -pl service -am io.quarkus:quarkus-maven-plugin:${quarkus.platform.version}:dev' \
    >> "$(LOG_FILE)" 2>&1 & \
  echo $$! > "$(PID_FILE)"; \
)
endef

.PHONY: start
start:
	@if [ -f "$(PID_FILE)" ] && ps -p $$(cat "$(PID_FILE)") >/dev/null 2>&1; then \
	  echo "==> [DEV] already running (pid $$(cat $(PID_FILE)))"; \
	else \
	  echo "==> [DEV] starting in background (profile=$(QUARKUS_PROFILE))"; \
	  $(call _bg_and_pid); \
	  sleep 1; \
	  echo "==> [DEV] pid $$(cat $(PID_FILE)) | logs -> $(LOG_FILE)"; \
	fi

.PHONY: stop
stop:
	@if [ -f "$(PID_FILE)" ]; then \
	  PID=$$(cat "$(PID_FILE)"); \
	  if ps -p $$PID >/dev/null 2>&1; then \
	    echo "==> [DEV] stopping pid $$PID"; \
	    kill $$PID || true; \
	  else \
	    echo "==> [DEV] stale PID file (no process)"; \
	  fi; \
	  rm -f "$(PID_FILE)"; \
	else \
	  echo "==> [DEV] not running"; \
	fi

.PHONY: logs
logs:
	@if [ -f "$(LOG_FILE)" ]; then \
	  echo "==> [LOGS] tail -f $(LOG_FILE)"; \
	  tail -f "$(LOG_FILE)"; \
	else \
	  echo "==> [LOGS] no log file yet: $(LOG_FILE)"; \
	fi

.PHONY: status
status:
	@if [ -f "$(PID_FILE)" ] && ps -p $$(cat "$(PID_FILE)") >/dev/null 2>&1; then \
	  echo "==> [STATUS] running (pid $$(cat $(PID_FILE)))"; \
	else \
	  echo "==> [STATUS] not running"; \
	fi

# ---------- CLI  ----------
M2_CLI_DIR := $(BIN_DIR)/.m2-cli

VERSION := $(shell mvn -q -DforceStdout help:evaluate -Dexpression=project.version -Drecursive=false 2>/dev/null | grep -v '^\[' | tail -n1)
ifeq ($(strip $(VERSION)),)
  VERSION := 0.1.0-SNAPSHOT
endif

PROTO_JAR := proto/target/metacat-proto-$(VERSION).jar

.PHONY: cli-clean
cli-clean:
	@echo "==> [CLI] clean isolated repo and output"
	rm -rf $(M2_CLI_DIR) client-cli/target
	mkdir -p $(M2_CLI_DIR)

.PHONY: parent-cli
parent-cli:
	@echo "==> [CLI] install parent POM into isolated repo"
	mvn -q -Dmaven.repo.local=$(M2_CLI_DIR) \
	  org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
	  -DgroupId=ai.floedb.metacat \
	  -DartifactId=metacat \
	  -Dversion=$(VERSION) \
	  -Dpackaging=pom \
	  -Dfile=pom.xml

.PHONY: proto-cli
proto-cli:
	@echo "==> [PROTO-CLI] install existing proto jar into isolated repo (no rebuild)"
	@test -f "$(PROTO_JAR)" || { echo "Missing $(PROTO_JAR). Build proto first."; exit 1; }
	mvn -q -Dmaven.repo.local=$(M2_CLI_DIR) \
	  org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file \
	  -Dfile=$(PROTO_JAR) \
	  -DgroupId=ai.floedb.metacat \
	  -DartifactId=metacat-proto \
	  -Dversion=$(VERSION) \
	  -Dpackaging=jar

.PHONY: cli
cli: cli-clean parent-cli proto-cli
	@echo "==> [CLI] build client-cli against isolated repo"
	mvn -q -f client-cli/pom.xml \
	  -Dmaven.repo.local=$(M2_CLI_DIR) \
	  -DskipTests \
	  clean package

.PHONY: cli-run
cli-run: cli
	@echo "==> [RUN] client CLI"
	java -jar client-cli/target/quarkus-app/quarkus-run.jar $(ARGS)
# ---------- Docker (Quarkus container-image) ----------
# Requires proper container-image config in service/pom.xml or application.properties
.PHONY: docker
docker:
	@echo "==> [DOCKER] quarkus container-image build"
	$(MVN) -f service/pom.xml -DskipTests -Dquarkus.container-image.build=true package

# ---------- Lint/format (optional hook points) ----------
.PHONY: fmt
fmt:
	@echo "==> [FMT] (google-java-format)"
	$(MVN) -q fmt:format

.PHONY: help
help:
	@awk 'BEGIN {FS":.*?#"} /^[a-zA-Z0-9._%-]+:.*?#/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

