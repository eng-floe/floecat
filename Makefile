# -------- Metacat Makefile (Quarkus + gRPC) --------
# Quick refs:
#   make / make build            # build all (skip tests)
#   make run                     # Quarkus dev
#   make start|stop|logs|status  # background dev helpers
#   make cli-run                 # build & run CLI
#   make docker                  # build service container image
#   make test|unit-test|integration-test|verify
#
# Examples:
#   make MVN=./mvnw run
#   make CLI_ISOLATED=0 cli-run

.SHELLFLAGS := -eo pipefail -c
SHELL       := bash
MAKEFLAGS  += --no-builtin-rules
.ONESHELL:
.DEFAULT_GOAL := build

MVN ?= mvn
MVN_FLAGS   := -q -T 1C --no-transfer-progress -DskipTests
MVN_TESTALL := --no-transfer-progress

# ---------- Quarkus dev settings ----------
QUARKUS_PROFILE  ?= dev
QUARKUS_DEV_ARGS ?=
QUARKUS_DEV_GOAL := io.quarkus:quarkus-maven-plugin:${quarkus.platform.version}:dev

# ---------- Dev dirs ----------
PID_DIR := .devpids
LOG_DIR := .devlogs
BIN_DIR := .devbin
$(shell mkdir -p $(PID_DIR) $(LOG_DIR) $(BIN_DIR) >/dev/null)

# ---------- Reactor shorthands ----------
REACTOR_SERVICE := -pl service -am

# ---------- Version / Artifacts ----------
VERSION := $(shell sed -n 's:.*<version>\(.*\)</version>.*:\1:p' pom.xml | head -n1)
ifeq ($(strip $(VERSION)),)
  VERSION := 0.1.0-SNAPSHOT
endif
PROTO_JAR := proto/target/metacat-proto-$(VERSION).jar

# ---------- CLI isolation toggle ----------
CLI_ISOLATED ?= 1
M2_CLI_DIR   := $(BIN_DIR)/.m2-cli
ifeq ($(CLI_ISOLATED),1)
  CLI_M2 := -Dmaven.repo.local=$(M2_CLI_DIR)
else
  CLI_M2 :=
endif

PARENT_STAMP := $(M2_CLI_DIR)/.parent-$(VERSION).stamp
PROTO_STAMP  := $(M2_CLI_DIR)/.proto-$(VERSION).stamp

# ---------- CLI outputs & inputs ----------
CLI_JAR := client-cli/target/quarkus-app/quarkus-run.jar
CLI_SRC := $(shell find client-cli/src -type f \( -name '*.java' -o -name '*.xml' -o -name '*.properties' -o -name '*.yaml' -o -name '*.yml' -o -name '*.json' \) ) client-cli/pom.xml

# ===================================================
# Aggregates
# ===================================================
.PHONY: all build build-all
all: build
build: proto build-all

build-all:
	@echo "==> [BUILD] all modules"
	$(MVN) $(MVN_FLAGS) package

# ===================================================
# Proto
# ===================================================
.PHONY: proto
proto: $(PROTO_JAR)

$(PROTO_JAR): proto/pom.xml $(shell find proto -type f -name '*.proto' -o -name 'pom.xml')
	@echo "==> [PROTO] package generated stubs ($(VERSION))"
	$(MVN) -q -f proto/pom.xml -DskipTests package
	@test -f "$@" || { echo "ERROR: expected $@ not found"; exit 1; }

# ===================================================
# Tests
# ===================================================
.PHONY: test unit-test integration-test verify
test: cli-test
	@echo "==> [BUILD] installing parent POM to local repo"
	$(MVN) $(MVN_TESTALL) install -N
	@echo "==> [TEST] service module"
	$(MVN) $(MVN_TESTALL) -pl service -am verify
	@echo "==> [TEST] client-cli module"
	@cd client-cli && $(MVN) $(MVN_TESTALL) test

unit-test:
	$(MVN) $(MVN_TESTALL) -pl service -am -DskipITs=true test

integration-test:
	$(MVN) $(MVN_TESTALL) -pl service -am -DskipUTs=true -DfailIfNoTests=false verify

verify:
	$(MVN) $(MVN_TESTALL) -pl service,client-cli -am verify

# ===================================================
# Clean
# ===================================================
.PHONY: clean clean-java clean-dev
clean: clean-java clean-dev

clean-java:
	$(MVN) -q -T 1C clean || true

clean-dev:
	@echo "==> [CLEAN-DEV] removing pid/logs and isolated repos"
	rm -rf $(PID_DIR) $(LOG_DIR) $(BIN_DIR)
	mkdir -p $(PID_DIR) $(LOG_DIR) $(BIN_DIR)

# ===================================================
# Dev (foreground)
# ===================================================
.PHONY: run
run: $(PROTO_JAR)
	@echo "==> [DEV] quarkus:dev (profile=$(QUARKUS_PROFILE))"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_SERVICE) \
	  $(QUARKUS_DEV_GOAL)

# ===================================================
# Dev (background)
# ===================================================
SERVICE_NAME := service
PID_FILE := $(PID_DIR)/$(SERVICE_NAME).pid
LOG_FILE := $(LOG_DIR)/$(SERVICE_NAME).log

define _bg_and_pid
( \
  set -m; \
  nohup bash -lc '$(MVN) -f ./pom.xml -Dquarkus.profile=$(QUARKUS_PROFILE) $(QUARKUS_DEV_ARGS) $(REACTOR_SERVICE) $(QUARKUS_DEV_GOAL)' \
    >> "$(LOG_FILE)" 2>&1 & \
  echo $$! > "$(PID_FILE)"; \
)
endef

.PHONY: start
start: $(PROTO_JAR)
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

# ===================================================
# CLI
# ===================================================

.PHONY: cli
cli:
	@mvn -q -f pom.xml -pl client-cli -am \
	  -Dquarkus.package.jar.type=fast-jar \
	  -DskipTests -DskipITs=true \
	  package

.PHONY: cli-run
cli-run: cli
	@echo "==> [RUN] client CLI"
	@java -jar $(CLI_JAR) $(ARGS)

.PHONY: cli-test
cli-test:
	@mvn -q -f pom.xml -pl client-cli -am  test

# ===================================================
# Docker (Quarkus container-image)
# ===================================================
.PHONY: docker
docker:
	@echo "==> [DOCKER] quarkus container-image build"
	$(MVN) -f service/pom.xml -DskipTests -Dquarkus.container-image.build=true package

# ===================================================
# Lint/format
# ===================================================
.PHONY: fmt
fmt:
	@echo "==> [FMT] (google-java-format)"
	$(MVN) -q fmt:format

# ===================================================
# Help
# ===================================================
.PHONY: help
help:
	@awk 'BEGIN {FS":.*?#"} /^[a-zA-Z0-9._%-]+:.*?#/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
