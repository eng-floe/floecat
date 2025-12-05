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
MVN_FLAGS   := -q -T 1C --no-transfer-progress -DskipTests -DskipUTs=true -DskipITs=true
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
REACTOR_REST := -pl protocol-gateway/iceberg-rest -am

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
	$(MVN) -q -f proto/pom.xml -DskipTests install
	@test -f "$@" || { echo "ERROR: expected $@ not found"; exit 1; }

# ===================================================
# Tests
# ===================================================
.PHONY: test unit-test integration-test verify

test: $(PROTO_JAR)
	@echo "==> [BUILD] installing parent POM to local repo"
	$(MVN) $(MVN_TESTALL) install -N
	@echo "==> [TEST] service + REST gateway + client-cli (unit + IT)"
	$(MVN) $(MVN_TESTALL) \
	  -pl service,protocol-gateway/iceberg-rest,client-cli -am \
	  verify

unit-test:
	@echo "==> [TEST] unit tests (service, REST gateway, client-cli)"
	$(MVN) $(MVN_TESTALL) \
	  -pl service,protocol-gateway/iceberg-rest,client-cli -am \
	  -DskipITs=true \
	  test

integration-test:
	@echo "==> [TEST] integration tests (service, REST gateway, client-cli)"
	$(MVN) $(MVN_TESTALL) \
	  -pl service,protocol-gateway/iceberg-rest,client-cli -am \
	  -DskipUTs=true -DfailIfNoTests=false \
	  verify

verify:
	@echo "==> [VERIFY] full lifecycle (service, REST gateway, client-cli)"
	$(MVN) $(MVN_TESTALL) \
	  -pl service,protocol-gateway/iceberg-rest,client-cli -am \
	  verify

# ===================================================
# Trino connector (requires Java 21)
# ===================================================
.PHONY: trino-connector trino-test
trino-connector: proto
	@echo "==> [TRINO] package connector with Java 21/proto rebuild"
	$(MVN) $(MVN_FLAGS) -Pwith-trino -pl connectors/clients/trino -am generate-sources
	$(MVN) $(MVN_FLAGS) -Pwith-trino -pl connectors/clients/trino -am package

trino-test: proto
	@echo "==> [TRINO] test connector only"
	$(MVN) $(MVN_TESTALL) -Pwith-trino -pl connectors/clients/trino -am generate-sources
	$(MVN) $(MVN_TESTALL) -Pwith-trino -pl connectors/clients/trino -am test

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
.PHONY: run run-service run-all
run: run-service

run-service: $(PROTO_JAR)
	@echo "==> [DEV] quarkus:dev (profile=$(QUARKUS_PROFILE))"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_SERVICE) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: run-all
run-all: start-rest run-service

# ===================================================
# Dev (background)
# ===================================================
SERVICE_NAME := service
SERVICE_PID := $(PID_DIR)/$(SERVICE_NAME).pid
SERVICE_LOG := $(LOG_DIR)/$(SERVICE_NAME).log

REST_NAME := iceberg-rest
REST_PID := $(PID_DIR)/$(REST_NAME).pid
REST_LOG := $(LOG_DIR)/$(REST_NAME).log

define _bg_and_pid
( \
  set -m; \
  nohup bash -lc '$(MVN) -f ./pom.xml -Dquarkus.profile=$(QUARKUS_PROFILE) $(QUARKUS_DEV_ARGS) $(1) $(QUARKUS_DEV_GOAL)' \
    >> "$(2)" 2>&1 & \
  echo $$! > "$(3)"; \
)
endef

.PHONY: run-rest
run-rest:
	@echo "==> [DEV] quarkus:dev (REST gateway)"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_REST) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: start-service start-rest start start-all
start: start-service
start-all: start-service start-rest

start-service: $(PROTO_JAR)
	@if [ -f "$(SERVICE_PID)" ] && ps -p $$(cat "$(SERVICE_PID)") >/dev/null 2>&1; then \
	  echo "==> [DEV] service already running (pid $$(cat $(SERVICE_PID)))"; \
	else \
	  echo "==> [DEV] starting service in background (profile=$(QUARKUS_PROFILE))"; \
	  $(call _bg_and_pid,$(REACTOR_SERVICE),$(SERVICE_LOG),$(SERVICE_PID)); \
	  sleep 1; \
	  echo "==> [DEV] service pid $$(cat $(SERVICE_PID)) | logs -> $(SERVICE_LOG)"; \
	fi

.PHONY: start-rest
start-rest: $(PROTO_JAR)
	@if [ -f "$(REST_PID)" ] && ps -p $$(cat "$(REST_PID)") >/dev/null 2>&1; then \
	  echo "==> [DEV] REST gateway already running (pid $$(cat $(REST_PID)))"; \
	else \
	  echo "==> [DEV] starting REST gateway in background (profile=$(QUARKUS_PROFILE))"; \
	  $(call _bg_and_pid,$(REACTOR_REST),$(REST_LOG),$(REST_PID)); \
	  sleep 1; \
	  echo "==> [DEV] REST pid $$(cat $(REST_PID)) | logs -> $(REST_LOG)"; \
	fi

.PHONY: stop stop-service stop-rest
stop: stop-service stop-rest

stop-service:
	@if [ -f "$(SERVICE_PID)" ]; then \
	  PID=$$(cat "$(SERVICE_PID)"); \
	  if ps -p $$PID >/dev/null 2>&1; then \
	    echo "==> [DEV] stopping service pid $$PID"; \
	    kill $$PID || true; \
	  else \
	    echo "==> [DEV] stale service PID file"; \
	  fi; \
	  rm -f "$(SERVICE_PID)"; \
	else \
	  echo "==> [DEV] service not running"; \
	fi

.PHONY: stop-rest
stop-rest:
	@if [ -f "$(REST_PID)" ]; then \
	  PID=$$(cat "$(REST_PID)"); \
	  if ps -p $$PID >/dev/null 2>&1; then \
	    echo "==> [DEV] stopping REST pid $$PID"; \
	    kill $$PID || true; \
	  else \
	    echo "==> [DEV] stale REST PID file"; \
	  fi; \
	  rm -f "$(REST_PID)"; \
	else \
	  echo "==> [DEV] REST gateway not running"; \
	fi

.PHONY: logs logs-rest
logs:
	@if [ -f "$(SERVICE_LOG)" ]; then \
	  echo "==> [LOGS] tail -f $(SERVICE_LOG)"; \
	  tail -f "$(SERVICE_LOG)"; \
	else \
	  echo "==> [LOGS] no service log yet"; \
	fi

logs-rest:
	@if [ -f "$(REST_LOG)" ]; then \
	  echo "==> [LOGS] tail -f $(REST_LOG)"; \
	  tail -f "$(REST_LOG)"; \
	else \
	  echo "==> [LOGS] no REST log yet"; \
	fi

.PHONY: status
status:
	@if [ -f "$(SERVICE_PID)" ] && ps -p $$(cat "$(SERVICE_PID)") >/dev/null 2>&1; then \
	  echo "==> [STATUS] service running (pid $$(cat $(SERVICE_PID)))"; \
	else \
	  echo "==> [STATUS] service not running"; \
	fi
	@if [ -f "$(REST_PID)" ] && ps -p $$(cat "$(REST_PID)") >/dev/null 2>&1; then \
	  echo "==> [STATUS] REST gateway running (pid $$(cat $(REST_PID)))"; \
	else \
	  echo "==> [STATUS] REST gateway not running"; \
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
cli-test: $(PROTO_JAR)
	@mvn -q -f pom.xml -pl client-cli -am test

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
