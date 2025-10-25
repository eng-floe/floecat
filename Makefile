# -------- Metacat Makefile (Quarkus + gRPC) --------
# Quick refs:
#   make / make build           # build proto + service (skip tests)
#   make test                   # run all tests
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

# ---------- Modules ----------
JAVA_MODULES   := proto service reconciler connectors-spi connectors-iceberg
JAVA_SERVICES  := service               # runnable module(s)

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
build: proto build-service

.PHONY: proto
proto:
	@echo "==> [PROTO] package generated stubs"
	$(MVN) -q -f proto/pom.xml -DskipTests package

.PHONY: build-service
build-service:
	@echo "==> [SERVICE] package"
	$(MVN) -q -DskipTests -pl service -am package

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
	@echo "==> [CLEAN-DEV] removing pid/logs"
	rm -rf $(PID_DIR) $(LOG_DIR) $(BIN_DIR)
	mkdir -p $(PID_DIR) $(LOG_DIR) $(BIN_DIR)

# ---------- Dev (foreground) ----------
# Runs Quarkus dev in the foreground (CTRL-C to stop).
.PHONY: run
run:
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

