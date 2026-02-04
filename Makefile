# Copyright 2026 Yellowbrick Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -------- Floecat Makefile (Quarkus + gRPC) --------
# Quick refs – build & tests:
#   make / make build            # build proto + all modules (skip tests)
#   make build-all               # build all modules only (skip tests)
#   make proto                   # generate/install protobuf stubs
#   make test                    # unit + IT (service, REST gateway, client-cli, in-memory)
#   make test-localstack          # unit + IT (upstream + catalog LocalStack)
#   make unit-test               # unit tests only (service, REST gateway, client-cli)
#   make integration-test        # integration tests only (service, REST gateway, client-cli)
#   make verify                  # full Maven verify lifecycle
#
# Dev – foreground & background:
#   make run                     # quarkus:dev for service (foreground)
#   make run-aws-aws              # upstream real AWS -> catalog real AWS
#   make run-localstack-aws       # upstream Localstack -> catalog real AWS
#   make run-aws-localstack       # upstream real AWS -> catalog Localstack
#   make run-localstack-localstack # upstream Localstack -> catalog Localstack
#   make run-rest                # quarkus:dev for REST gateway (foreground, in-memory)
#   make run-rest-aws             # REST gateway upstream real AWS
#   make run-rest-localstack      # REST gateway upstream LocalStack
#   make run-all                 # start REST (bg), then run service (fg)
#   make start                   # start service in background
#   make start-rest              # start REST gateway in background
#   make start-all               # start service + REST gateway in background
#   make stop                    # stop service + REST gateway
#   make logs                    # tail -f service log
#   make localstack-up           # start LocalStack container
#   make localstack-down         # stop LocalStack container (if running)
#   make keycloak-up            # start Keycloak container
#   make keycloak-down          # stop Keycloak container (if running)
#   make logs-rest               # tail -f REST gateway log
#   make status                  # show background dev status
#
# Connectors:
#   make trino-connector         # build Trino connector (Java 21, with-trino profile)
#   make trino-test              # test Trino connector only
#
# CLI:
#   make cli                     # build client CLI (fast-jar, skip tests)
#   make cli-run                 # run client CLI (use ARGS=...)
#   make cli-docker              # run client CLI in docker network (OIDC-friendly)
#   make cli-docker-token        # print OIDC token for docker network issuer
#   make oidc-up                 # start docker stack with Keycloak + OIDC env
#   make oidc-down               # stop docker stack started by oidc-up
#   make cli-test                # run CLI tests
#
# Utilities:
#   make clean                   # mvn clean + remove dev dirs
#   make clean-java              # mvn clean only
#   make clean-dev               # remove dev pids/logs/isolated repos
#   make docker                  # build service container image
#   make fmt                     # format Java sources (google-java-format)
#   make help                    # show target list from this Makefile
#
# Examples:
#   make MVN=./mvnw run
#   make CLI_ISOLATED=0 cli-run
#   make ARGS="catalog list" cli-run
#   make QUARKUS_PROFILE=test run-service
.SHELLFLAGS := -eo pipefail -c
SHELL       := bash
MAKEFLAGS  += --no-builtin-rules
.ONESHELL:
.DEFAULT_GOAL := build

MVN ?= mvn
MVN_FLAGS   := -q -T 1C --no-transfer-progress -DskipTests -DskipUTs=true -DskipITs=true
MVN_TESTALL := --no-transfer-progress

DOCKER_COMPOSE ?= docker compose
DOCKER_COMPOSE_MAIN ?= $(DOCKER_COMPOSE) -f docker/docker-compose.yml
DOCKER_COMPOSE_LOCALSTACK ?= $(DOCKER_COMPOSE) -f $(LOCALSTACK_COMPOSE)
DOCKER_COMPOSE_KEYCLOAK ?= $(DOCKER_COMPOSE) -f docker/docker-compose.yml --profile keycloak
KEYCLOAK_PORT ?= 12221
KEYCLOAK_ENDPOINT ?= http://127.0.0.1:$(KEYCLOAK_PORT)
KEYCLOAK_HEALTH := $(KEYCLOAK_ENDPOINT)/realms/floecat/.well-known/openid-configuration
JIB_PLATFORMS ?=
JIB_BASE_IMAGE ?= eclipse-temurin:25-jre
UNAME_M := $(shell uname -m)

ifeq ($(strip $(JIB_PLATFORMS)),)
  ifeq ($(UNAME_M),arm64)
    JIB_PLATFORMS := linux/arm64
  else ifeq ($(UNAME_M),aarch64)
    JIB_PLATFORMS := linux/arm64
  else
    JIB_PLATFORMS := linux/amd64
  endif
endif

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
REPO_DIR := ~/.m2/repository/ai/floedb/floecat
PROTO_JAR := $(REPO_DIR)/floecat-proto/$(VERSION)/floecat-proto-$(VERSION).jar
TEST_SUPPORT_JAR := $(REPO_DIR)/floecat-storage-spi/$(VERSION)/floecat-storage-spi-$(VERSION)-tests.jar

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

APP_NAME     := floedb-floecat

# ---------- Version bump ----------
.PHONY: bump-version
bump-version:
	@test -n "$(VERSION)" || (echo "VERSION is required, e.g. make bump-version VERSION=0.1.1-SNAPSHOT" && exit 1)
	mvn -q versions:set -DnewVersion=$(VERSION) -DgenerateBackupPoms=false

# ---------- CLI outputs & inputs ----------
CLI_JAR := client-cli/target/quarkus-app/quarkus-run.jar
CLI_SRC := $(shell find client-cli/src -type f \( -name '*.java' -o -name '*.xml' -o -name '*.properties' -o -name '*.yaml' -o -name '*.yml' -o -name '*.json' \) ) client-cli/pom.xml

# ---------- LocalStack / AWS storage ----------
LOCALSTACK_DIR := tools/localstack
LOCALSTACK_COMPOSE := $(LOCALSTACK_DIR)/docker-compose.yml
LOCALSTACK_PROJECT := floecat-localstack
LOCALSTACK_ENDPOINT ?= http://localhost:4566
LOCALSTACK_HEALTH := $(LOCALSTACK_ENDPOINT)/_localstack/health
LOCALSTACK_BUCKET ?= floecat-dev
LOCALSTACK_TABLE ?= floecat_pointers
LOCALSTACK_REGION ?= us-east-1
LOCALSTACK_ACCESS_KEY ?= test
LOCALSTACK_SECRET_KEY ?= test
LOCALSTACK_ENV := \
	AWS_REGION=$(LOCALSTACK_REGION) \
	AWS_DEFAULT_REGION=$(LOCALSTACK_REGION) \
	AWS_ACCESS_KEY_ID=$(LOCALSTACK_ACCESS_KEY) \
	AWS_SECRET_ACCESS_KEY=$(LOCALSTACK_SECRET_KEY) \
	AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED \
	AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED

REAL_AWS_BUCKET ?=
REAL_AWS_TABLE ?=
REAL_AWS_REGION ?= us-east-1

LOCALSTACK_S3_OVERRIDES := \
	-Dfloecat.fileio.override.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
	-Dfloecat.fileio.override.s3.endpoint=$(LOCALSTACK_ENDPOINT) \
	-Dfloecat.fileio.override.s3.region=$(LOCALSTACK_REGION) \
	-Dfloecat.fileio.override.s3.access-key-id=$(LOCALSTACK_ACCESS_KEY) \
	-Dfloecat.fileio.override.s3.secret-access-key=$(LOCALSTACK_SECRET_KEY) \
	-Dfloecat.fileio.override.s3.path-style-access=true

CATALOG_LOCALSTACK_PROPS := \
	-Dfloecat.kv=dynamodb \
	-Dfloecat.kv.table=$(LOCALSTACK_TABLE) \
	-Dfloecat.kv.auto-create=true \
	-Dfloecat.kv.ttl-enabled=true \
	-Dfloecat.blob=s3 \
	-Dfloecat.blob.s3.bucket=$(LOCALSTACK_BUCKET) \
	$(LOCALSTACK_S3_OVERRIDES) \
	-Dfloecat.fileio.override.aws.dynamodb.endpoint-override=$(LOCALSTACK_ENDPOINT) \
	-Dfloecat.fixtures.use-aws-s3=true \
	-Daws.requestChecksumCalculation=when_required \
	-Daws.responseChecksumValidation=when_required

UPSTREAM_LOCALSTACK_PROPS := $(LOCALSTACK_S3_OVERRIDES)

CATALOG_REAL_AWS_PROPS := \
	-Dfloecat.kv=dynamodb \
	-Dfloecat.kv.table=$(REAL_AWS_TABLE) \
	-Dfloecat.kv.auto-create=true \
	-Dfloecat.kv.ttl-enabled=true \
	-Dfloecat.blob=s3 \
	-Dfloecat.blob.s3.bucket=$(REAL_AWS_BUCKET) \
	-Dfloecat.fixtures.use-aws-s3=true

UPSTREAM_REAL_AWS_PROPS :=

AWS_STORE_PROPS := $(CATALOG_LOCALSTACK_PROPS) $(UPSTREAM_LOCALSTACK_PROPS)

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

# ===================================================
# Test Support
# ===================================================
.PHONY: test-support
test-support: $(TEST_SUPPORT_JAR)

# ===================================================
# JAR Dependencies
# ===================================================
.PHONY: jar-dependencies
jar-dependencies: proto test-support

$(PROTO_JAR): core/proto/pom.xml $(shell find core/proto -type f -name '*.proto' -o -name 'pom.xml')
	@echo "==> [PROTO] package generated stubs ($(VERSION))"
	$(MVN) -q -f core/proto/pom.xml -DskipTests install
	@test -f "$@" || { echo "ERROR: expected $@ not found"; exit 1; }

$(TEST_SUPPORT_JAR): $(shell find core/storage-spi/src/test -type f -name '*.java' -o -name '*.properties')
	@echo "==> [BUILD] storage-spi test-jar (for test scope deps)"
	$(MVN) -q -f ./pom.xml -DskipTests -DskipUTs=true -DskipITs=true -pl core/storage-spi -am install

# ===================================================
# Tests
# - test: in-memory stores (fast default)
# - test-localstack: upstream + catalog LocalStack
# ===================================================
.PHONY: test test-localstack unit-test integration-test verify

test: $(PROTO_JAR) keycloak-up
	@echo "==> [BUILD] installing parent POM to local repo"
	$(MVN) $(MVN_TESTALL) install -N
	@echo "==> [TEST] service + REST gateway + client-cli (unit + IT, in-memory)"
	$(MVN) $(MVN_TESTALL) \
	  -pl service,protocol-gateway/iceberg-rest,client-cli -am \
	  verify

.PHONY: test-localstack
test-localstack: $(PROTO_JAR) localstack-down localstack-up keycloak-up
	@echo "==> [BUILD] installing parent POM to local repo"
	$(MVN) $(MVN_TESTALL) install -N
	@echo "==> [TEST] full suite (service + REST + CLI) upstream LocalStack -> catalog LocalStack"
	$(LOCALSTACK_ENV) \
	$(MVN) $(MVN_TESTALL) $(CATALOG_LOCALSTACK_PROPS) $(UPSTREAM_LOCALSTACK_PROPS) \
	  -pl service,protocol-gateway/iceberg-rest,client-cli -am \
	  verify

.PHONY: localstack-restart
localstack-restart: localstack-down localstack-up

.PHONY: keycloak-up keycloak-down keycloak-restart
keycloak-up:
	@echo "==> [KEYCLOAK] starting docker compose (keycloak profile)"
	KEYCLOAK_PORT=$(KEYCLOAK_PORT) $(DOCKER_COMPOSE_KEYCLOAK) up -d keycloak
	@echo "==> [KEYCLOAK] waiting for realm readiness"
	@bash -c 'set -euo pipefail; \
	for i in $$(seq 1 45); do \
	  if curl -fs $(KEYCLOAK_ENDPOINT)/ >/dev/null 2>&1; then break; fi; \
	  sleep 1; \
	done; \
	for i in $$(seq 1 45); do \
	  if curl -fs $(KEYCLOAK_HEALTH) | grep -q "\"issuer\""; then exit 0; fi; \
	  sleep 1; \
	done; \
	echo "Keycloak failed to start at $(KEYCLOAK_ENDPOINT)" >&2; \
	exit 1'
	@echo "Keycloak running at $(KEYCLOAK_ENDPOINT)"

keycloak-down:
	@echo "==> [KEYCLOAK] stopping docker compose (keycloak profile)"
	$(DOCKER_COMPOSE_KEYCLOAK) down --remove-orphans

keycloak-restart: keycloak-down keycloak-up

unit-test:
	@echo "==> [TEST] unit tests (service, REST gateway, client-cli)"
	$(MVN) $(MVN_TESTALL) \
	  -pl service,protocol-gateway/iceberg-rest,client-cli -am \
	  -DskipITs=true \
	  test

integration-test: keycloak-up
	@echo "==> [TEST] integration tests (service, REST gateway, client-cli)"
	$(MVN) $(MVN_TESTALL) \
	  -pl service,protocol-gateway/iceberg-rest,client-cli -am \
	  -DskipUTs=true -DfailIfNoTests=false \
	  verify

verify: keycloak-up
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

run-service: jar-dependencies
	@echo "==> [DEV] quarkus:dev (profile=$(QUARKUS_PROFILE))"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  -Dfloecat.seed.enabled=true \
	  -Dfloecat.seed.mode=iceberg \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_SERVICE) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: run-aws-aws
run-aws-aws: jar-dependencies
	@if [ -z "$(REAL_AWS_BUCKET)" ] || [ -z "$(REAL_AWS_TABLE)" ]; then \
	  echo "ERROR: REAL_AWS_BUCKET and REAL_AWS_TABLE must be set"; \
	  exit 1; \
	fi
	@echo "==> [DEV] quarkus:dev upstream real AWS -> catalog real AWS"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  -Dfloecat.seed.enabled=true \
	  -Dfloecat.seed.mode=iceberg \
	  $(CATALOG_REAL_AWS_PROPS) $(UPSTREAM_REAL_AWS_PROPS) \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_SERVICE) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: run-localstack-aws
run-localstack-aws: localstack-up jar-dependencies
	@if [ -z "$(REAL_AWS_BUCKET)" ] || [ -z "$(REAL_AWS_TABLE)" ]; then \
	  echo "ERROR: REAL_AWS_BUCKET and REAL_AWS_TABLE must be set"; \
	  exit 1; \
	fi
	@echo "==> [DEV] quarkus:dev upstream LocalStack -> catalog real AWS"
	$(LOCALSTACK_ENV) \
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  -Dfloecat.seed.enabled=true \
	  -Dfloecat.seed.mode=iceberg \
	  $(CATALOG_REAL_AWS_PROPS) $(UPSTREAM_LOCALSTACK_PROPS) \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_SERVICE) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: run-aws-localstack
run-aws-localstack: localstack-up jar-dependencies
	@echo "==> [DEV] quarkus:dev upstream real AWS -> catalog LocalStack"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  -Dfloecat.connector.fileio.overrides=false \
	  -Dfloecat.seed.enabled=true \
	  -Dfloecat.seed.mode=iceberg \
	  $(CATALOG_LOCALSTACK_PROPS) $(UPSTREAM_REAL_AWS_PROPS) \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_SERVICE) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: run-localstack-localstack
run-localstack-localstack: localstack-up jar-dependencies
	@echo "==> [DEV] quarkus:dev upstream LocalStack -> catalog LocalStack"
	$(LOCALSTACK_ENV) \
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  -Dfloecat.seed.enabled=true \
	  -Dfloecat.seed.mode=iceberg \
	  $(CATALOG_LOCALSTACK_PROPS) $(UPSTREAM_LOCALSTACK_PROPS) \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_SERVICE) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: run-all
run-all: start-rest run-service

.PHONY: dev-start dev-stop stop-service
dev-start: localstack-up jar-dependencies
	$(LOCALSTACK_ENV) \
	$(MVN) -f ./pom.xml \
	  -Dapplication.name=$(APP_NAME) \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  -Dfloecat.seed.enabled=true \
	  -Dfloecat.seed.mode=iceberg \
	  $(CATALOG_LOCALSTACK_PROPS) $(UPSTREAM_LOCALSTACK_PROPS) \
	  $(QUARKUS_DEV_ARGS) \
	  --projects service \
	  $(QUARKUS_DEV_GOAL) &
dev-stop: stop-service localstack-down

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
	@echo "==> [DEV] quarkus:dev (REST gateway, in-memory)"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_REST) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: run-rest-localstack
run-rest-localstack: localstack-up $(PROTO_JAR)
	@echo "==> [DEV] quarkus:dev REST gateway upstream LocalStack"
	$(LOCALSTACK_ENV) \
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  $(UPSTREAM_LOCALSTACK_PROPS) \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_REST) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: run-rest-aws
run-rest-aws: $(PROTO_JAR)
	@echo "==> [DEV] quarkus:dev REST gateway upstream real AWS"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  $(UPSTREAM_REAL_AWS_PROPS) \
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
	  pkill -f "application.name=$(APP_NAME)" || echo "==> [DEV] service not running"; \
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

.PHONY: localstack-up
localstack-up:
	@if curl -fs $(LOCALSTACK_HEALTH) >/dev/null 2>&1; then \
	  echo "==> [LOCALSTACK] already running at $(LOCALSTACK_ENDPOINT)"; \
	else \
	  echo "==> [LOCALSTACK] starting docker compose ($(LOCALSTACK_COMPOSE))"; \
	  $(DOCKER_COMPOSE_LOCALSTACK) -p $(LOCALSTACK_PROJECT) up -d; \
	  echo "==> [LOCALSTACK] waiting for health endpoint"; \
	  bash -c 'set -euo pipefail; for i in $$(seq 1 30); do \
	    if curl -fs $(LOCALSTACK_HEALTH) >/dev/null 2>&1; then exit 0; fi; \
	    sleep 1; \
	  done; \
	  echo "LocalStack failed to start at $(LOCALSTACK_ENDPOINT)" >&2; \
	  exit 1'; \
	fi
	@echo "==> [LOCALSTACK] ensuring S3 bucket $(LOCALSTACK_BUCKET) exists"
	@$(DOCKER_COMPOSE_LOCALSTACK) -p $(LOCALSTACK_PROJECT) exec -T localstack \
	  awslocal s3api create-bucket \
	    --bucket $(LOCALSTACK_BUCKET) \
	    --region $(LOCALSTACK_REGION) >/dev/null 2>&1 || true
	@echo "==> [LOCALSTACK] ensuring S3 bucket bucket exists"
	@$(DOCKER_COMPOSE_LOCALSTACK) -p $(LOCALSTACK_PROJECT) exec -T localstack \
	  awslocal s3api create-bucket \
	    --bucket bucket \
	    --region $(LOCALSTACK_REGION) >/dev/null 2>&1 || true
	@echo "==> [LOCALSTACK] ensuring S3 bucket warehouse exists"
	@$(DOCKER_COMPOSE_LOCALSTACK) -p $(LOCALSTACK_PROJECT) exec -T localstack \
	  awslocal s3api create-bucket \
	    --bucket warehouse \
	    --region $(LOCALSTACK_REGION) >/dev/null 2>&1 || true
	@echo "==> [LOCALSTACK] ensuring DynamoDB table $(LOCALSTACK_TABLE) exists"
	@$(DOCKER_COMPOSE_LOCALSTACK) -p $(LOCALSTACK_PROJECT) exec -T localstack \
	  awslocal dynamodb create-table \
	    --table-name $(LOCALSTACK_TABLE) \
	    --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
	    --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
	    --billing-mode PAY_PER_REQUEST >/dev/null 2>&1 || true
	@$(DOCKER_COMPOSE_LOCALSTACK) -p $(LOCALSTACK_PROJECT) exec -T localstack \
	  awslocal dynamodb update-time-to-live \
	    --table-name $(LOCALSTACK_TABLE) \
	    --time-to-live-specification '{"Enabled":true,"AttributeName":"expires_at"}' >/dev/null 2>&1 || true

.PHONY: localstack-down
localstack-down:
	@if curl -fs $(LOCALSTACK_HEALTH) >/dev/null 2>&1; then \
	  echo "==> [LOCALSTACK] stopping docker compose ($(LOCALSTACK_PROJECT))"; \
	  $(DOCKER_COMPOSE_LOCALSTACK) -p $(LOCALSTACK_PROJECT) down; \
	else \
	  echo "==> [LOCALSTACK] not running"; \
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

.PHONY: cli-docker-token
cli-docker-token:
	@echo "==> [TOKEN] client credentials via docker network"
	@docker run --rm --network docker_floecat curlimages/curl:8.11.1 -s \
	  -d "client_id=floecat-client" \
	  -d "client_secret=floecat-secret" \
	  -d "grant_type=client_credentials" \
	  http://host.docker.internal:12221/realms/floecat/protocol/openid-connect/token | jq -r .access_token

.PHONY: cli-docker
cli-docker:
	@echo "==> [RUN] client CLI (docker)"
	@TOKEN=$$(docker run --rm --network docker_floecat curlimages/curl:8.11.1 -s \
	  -d "client_id=floecat-client" \
	  -d "client_secret=floecat-secret" \
	  -d "grant_type=client_credentials" \
	  http://host.docker.internal:12221/realms/floecat/protocol/openid-connect/token | jq -r .access_token); \
	FLOECAT_ENV_FILE=./env.localstack \
	$(DOCKER_COMPOSE_MAIN) run --rm \
	  -e FLOECAT_TOKEN=$$TOKEN \
	  -e FLOECAT_ACCOUNT=$$FLOECAT_ACCOUNT \
	  cli

.PHONY: oidc-up
oidc-up:
	@echo "==> [DOCKER] starting stack with Keycloak + OIDC env"
	@FLOECAT_ENV_FILE=./env.localstack \
	  $(DOCKER_COMPOSE_MAIN) --profile keycloak --profile localstack up -d

.PHONY: oidc-down
oidc-down:
	@echo "==> [DOCKER] stopping stack with Keycloak + OIDC env"
	@FLOECAT_ENV_FILE=./env.localstack \
	  $(DOCKER_COMPOSE_MAIN) --profile keycloak --profile localstack down --remove-orphans

.PHONY: cli-test
cli-test: $(PROTO_JAR)
	@mvn -q -f pom.xml -pl client-cli -am test

# ===================================================
# Docker (Quarkus container-image)
# ===================================================
.PHONY: docker docker-service docker-iceberg-rest docker-cli compose-up compose-down compose-shell
docker: docker-service docker-iceberg-rest docker-cli

docker-service:
	@echo "==> [DOCKER] service (jib -> docker daemon)"
	$(MVN) -f ./pom.xml -pl service -am -DskipTests -Dmaven.test.skip=true \
	  -DskipUTs=true -DskipITs=true \
	  -Dquarkus.container-image.build=true \
	  -Dquarkus.jib.base-jvm-image=$(JIB_BASE_IMAGE) \
	  $(if $(JIB_PLATFORMS),-Dquarkus.jib.platforms=$(JIB_PLATFORMS)) \
	  -Dquarkus.container-image.image=floecat-service:local \
	  package

docker-iceberg-rest:
	@echo "==> [DOCKER] iceberg-rest (jib -> docker daemon)"
	$(MVN) -f ./pom.xml -pl protocol-gateway/iceberg-rest -am -DskipTests -Dmaven.test.skip=true \
	  -DskipUTs=true -DskipITs=true \
	  -Dquarkus.container-image.build=true \
	  -Dquarkus.jib.base-jvm-image=$(JIB_BASE_IMAGE) \
	  $(if $(JIB_PLATFORMS),-Dquarkus.jib.platforms=$(JIB_PLATFORMS)) \
	  -Dquarkus.container-image.image=floecat-iceberg-rest:local \
	  package

docker-cli:
	@echo "==> [DOCKER] cli (jib -> docker daemon)"
	$(MVN) -f ./pom.xml -pl client-cli -am -DskipTests -Dmaven.test.skip=true \
	  -DskipUTs=true -DskipITs=true \
	  -Dquarkus.container-image.build=true \
	  -Dquarkus.jib.base-jvm-image=$(JIB_BASE_IMAGE) \
	  $(if $(JIB_PLATFORMS),-Dquarkus.jib.platforms=$(JIB_PLATFORMS)) \
	  -Dquarkus.container-image.image=floecat-cli:local \
	  package

compose-up: docker
	@echo "==> [COMPOSE] up"
	$(DOCKER_COMPOSE_MAIN) up -d

compose-down:
	@echo "==> [COMPOSE] down"
	$(DOCKER_COMPOSE_MAIN) down

compose-shell:
	@echo "==> [COMPOSE] shell"
	COMPOSE_PROFILES=cli $(DOCKER_COMPOSE_MAIN) run --rm cli

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
