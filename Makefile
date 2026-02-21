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
#   make test-localstack          # unit + IT (fixtures + catalog LocalStack)
#   make unit-test               # unit tests only (service, REST gateway, client-cli)
#   make integration-test        # integration tests only (service, REST gateway, client-cli)
#   make verify                  # full Maven verify lifecycle
#
# Dev – foreground & background:
#   make run                     # quarkus:dev for service (foreground, always seeded fake data)
#   make run-aws REAL_AWS_BUCKET=<bucket> REAL_AWS_TABLE=<table> [SEED_ENABLED=true SEED_SOURCE=aws|localstack] # catalog real AWS
#   make run-localstack [SEED_ENABLED=true SEED_SOURCE=aws|localstack] # catalog LocalStack
#   make run-rest                # quarkus:dev for REST gateway (foreground, in-memory)
#   make run-rest-aws             # REST gateway with AWS-style storage credentials
#   make run-rest-localstack      # REST gateway with LocalStack-style storage credentials
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
#   make compose-up             # build images + start compose stack (default: COMPOSE_ENV_FILE=./env.inmem)
#   make compose-down           # stop compose stack for COMPOSE_ENV_FILE/COMPOSE_PROFILES
#   make compose-up COMPOSE_ENV_FILE=./env.localstack COMPOSE_PROFILES=localstack
#   make compose-down COMPOSE_ENV_FILE=./env.localstack COMPOSE_PROFILES=localstack
#   make compose-up COMPOSE_ENV_FILE=./env.localstack-oidc COMPOSE_PROFILES=localstack-oidc
#   make compose-down COMPOSE_ENV_FILE=./env.localstack-oidc COMPOSE_PROFILES=localstack-oidc
#   make compose-smoke          # sequential docker smoke (inmem + localstack + localstack-oidc)
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
#   make docker                  # build service container images
#   make docker-clean-cache      # clear local Jib temp caches (fixes cache corruption errors)
#   make fmt                     # format Java sources (google-java-format)
#   make help                    # show target list from this Makefile
#
# Examples:
#   make MVN=./mvnw run
#   make CLI_ISOLATED=0 cli-run
#   make ARGS="catalog list" cli-run
#   make QUARKUS_PROFILE=test run
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
COMPOSE_ENV_FILE ?= ./env.inmem
COMPOSE_PROFILES ?=
KEYCLOAK_PORT ?= 12221
KEYCLOAK_ENDPOINT ?= http://127.0.0.1:$(KEYCLOAK_PORT)
KEYCLOAK_HEALTH := $(KEYCLOAK_ENDPOINT)/realms/floecat/.well-known/openid-configuration
KEYCLOAK_TOKEN_URL_DOCKER ?= http://keycloak:8080/realms/floecat/protocol/openid-connect/token
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
QUARKUS_DEV_GOAL := quarkus:dev

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

LOCALSTACK_STORAGE_AWS_PROPS := \
	-Dfloecat.storage.aws.s3.endpoint=$(LOCALSTACK_ENDPOINT) \
	-Dfloecat.storage.aws.region=$(LOCALSTACK_REGION) \
	-Dfloecat.storage.aws.access-key-id=$(LOCALSTACK_ACCESS_KEY) \
	-Dfloecat.storage.aws.secret-access-key=$(LOCALSTACK_SECRET_KEY) \
	-Dfloecat.storage.aws.s3.path-style-access=true \
	-Dfloecat.storage.aws.dynamodb.endpoint-override=$(LOCALSTACK_ENDPOINT)

LOCALSTACK_FIXTURE_AWS_PROPS := \
	-Dfloecat.fixture.aws.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
	-Dfloecat.fixture.aws.s3.endpoint=$(LOCALSTACK_ENDPOINT) \
	-Dfloecat.fixture.aws.s3.region=$(LOCALSTACK_REGION) \
	-Dfloecat.fixture.aws.s3.access-key-id=$(LOCALSTACK_ACCESS_KEY) \
	-Dfloecat.fixture.aws.s3.secret-access-key=$(LOCALSTACK_SECRET_KEY) \
	-Dfloecat.fixture.aws.s3.path-style-access=true

CATALOG_LOCALSTACK_PROPS := \
	-Dfloecat.kv=dynamodb \
	-Dfloecat.kv.table=$(LOCALSTACK_TABLE) \
	-Dfloecat.kv.auto-create=true \
	-Dfloecat.kv.ttl-enabled=true \
	-Dfloecat.blob=s3 \
	-Dfloecat.blob.s3.bucket=$(LOCALSTACK_BUCKET) \
	$(LOCALSTACK_STORAGE_AWS_PROPS) \
	-Daws.requestChecksumCalculation=when_required \
	-Daws.responseChecksumValidation=when_required

FIXTURE_LOCALSTACK_PROPS := \
	-Dfloecat.fixtures.use-aws-s3=true \
	$(LOCALSTACK_FIXTURE_AWS_PROPS)

REST_LOCALSTACK_IO_PROPS := \
	-Dfloecat.gateway.metadata-file-io=org.apache.iceberg.aws.s3.S3FileIO \
	-Dfloecat.gateway.storage-credential.scope=* \
	-Dfloecat.gateway.storage-credential.properties.type=s3 \
	-Dfloecat.gateway.storage-credential.properties.s3.endpoint=$(LOCALSTACK_ENDPOINT) \
	-Dfloecat.gateway.storage-credential.properties.s3.region=$(LOCALSTACK_REGION) \
	-Dfloecat.gateway.storage-credential.properties.s3.access-key-id=$(LOCALSTACK_ACCESS_KEY) \
	-Dfloecat.gateway.storage-credential.properties.s3.secret-access-key=$(LOCALSTACK_SECRET_KEY) \
	-Dfloecat.gateway.storage-credential.properties.s3.path-style-access=true

CATALOG_REAL_AWS_PROPS := \
	-Dfloecat.kv=dynamodb \
	-Dfloecat.kv.table=$(REAL_AWS_TABLE) \
	-Dfloecat.kv.auto-create=true \
	-Dfloecat.kv.ttl-enabled=true \
	-Dfloecat.blob=s3 \
	-Dfloecat.blob.s3.bucket=$(REAL_AWS_BUCKET)

FIXTURE_REAL_AWS_PROPS := -Dfloecat.fixtures.use-aws-s3=true

SEED_ENABLED ?= false
SEED_MODE ?= iceberg
SEED_SOURCE ?= aws

ifeq ($(SEED_ENABLED),true)
SEED_PROPS := \
	-Dfloecat.seed.enabled=true \
	-Dfloecat.seed.mode=$(SEED_MODE)
ifeq ($(SEED_SOURCE),localstack)
SEED_PROPS += $(FIXTURE_LOCALSTACK_PROPS)
else ifeq ($(SEED_SOURCE),aws)
SEED_PROPS += $(FIXTURE_REAL_AWS_PROPS)
else
$(error SEED_SOURCE must be one of: aws, localstack)
endif
else
SEED_PROPS := \
	-Dfloecat.seed.enabled=false \
	-Dfloecat.fixtures.use-aws-s3=false
endif

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
# - test-localstack: fixtures + catalog LocalStack
# ===================================================
.PHONY: test test-localstack unit-test integration-test verify

test: $(PROTO_JAR) keycloak-up
	@echo "==> [BUILD] installing parent POM to local repo"
	$(MVN) $(MVN_TESTALL) install -N
	@echo "==> [TEST] service + REST gateway + client-cli (unit + IT, in-memory)"
	$(MVN) $(MVN_TESTALL) \
	  -Dfloecat.fixtures.use-aws-s3=false \
	  -pl service,protocol-gateway/iceberg-rest,client-cli -am \
	  verify

.PHONY: test-localstack
test-localstack: $(PROTO_JAR) localstack-down localstack-up keycloak-up
	@echo "==> [BUILD] installing parent POM to local repo"
	$(MVN) $(MVN_TESTALL) install -N
	@echo "==> [TEST] full suite (service + REST + CLI) fixtures LocalStack + catalog LocalStack"
	$(LOCALSTACK_ENV) \
	$(MVN) $(MVN_TESTALL) $(CATALOG_LOCALSTACK_PROPS) $(FIXTURE_LOCALSTACK_PROPS) $(REST_LOCALSTACK_IO_PROPS) \
	  -pl service,protocol-gateway/iceberg-rest,client-cli -am \
	  verify

.PHONY: localstack-restart
localstack-restart: localstack-down localstack-up

.PHONY: keycloak-up keycloak-down keycloak-restart
keycloak-up:
	@echo "==> [KEYCLOAK] starting docker compose (keycloak profile)"
	KEYCLOAK_PORT=$(KEYCLOAK_PORT) KC_HOSTNAME=127.0.0.1 KC_HOSTNAME_PORT=$(KEYCLOAK_PORT) $(DOCKER_COMPOSE_KEYCLOAK) up -d keycloak
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
	  -Dfloecat.fixtures.use-aws-s3=false \
	  -pl service,protocol-gateway/iceberg-rest,client-cli -am \
	  -DskipITs=true \
	  test

integration-test: keycloak-up
	@echo "==> [TEST] integration tests (service, REST gateway, client-cli)"
	$(MVN) $(MVN_TESTALL) \
	  -Dfloecat.fixtures.use-aws-s3=false \
	  -pl service,protocol-gateway/iceberg-rest,client-cli -am \
	  -DskipUTs=true -DfailIfNoTests=false \
	  verify

verify: keycloak-up
	@echo "==> [VERIFY] full lifecycle (service, REST gateway, client-cli)"
	$(MVN) $(MVN_TESTALL) \
	  -Dfloecat.fixtures.use-aws-s3=false \
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
	@echo "==> [DEV] quarkus:dev (profile=$(QUARKUS_PROFILE), seeded fake data)"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  -Dfloecat.seed.enabled=true \
	  -Dfloecat.seed.mode=fake \
	  -Dfloecat.fixtures.use-aws-s3=false \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_SERVICE) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: run-aws
run-aws: jar-dependencies
	@if [ -z "$(REAL_AWS_BUCKET)" ] || [ -z "$(REAL_AWS_TABLE)" ]; then \
	  echo "ERROR: REAL_AWS_BUCKET and REAL_AWS_TABLE must be set"; \
	  echo "Example: make run-aws REAL_AWS_BUCKET=my-bucket REAL_AWS_TABLE=my-table"; \
	  echo "Seeded example: make run-aws REAL_AWS_BUCKET=my-bucket REAL_AWS_TABLE=my-table SEED_ENABLED=true SEED_SOURCE=localstack"; \
	  exit 1; \
	fi
	@if [ "$(SEED_ENABLED)" = "true" ] && [ "$(SEED_SOURCE)" = "localstack" ]; then \
	  $(MAKE) localstack-up; \
	fi
	@echo "==> [DEV] quarkus:dev catalog real AWS (seed enabled=$(SEED_ENABLED), source=$(SEED_SOURCE))"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  $(SEED_PROPS) \
	  $(CATALOG_REAL_AWS_PROPS) \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_SERVICE) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: run-localstack
run-localstack: localstack-up jar-dependencies
	@echo "==> [DEV] quarkus:dev catalog LocalStack (seed enabled=$(SEED_ENABLED), source=$(SEED_SOURCE))"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  $(SEED_PROPS) \
	  $(CATALOG_LOCALSTACK_PROPS) \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_SERVICE) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: run-service-aws run-service-localstack
run-service-aws: run-aws
run-service-localstack: run-localstack

.PHONY: run-all
run-all: start-rest run-service

.PHONY: dev-start dev-stop stop-service
dev-start: localstack-up jar-dependencies
	$(MVN) -f ./pom.xml \
	  -Dapplication.name=$(APP_NAME) \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  $(SEED_PROPS) \
	  $(CATALOG_LOCALSTACK_PROPS) \
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
	@echo "==> [DEV] quarkus:dev REST gateway (LocalStack-style storage credentials)"
	$(MVN) -f ./pom.xml \
	  -Dquarkus.profile=$(QUARKUS_PROFILE) \
	  $(REST_LOCALSTACK_IO_PROPS) \
	  $(QUARKUS_DEV_ARGS) \
	  $(REACTOR_REST) \
	  $(QUARKUS_DEV_GOAL)

.PHONY: run-rest-aws
run-rest-aws: $(PROTO_JAR)
	@echo "==> [DEV] quarkus:dev REST gateway (AWS-style storage credentials)"
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
	  $(KEYCLOAK_TOKEN_URL_DOCKER) | jq -r .access_token

.PHONY: cli-docker
cli-docker:
	@echo "==> [RUN] client CLI (docker, OIDC auto-refresh)"
	FLOECAT_ENV_FILE=./env.localstack-oidc \
	$(DOCKER_COMPOSE_MAIN) run --rm \
	  -e FLOECAT_ACCOUNT=$$FLOECAT_ACCOUNT \
	  cli

.PHONY: oidc-up
oidc-up:
	@echo "==> [DOCKER] starting stack with Keycloak + OIDC env"
	@FLOECAT_ENV_FILE=./env.localstack-oidc \
	  $(DOCKER_COMPOSE_MAIN) --profile localstack-oidc up -d

.PHONY: oidc-down
oidc-down:
	@echo "==> [DOCKER] stopping stack with Keycloak + OIDC env"
	@FLOECAT_ENV_FILE=./env.localstack-oidc \
	  $(DOCKER_COMPOSE_MAIN) --profile localstack-oidc down --remove-orphans

.PHONY: cli-test
cli-test: $(PROTO_JAR)
	@mvn -q -f pom.xml -pl client-cli -am test

# ===================================================
# Docker (Quarkus container-image)
# ===================================================
.PHONY: docker docker-service docker-iceberg-rest docker-cli docker-clean-cache compose-up compose-down compose-shell compose-smoke

docker-clean-cache:
	@APP_CACHE="$${TMPDIR%/}/jib-core-application-layers-cache"; \
	BASE_CACHE="$${TMPDIR%/}/jib-core-base-image-cache"; \
	echo "==> [DOCKER] removing Jib caches"; \
	echo "    $$APP_CACHE"; \
	echo "    $$BASE_CACHE"; \
	rm -rf "$$APP_CACHE" "$$BASE_CACHE"

docker: docker-service docker-iceberg-rest docker-cli

docker-service:
	@echo "==> [DOCKER] service (jib -> docker daemon)"
	$(MVN) -f ./pom.xml -pl service -am -DskipTests \
	  -DskipUTs=true -DskipITs=true \
	  -Dquarkus.container-image.build=true \
	  -Dquarkus.jib.base-jvm-image=$(JIB_BASE_IMAGE) \
	  $(if $(JIB_PLATFORMS),-Dquarkus.jib.platforms=$(JIB_PLATFORMS)) \
	  -Dquarkus.container-image.image=floecat-service:local \
	  package

docker-iceberg-rest:
	@echo "==> [DOCKER] iceberg-rest (jib -> docker daemon)"
	$(MVN) -f ./pom.xml -pl protocol-gateway/iceberg-rest -am -DskipTests \
	  -DskipUTs=true -DskipITs=true \
	  -Dquarkus.container-image.build=true \
	  -Dquarkus.jib.base-jvm-image=$(JIB_BASE_IMAGE) \
	  $(if $(JIB_PLATFORMS),-Dquarkus.jib.platforms=$(JIB_PLATFORMS)) \
	  -Dquarkus.container-image.image=floecat-iceberg-rest:local \
	  package

docker-cli:
	@echo "==> [DOCKER] cli (jib -> docker daemon)"
	$(MVN) -f ./pom.xml -pl client-cli -am -DskipTests \
	  -DskipUTs=true -DskipITs=true \
	  -Dquarkus.container-image.build=true \
	  -Dquarkus.jib.base-jvm-image=$(JIB_BASE_IMAGE) \
	  $(if $(JIB_PLATFORMS),-Dquarkus.jib.platforms=$(JIB_PLATFORMS)) \
	  -Dquarkus.container-image.image=floecat-cli:local \
	  package

compose-up: docker
	@echo "==> [COMPOSE] up"
	FLOECAT_ENV_FILE=$(COMPOSE_ENV_FILE) COMPOSE_PROFILES=$(COMPOSE_PROFILES) $(DOCKER_COMPOSE_MAIN) up -d

compose-down:
	@echo "==> [COMPOSE] down"
	FLOECAT_ENV_FILE=$(COMPOSE_ENV_FILE) COMPOSE_PROFILES=$(COMPOSE_PROFILES) $(DOCKER_COMPOSE_MAIN) down --remove-orphans

compose-shell:
	@echo "==> [COMPOSE] shell"
	FLOECAT_ENV_FILE=$(COMPOSE_ENV_FILE) COMPOSE_PROFILES=cli $(DOCKER_COMPOSE_MAIN) run --rm --use-aliases cli

compose-smoke: docker
	@echo "==> [COMPOSE] smoke (inmem + localstack + localstack-oidc)"
	run_mode() { \
	  env_file="$$1"; profile="$$2"; label="$$3"; pre_services="$$4"; kc_host="$$5"; kc_port="$$6"; \
	  assert_contains() { \
	    check_name="$$1"; output="$$2"; pattern="$$3"; \
	    if echo "$$output" | grep -q "$$pattern"; then \
	      echo "[PASS] $$check_name"; \
	    else \
	      echo "[FAIL] $$check_name (missing: $$pattern)"; \
	      echo "---- output begin ----"; \
	      echo "$$output"; \
	      echo "---- output end ----"; \
	      return 1; \
	    fi; \
	  }; \
	  compose_project="floecat-smoke-$$label"; \
	  mode_env="FLOECAT_ENV_FILE=$$env_file COMPOSE_PROFILES=$$profile COMPOSE_PROJECT_NAME=$$compose_project"; \
	  if [ -n "$$kc_host" ]; then mode_env="$$mode_env KC_HOSTNAME=$$kc_host"; fi; \
	  if [ -n "$$kc_port" ]; then mode_env="$$mode_env KC_HOSTNAME_PORT=$$kc_port"; fi; \
	  compose_cmd="$$mode_env $(DOCKER_COMPOSE_MAIN)"; \
	  cleanup() { eval "$$compose_cmd down --remove-orphans -v" >/dev/null 2>&1 || true; }; \
	  trap cleanup RETURN; \
	  echo "==> [SMOKE] mode=$$label"; \
	  eval "$$compose_cmd down --remove-orphans -v" >/dev/null 2>&1 || true; \
	  if [ -n "$$pre_services" ]; then eval "$$compose_cmd up -d $$pre_services"; fi; \
	  if [ "$$profile" = "localstack" ] || [ "$$profile" = "localstack-oidc" ]; then \
	    for i in $$(seq 1 120); do \
	      if curl -fsS http://localhost:4566/_localstack/health >/dev/null 2>&1; then break; fi; \
	      if [ $$i -eq 120 ]; then echo "LocalStack health timed out" >&2; exit 1; fi; \
	      sleep 1; \
	    done; \
	  fi; \
	  if [ "$$profile" = "localstack-oidc" ]; then \
	    for i in $$(seq 1 180); do \
	      if curl -fsS http://localhost:8080/realms/floecat/.well-known/openid-configuration >/dev/null 2>&1; then break; fi; \
	      if [ $$i -eq 180 ]; then echo "Keycloak health timed out" >&2; exit 1; fi; \
	      sleep 1; \
	    done; \
	  fi; \
	  eval "$$compose_cmd up -d"; \
	  for i in $$(seq 1 180); do \
	    if eval "$$compose_cmd logs service 2>&1" | grep -q "Startup seeding completed successfully"; then break; fi; \
	    if eval "$$compose_cmd logs service 2>&1" | grep -q "Startup seeding failed"; then \
	      eval "$$compose_cmd logs --no-color"; \
	      exit 1; \
	    fi; \
	    if [ $$i -eq 180 ]; then \
	      echo "Service seed completion timed out" >&2; \
	      eval "$$compose_cmd logs --no-color"; \
	      exit 1; \
	    fi; \
	    sleep 1; \
	  done; \
	  out_iceberg=$$(printf "account t-0001\nresolve table examples.iceberg.trino_types\nquit\n" | eval "$$compose_cmd run --rm -T cli"); \
	  echo "$$out_iceberg"; \
	  assert_contains "$$label cli resolve iceberg account" "$$out_iceberg" "account set:"; \
	  assert_contains "$$label cli resolve iceberg table" "$$out_iceberg" "table id:"; \
	  out_delta=$$(printf "account t-0001\nresolve table examples.delta.call_center\nquit\n" | eval "$$compose_cmd run --rm -T cli"); \
	  echo "$$out_delta"; \
	  assert_contains "$$label cli resolve call_center account" "$$out_delta" "account set:"; \
	  assert_contains "$$label cli resolve call_center table" "$$out_delta" "table id:"; \
	  out_delta_local=$$(printf "account t-0001\nresolve table examples.delta.my_local_delta_table\nquit\n" | eval "$$compose_cmd run --rm -T cli"); \
	  echo "$$out_delta_local"; \
	  assert_contains "$$label cli resolve my_local_delta_table account" "$$out_delta_local" "account set:"; \
	  assert_contains "$$label cli resolve my_local_delta_table table" "$$out_delta_local" "table id:"; \
	  out_delta_dv=$$(printf "account t-0001\nresolve table examples.delta.dv_demo_delta\nquit\n" | eval "$$compose_cmd run --rm -T cli"); \
	  echo "$$out_delta_dv"; \
	  assert_contains "$$label cli resolve dv_demo_delta account" "$$out_delta_dv" "account set:"; \
	  assert_contains "$$label cli resolve dv_demo_delta table" "$$out_delta_dv" "table id:"; \
	  if [ "$$profile" = "localstack" ]; then \
	    echo "==> [SMOKE] duckdb federation check (localstack)"; \
	    duckdb_out=$$(docker run --rm --network "$${compose_project}_floecat" duckdb/duckdb:latest \
	      duckdb -c "INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws; INSTALL iceberg; LOAD iceberg; SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_region='us-east-1'; SET s3_access_key_id='test'; SET s3_secret_access_key='test'; ATTACH 'examples' AS iceberg_floecat (TYPE iceberg, ENDPOINT 'http://iceberg-rest:9200/', AUTHORIZATION_TYPE none, ACCESS_DELEGATION_MODE 'none'); SELECT 'duckdb_smoke_ok' AS status; SELECT 'call_center=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.call_center; SELECT 'my_local_delta_table=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.my_local_delta_table; SELECT 'dv_demo_delta=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.delta.dv_demo_delta; SELECT 'empty_join=' || CAST(COUNT(*) AS VARCHAR) AS check FROM iceberg_floecat.iceberg.trino_types i JOIN iceberg_floecat.delta.call_center d ON 1=0;"); \
	    echo "$$duckdb_out"; \
	    assert_contains "$$label duckdb smoke marker" "$$duckdb_out" "duckdb_smoke_ok"; \
	    assert_contains "$$label duckdb call_center count" "$$duckdb_out" "call_center=42"; \
	    assert_contains "$$label duckdb my_local_delta_table count" "$$duckdb_out" "my_local_delta_table=1"; \
	    assert_contains "$$label duckdb dv_demo_delta count" "$$duckdb_out" "dv_demo_delta=2"; \
	  fi; \
	}; \
	run_mode ./env.inmem "" inmem "" "" ""; \
	run_mode ./env.localstack localstack localstack "localstack" "" ""; \
	run_mode ./env.localstack-oidc localstack-oidc localstack-oidc "localstack keycloak" "keycloak" "8080"

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
