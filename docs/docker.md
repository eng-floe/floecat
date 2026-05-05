# Docker

## Build Images

```bash
make docker
```

## Compose Modes

The stack runs in four modes by selecting an env file and compose profiles as needed.

Quickstart from published GHCR images (LocalStack + `:main` tags):

```bash
make quickstart-up
```

```bash
make quickstart-down
```

In-memory (default storage, no AWS dependencies):

```bash
FLOECAT_ENV_FILE=./env.inmem docker compose -f docker/docker-compose.yml up -d
```

This mode runs with DEV auth (OIDC disabled), so it does not require Keycloak. The default
`docker/env.inmem` also enables seeding in `iceberg` mode.

LocalStack (DynamoDB + S3, DEV auth, no OIDC):

```bash
FLOECAT_ENV_FILE=./env.localstack docker compose -f docker/docker-compose.yml --profile localstack up -d
```

LocalStack + OIDC (DynamoDB + S3 + Keycloak):

```bash
FLOECAT_ENV_FILE=./env.localstack-oidc docker compose -f docker/docker-compose.yml --profile localstack-oidc up -d
```

Real AWS (DynamoDB + S3):

```bash
FLOECAT_ENV_FILE=./env.aws docker compose -f docker/docker-compose.yml up -d
```

Make equivalents:

```bash
# In-memory
make compose-up
make compose-down

# LocalStack
make compose-up COMPOSE_ENV_FILE=./env.localstack COMPOSE_PROFILES=localstack
make compose-down COMPOSE_ENV_FILE=./env.localstack COMPOSE_PROFILES=localstack

# LocalStack + OIDC
make compose-up COMPOSE_ENV_FILE=./env.localstack-oidc COMPOSE_PROFILES=localstack-oidc
make compose-down COMPOSE_ENV_FILE=./env.localstack-oidc COMPOSE_PROFILES=localstack-oidc
```

### Split reconciler mode

The same service image can run as a control-plane node or as a remote executor node.

Run a control plane plus one executor node:

```bash
QUARKUS_PROFILE_SERVICE=reconciler-control \
FLOECAT_ENV_FILE=./env.localstack \
COMPOSE_PROFILES=localstack,reconciler-executor \
docker compose -f docker/docker-compose.yml up -d
```

Run a control plane plus three executor nodes:

```bash
QUARKUS_PROFILE_SERVICE=reconciler-control \
FLOECAT_ENV_FILE=./env.localstack \
COMPOSE_PROFILES=localstack,reconciler-executor \
docker compose -f docker/docker-compose.yml up -d --scale executor=3
```

Notes:

- For a true split deployment, run `service` as the control plane by setting
  `QUARKUS_PROFILE_SERVICE=reconciler-control`. The compose file pins `executor` to
  `QUARKUS_PROFILE_EXECUTOR=reconciler-executor` by default. That keeps the public APIs,
  durable queue ownership, and automatic scheduling on the `service` container while setting
  `reconciler.max-parallelism=0` there.
- `executor` uses the same image but sets:
  `QUARKUS_PROFILE=reconciler-executor`,
  `FLOECAT_RECONCILER_REMOTE_FILE_GROUP_EXECUTOR_ENABLED=true`,
  `FLOECAT_RECONCILER_WORKER_MODE=remote`,
  and `FLOECAT_RECONCILER_AUTO_ENABLED=false`.
- `docker compose --scale executor=3` starts three identical executor replicas. Each replica
  connects to `service:9100`, leases work independently, and heartbeats/completes jobs through
  the control-plane RPCs. No executor leader election is required.
- Both services must share the same blob/kv backend configuration.
- If your control plane requires an authorization token for remote reconciler calls, set `FLOECAT_RECONCILER_AUTHORIZATION_TOKEN` in the env file or shell before `docker compose up`.

In Kubernetes or another orchestrator, the equivalent shape is:

- 1 `service` replica with `QUARKUS_PROFILE=reconciler-control`
- 3 executor replicas with `QUARKUS_PROFILE=reconciler-executor`
- Shared durable blob/kv configuration for all 4 pods
- Executor gRPC client target pointed at the control-plane service DNS name and port

Interactive CLI in a container:

```bash
make compose-shell
```

OIDC-enabled CLI (for `docker/env.localstack-oidc`) with automatic token refresh:

```bash
make cli-docker
```

### Attach/Detach CLI Container

If the stack is already running (including `cli`), attach to the running CLI process:

```bash
docker compose -f docker/docker-compose.yml attach cli
```

Detach without stopping the container:

```text
Ctrl+P, Ctrl+Q
```

To open a new one-off interactive CLI session instead of attaching:

```bash
make compose-shell
```

If you switch between LocalStack modes, stop the previous stack first to avoid leftover services:

```bash
FLOECAT_ENV_FILE=./env.localstack docker compose -f docker/docker-compose.yml --profile localstack down --remove-orphans
```

```bash
FLOECAT_ENV_FILE=./env.localstack-oidc docker compose -f docker/docker-compose.yml --profile localstack-oidc down --remove-orphans
```

## Configuration and Overrides

Compose uses env files to set ports, storage backends, AWS wiring, and feature flags. Defaults live
in `docker/docker-compose.yml`, and per-mode overrides live in `docker/env.*`.

Common configuration knobs:

- **Hosts/IPs**: `FLOECAT_HTTP_HOST` (service bind), `FLOECAT_REST_HOST` (Iceberg REST bind),
  `FLOECAT_GRPC_HOST` (gateway → service target).
- **Ports**: `FLOECAT_HTTP_PORT` (service), `FLOECAT_REST_PORT` (Iceberg REST),
  `FLOECAT_GRPC_PORT` (gateway → service target).
- **Storage backends**: `FLOECAT_KV`, `FLOECAT_BLOB`, `FLOECAT_KV_TABLE`, `FLOECAT_BLOB_S3_BUCKET`.
- **AWS wiring**: `FLOECAT_STORAGE_AWS_REGION`, `FLOECAT_STORAGE_AWS_S3_ENDPOINT`,
  `FLOECAT_STORAGE_AWS_DYNAMODB_ENDPOINT`, `FLOECAT_STORAGE_AWS_ACCESS_KEY_ID`,
  `FLOECAT_STORAGE_AWS_SECRET_ACCESS_KEY`, `FLOECAT_STORAGE_AWS_S3_PATH_STYLE`.
- **Gateway client-safe storage defaults**:
  `FLOECAT_CONNECTOR_INTEGRATION_STORAGE_CREDENTIAL_PROPERTIES_S3_ENDPOINT` and
  `FLOECAT_CONNECTOR_INTEGRATION_STORAGE_CREDENTIAL_PROPERTIES_S3_PATH_STYLE_ACCESS` for
  non-default endpoints. Temporary vended credentials come from storage authorities.
- **Seed/fixtures**: `FLOECAT_SEED_ENABLED`, `FLOECAT_SEED_MODE`, `FLOECAT_FIXTURES_USE_AWS_S3`.
- **Reconciler split deployment**:
  `FLOECAT_RECONCILER_WORKER_MODE`, `FLOECAT_RECONCILER_MAX_PARALLELISM`,
  `FLOECAT_RECONCILER_REMOTE_DEFAULT_EXECUTOR_ENABLED`,
  `FLOECAT_RECONCILER_REMOTE_PLANNER_EXECUTOR_ENABLED`,
  `FLOECAT_RECONCILER_REMOTE_SNAPSHOT_PLANNER_EXECUTOR_ENABLED`,
  `FLOECAT_RECONCILER_REMOTE_FILE_GROUP_EXECUTOR_ENABLED`,
  `FLOECAT_RECONCILER_AUTHORIZATION_HEADER`, `FLOECAT_RECONCILER_AUTHORIZATION_TOKEN`,
  `QUARKUS_PROFILE_SERVICE`, `QUARKUS_PROFILE_EXECUTOR`.

Where variables are consumed (all `FLOECAT_*`):

- `service/src/main/resources/application.properties`
- `protocol-gateway/iceberg-rest/src/main/resources/application.properties`
- `client-cli/src/main/resources/application.properties`

## AWS Credentials (Local Dev)

The containers can use credentials in this order: env vars first, then a local AWS profile when
mounted. This lets SSO or named profiles work without exporting short-lived keys.

Option A: Export credentials into env vars before `docker compose up`:

```bash
aws configure export-credentials --profile awsmark --format env
```

Option B: Mount `~/.aws` and set a profile (this is already wired in `docker/docker-compose.yml`):

```bash
export AWS_PROFILE=awsmark
```

If `AWS_PROFILE` is unset, the SDK falls back to env vars or the instance/container role.

If your images run as a non-root user, mount `~/.aws` into that user’s home in
`docker/docker-compose.yml`.

For Iceberg REST credential vending, define a storage authority after the stack starts instead of
injecting static gateway credentials. In LocalStack-style setups, the authority typically carries a
storage prefix, region, endpoint, path-style flag, and source credentials supplied with
`storage-authority create ... --cred-type aws ...`.

## OIDC Notes

- `docker/env.inmem` explicitly uses DEV auth (`FLOECAT_AUTH_MODE=dev`,
  `FLOECAT_GATEWAY_AUTH_MODE=dev`) and disables OIDC.
- `docker/env.localstack` also uses DEV auth and keeps OIDC disabled for quick starts.
- `docker/env.localstack-oidc` enables OIDC and expects the Keycloak service.
- Services running **inside** the Docker network must use the Keycloak service name:
  `http://keycloak:8080/realms/floecat`.
- Clients running on the **host** (or external containers not on the `docker_floecat` network)
  must use `http://host.docker.internal:8080/realms/floecat` (or `KEYCLOAK_PORT` if overridden).
- If you use Trino with the REST catalog, ensure the gateway audience list includes both
  `floecat-client` and `trino-client` (see `docker/env.localstack-oidc`).

### OIDC Token for Shell

When the stack is running with `docker/env.localstack-oidc`, the CLI must send a bearer token.
`make compose-shell` does not fetch/inject a token. Use one of:

```bash
# Recommended: fetch token and run CLI in one step
make cli-docker
```

```bash
# Token only (if you want to run compose manually)
make cli-docker-token
```
