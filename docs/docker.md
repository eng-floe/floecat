# Docker

## Build Images

```bash
make docker
```

## Compose Modes

The stack runs in four modes by selecting an env file and compose profiles as needed.

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
- **Gateway storage credentials**: `ICEBERG_STORAGE_SCOPE`, `ICEBERG_STORAGE_TYPE`,
  `ICEBERG_STORAGE_KEY_ID`, `ICEBERG_STORAGE_SECRET`, `ICEBERG_STORAGE_REGION`,
  plus `FLOECAT_GATEWAY_STORAGE_CREDENTIAL_PROPERTIES_S3_ENDPOINT` and
  `FLOECAT_GATEWAY_STORAGE_CREDENTIAL_PROPERTIES_S3_PATH_STYLE_ACCESS` for non-default endpoints.
- **Seed/fixtures**: `FLOECAT_SEED_ENABLED`, `FLOECAT_SEED_MODE`, `FLOECAT_FIXTURES_USE_AWS_S3`.

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
