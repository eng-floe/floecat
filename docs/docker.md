# Docker

## Build Images

```bash
make docker
```

## Compose Modes

The stack runs in three modes by selecting an env file and, for LocalStack, a compose profile.

In-memory (default storage, no AWS dependencies):

```bash
docker compose -f docker/docker-compose.yml --env-file docker/env.inmem up -d
```

LocalStack (DynamoDB + S3):

```bash
docker compose -f docker/docker-compose.yml --env-file docker/env.localstack --profile localstack up -d
```

Real AWS (DynamoDB + S3):

```bash
docker compose -f docker/docker-compose.yml --env-file docker/env.aws up -d
```

Interactive CLI in a container:

```bash
make compose-shell
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
- **AWS wiring**: `FLOECAT_S3_REGION`, `FLOECAT_S3_ENDPOINT`, `FLOECAT_DYNAMODB_ENDPOINT`.
- **File IO overrides**: `FLOECAT_FILEIO_IMPL`, `FLOECAT_S3_ACCESS_KEY_ID`,
  `FLOECAT_S3_SECRET_ACCESS_KEY`, `FLOECAT_S3_PATH_STYLE`.
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

- Only `docker/env.localstack` enables OIDC by default.
- The Keycloak issuer for Docker runs is `http://host.docker.internal:12221/realms/floecat`.
- If you use Trino with the REST catalog, ensure the gateway audience list includes both
  `floecat-client` and `trino-client` (see `docker/env.localstack`).
