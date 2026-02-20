# External Authentication

## External Session Header

The `floecat` service can accept an external session header carrying a JWT. This is validated by
Quarkus OIDC and used to authenticate gRPC calls.

Auth modes:
- `floecat.auth.mode=oidc`: require session or authorization header.
- `floecat.auth.mode=dev`: require dev context (for local-only use).
Default is `oidc`; dev mode must be explicitly configured.

Account management is authorized via a global IdP role (see below). Floecat does not auto-create
an admin account on startup.

## Local Keycloak (dev)

For local testing you can run Keycloak with a pre-seeded realm that issues `account_id` and `roles`
claims compatible with Floecat.

Start Keycloak:
```
KEYCLOAK_PORT=12221 docker compose --profile keycloak up
```

The realm import defines:
- realm: `floecat`
- client: `floecat-client`
- user: `floecat-admin` / password `floecat-admin`
- roles: `administrator`, `platform-admin`
- hardcoded claim: `account_id=5eaa9cd5-7d08-3750-9457-cfe800b0b9d2` (seed account `t-0001`)

Role intent:
- `platform-admin` grants account management (`account.write`) and is intended for platform operators.
- `administrator` grants full tenant‑scoped access (catalogs/namespaces/tables/connectors) but does
  not grant account management.

Note: the realm config uses a service-account client (`client_credentials` flow). Password grant is
disabled.

Important: if you change the seed account ID, update
`docker/keycloak/realm-floecat.json` so the `account_id` claim matches.

Example service config (dev, running service on the host):
```
floecat.auth.mode=oidc
floecat.interceptor.authorization.header=authorization
quarkus.oidc.auth-server-url=http://127.0.0.1:12221/realms/floecat
quarkus.oidc.token.audience=floecat-client
```

If the service runs in Docker, set the issuer to the Keycloak service name on the Docker network:
```
quarkus.oidc.auth-server-url=http://keycloak:8080/realms/floecat
```
If you run the service on the host, keep the host issuer:
```
quarkus.oidc.auth-server-url=http://host.docker.internal:8080/realms/floecat  # or KEYCLOAK_PORT override
```

Example token request:
```
curl -s \
  -d "client_id=floecat-client" \
  -d "client_secret=floecat-secret" \
  -d "grant_type=client_credentials" \
  http://127.0.0.1:12221/realms/floecat/protocol/openid-connect/token
```

## OIDC Quickstart (Local + Shell CLI)

1) Enable local OIDC in `docker/env.localstack-oidc`:
```
FLOECAT_AUTH_MODE=oidc
FLOECAT_AUTH_PLATFORM_ADMIN_ROLE=platform-admin
FLOECAT_INTERCEPTOR_AUTHORIZATION_HEADER=authorization
FLOECAT_SEED_OIDC_ISSUER=http://keycloak:8080/realms/floecat
FLOECAT_SEED_OIDC_CLIENT_ID=floecat-client
FLOECAT_SEED_OIDC_CLIENT_SECRET=floecat-secret
QUARKUS_OIDC_TENANT_ENABLED=true
QUARKUS_OIDC_AUTH_SERVER_URL=http://keycloak:8080/realms/floecat
QUARKUS_OIDC_TOKEN_AUDIENCE=floecat-client,trino-client
```

2) Start Keycloak + service:
```
make oidc-up
```

3) Fetch a token on the host (issuer must be `http://host.docker.internal:8080/...` unless you
   override `KEYCLOAK_PORT`):
```
TOKEN=$(curl -s \
  -d "client_id=floecat-client" \
  -d "client_secret=floecat-secret" \
  -d "grant_type=client_credentials" \
  http://host.docker.internal:8080/realms/floecat/protocol/openid-connect/token \
  | jq -r .access_token)
```

4) Run the Shell CLI in Docker (interactive):
```
FLOECAT_ACCOUNT=5eaa9cd5-7d08-3750-9457-cfe800b0b9d2 \
make cli-docker
```
Note: `make cli-docker` obtains tokens using `FLOECAT_OIDC_*` settings in
`docker/env.localstack-oidc` and refreshes them automatically before expiry.

Inside the shell:
```
account 5eaa9cd5-7d08-3750-9457-cfe800b0b9d2
account list
```

Configuration (service `application.properties`):
- `quarkus.oidc.tenant-enabled=true` to opt into OIDC validation.
- `floecat.auth.platform-admin.role=platform-admin` to authorize account management.
- `floecat.interceptor.session.header=x-floe-session` to enable the header.
Seed fixture sync in OIDC mode (optional):
- `floecat.seed.oidc.issuer=http://keycloak:8080/realms/floecat` (when running in Docker)
- `floecat.seed.oidc.client-id=floecat-client`
- `floecat.seed.oidc.client-secret=floecat-secret`
- `floecat.interceptor.validate.account=false` to skip account lookup when the caller supplies a
  trusted account id.
- `quarkus.oidc.token.audience=...` to set the `aud` claim value expected.

One of:
- `quarkus.oidc.auth-server-url=...` to validate with an issuer URL.
       or
- `quarkus.oidc.public-key=...` to validate locally with a public key (tests/dev).

Environment equivalents:
- QUARKUS_OIDC_TENANT_ENABLED
- FLOECAT_AUTH_MODE
- FLOECAT_AUTH_PLATFORM_ADMIN_ROLE
- FLOECAT_INTERCEPTOR_SESSION_HEADER
- FLOECAT_INTERCEPTOR_VALIDATE_ACCOUNT
- FLOECAT_SEED_OIDC_ISSUER
- FLOECAT_SEED_OIDC_CLIENT_ID
- FLOECAT_SEED_OIDC_CLIENT_SECRET
- QUARKUS_OIDC_TOKEN_AUDIENCE
- QUARKUS_OIDC_AUTH_SERVER_URL
- QUARKUS_OIDC_PUBLIC_KEY

Further documentation on integration to external OpenID Connect IDPs can be found here:
https://quarkus.io/guides/security-oidc-configuration-properties-reference

## Dev vs Production Configuration

### Development (local)
- Use `docker/env.inmem` with `docker/docker-compose.yml` (via `env_file`) for local defaults.
- For OIDC + Keycloak, use `docker/env.localstack-oidc` by setting
  `FLOECAT_ENV_FILE=./env.localstack-oidc` (or run `make oidc-up`).
- `floecat.auth.mode=oidc` (or `dev` for fully local use).
- Use Keycloak dev realm or another local IdP.
- `quarkus.oidc.auth-server-url=http://127.0.0.1:12221/realms/floecat`
- `quarkus.oidc.token.audience=floecat-client` (or `floecat-client,trino-client` if using Trino).
- Use the IdP role `platform-admin` for account management.
- `floecat.interceptor.authorization.header=authorization` (or `floecat.interceptor.session.header`)
- Consider `floecat.interceptor.validate.account=false` if account ids are trusted in dev.

### Production
- Use `docker/env.aws` (or your own env file) and pass the values to the container.
- `floecat.auth.mode=oidc`
- `quarkus.oidc.tenant-enabled=true`
- Configure exactly one of:
  - `quarkus.oidc.auth-server-url=https://<issuer>/realms/<realm>`
  - `quarkus.oidc.public-key=...` (offline JWT validation)
- `quarkus.oidc.token.audience=<audience>`
- Use the IdP role `platform-admin` for account management.
- `floecat.interceptor.authorization.header=authorization` (or `floecat.interceptor.session.header`)
- `floecat.interceptor.validate.account=true` (recommended in prod)
- Ensure transport security (TLS) at the edge and IdP issuer URLs use HTTPS.

## Direct OIDC Authorization Header

Floecat can also validate a standard `authorization: Bearer <jwt>` header using the same Quarkus
OIDC provider configuration. This is useful when clients authenticate directly with an IdP instead
of via an upstream engine session token.

Configuration (service `application.properties`):
- `quarkus.oidc.tenant-enabled=true` to opt into OIDC validation.
- `floecat.auth.platform-admin.role=platform-admin` to authorize account management.
- `floecat.interceptor.authorization.header=authorization` to enable the header.
Seed fixture sync in OIDC mode (optional):
- `floecat.seed.oidc.issuer=http://keycloak:8080/realms/floecat` (when running in Docker)
- `floecat.seed.oidc.client-id=floecat-client`
- `floecat.seed.oidc.client-secret=floecat-secret`
- `floecat.interceptor.validate.account=false` to skip account lookup when the caller supplies a
  trusted account id.
- `quarkus.oidc.token.audience=...` to set the `aud` claim value expected.

One of:
- `quarkus.oidc.auth-server-url=...` to validate with an issuer URL.
       or
- `quarkus.oidc.public-key=...` to validate locally with a public key (tests/dev).

Environment equivalents:
- QUARKUS_OIDC_TENANT_ENABLED
- FLOECAT_AUTH_MODE
- FLOECAT_AUTH_PLATFORM_ADMIN_ROLE
- FLOECAT_INTERCEPTOR_AUTHORIZATION_HEADER
- FLOECAT_INTERCEPTOR_VALIDATE_ACCOUNT
- FLOECAT_SEED_OIDC_ISSUER
- FLOECAT_SEED_OIDC_CLIENT_ID
- FLOECAT_SEED_OIDC_CLIENT_SECRET
- QUARKUS_OIDC_TOKEN_AUDIENCE
- QUARKUS_OIDC_AUTH_SERVER_URL
- QUARKUS_OIDC_PUBLIC_KEY
