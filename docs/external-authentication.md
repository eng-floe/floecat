# External Authentication

## External Session Header

The `floecat` service can accept an external session header carrying a JWT. This is validated by
Quarkus OIDC and used to authenticate gRPC calls.

Auth modes:
- `floecat.auth.mode=oidc`: require session or authorization header.
- `floecat.auth.mode=dev`: require dev context (for local-only use).
Default is `oidc`; dev mode must be explicitly configured.

When `floecat.auth.mode=oidc`, Floecat will ensure an admin account exists on startup. Configure:
- `floecat.auth.admin.account=floecat-admin` (display name for the admin account)
- `floecat.auth.admin.account.id=21232f29-7a57-35a7-8389-4a0e4a801fc3` (optional fixed ID)
- `floecat.auth.admin.account.description=...` (optional description)

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
- role: `administrator`
- hardcoded claim: `account_id=21232f29-7a57-35a7-8389-4a0e4a801fc3`

Note: the realm config uses a service-account client (`client_credentials` flow). Password grant is
disabled.

Important: if you change the admin account ID, update
`docker/keycloak/realm-floecat.json` so the `account_id` claim matches.

Example service config (dev, running service on the host):
```
floecat.auth.mode=oidc
floecat.auth.admin.account=floecat-admin
floecat.auth.admin.account.id=21232f29-7a57-35a7-8389-4a0e4a801fc3
floecat.interceptor.authorization.header=authorization
quarkus.oidc.auth-server-url=http://127.0.0.1:12221/realms/floecat
quarkus.oidc.token.audience=floecat-client
```

If the service runs in Docker, set the issuer to the Docker Desktop hostname:
```
quarkus.oidc.auth-server-url=http://host.docker.internal:12221/realms/floecat
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

1) Enable local OIDC in `docker/env.localstack` (the only OIDC-enabled env file):
```
FLOECAT_AUTH_MODE=oidc
FLOECAT_AUTH_ADMIN_ACCOUNT=floecat-admin
FLOECAT_AUTH_ADMIN_ACCOUNT_ID=21232f29-7a57-35a7-8389-4a0e4a801fc3
FLOECAT_INTERCEPTOR_AUTHORIZATION_HEADER=authorization
QUARKUS_OIDC_TENANT_ENABLED=true
QUARKUS_OIDC_AUTH_SERVER_URL=http://host.docker.internal:12221/realms/floecat
QUARKUS_OIDC_TOKEN_AUDIENCE=floecat-client,trino-client
```

2) Start Keycloak + service:
```
make oidc-up
```

3) Fetch a token on the host (issuer must be `http://host.docker.internal:12221/...`):
```
TOKEN=$(curl -s \
  -d "client_id=floecat-client" \
  -d "client_secret=floecat-secret" \
  -d "grant_type=client_credentials" \
  http://host.docker.internal:12221/realms/floecat/protocol/openid-connect/token \
  | jq -r .access_token)
```

4) Run the Shell CLI in Docker (interactive):
```
FLOECAT_TOKEN=$TOKEN \
FLOECAT_ACCOUNT=<admin_account_id> \
make cli-docker
```

Inside the shell:
```
account <admin_account_id>
account list
```

Configuration (service `application.properties`):
- `quarkus.oidc.tenant-enabled=true` to opt into OIDC validation.
- `floecat.interceptor.session.header=x-floe-session` to enable the header.
- `floecat.interceptor.account.header=x-account-id` to provide the account identifier (internal;
  not part of Iceberg REST).
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
- FLOECAT_AUTH_ADMIN_ACCOUNT
- FLOECAT_AUTH_ADMIN_ACCOUNT_ID
- FLOECAT_INTERCEPTOR_SESSION_HEADER
- FLOECAT_INTERCEPTOR_ACCOUNT_HEADER
- FLOECAT_INTERCEPTOR_VALIDATE_ACCOUNT
- QUARKUS_OIDC_TOKEN_AUDIENCE
- QUARKUS_OIDC_AUTH_SERVER_URL
- QUARKUS_OIDC_PUBLIC_KEY

Further documentation on integration to external OpenID Connect IDPs can be found here:
https://quarkus.io/guides/security-oidc-configuration-properties-reference

## Dev vs Production Configuration

### Development (local)
- Use `docker/env.inmem` with `docker/docker-compose.yml` (via `env_file`) for local defaults.
- For OIDC + Keycloak, use `docker/env.localstack` by setting `FLOECAT_ENV_FILE=./env.localstack`
  (or run `make oidc-up`). This is the only OIDC-enabled env file.
- `floecat.auth.mode=oidc` (or `dev` for fully local use).
- Use Keycloak dev realm or another local IdP.
- `quarkus.oidc.auth-server-url=http://127.0.0.1:12221/realms/floecat`
- `quarkus.oidc.token.audience=floecat-client` (or `floecat-client,trino-client` if using Trino).
- `floecat.auth.admin.account=floecat-admin`
- `floecat.auth.admin.account.id=21232f29-7a57-35a7-8389-4a0e4a801fc3`
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
- `floecat.auth.admin.account=<admin display name>`
- `floecat.interceptor.authorization.header=authorization` (or `floecat.interceptor.session.header`)
- `floecat.interceptor.validate.account=true` (recommended in prod)
- Ensure transport security (TLS) at the edge and IdP issuer URLs use HTTPS.

## Direct OIDC Authorization Header

Floecat can also validate a standard `authorization: Bearer <jwt>` header using the same Quarkus
OIDC provider configuration. This is useful when clients authenticate directly with an IdP instead
of via an upstream engine session token.

Configuration (service `application.properties`):
- `quarkus.oidc.tenant-enabled=true` to opt into OIDC validation.
- `floecat.interceptor.authorization.header=authorization` to enable the header.
- `floecat.interceptor.account.header=x-account-id` to provide the account identifier (internal;
  not part of Iceberg REST).
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
- FLOECAT_AUTH_ADMIN_ACCOUNT
- FLOECAT_AUTH_ADMIN_ACCOUNT_ID
- FLOECAT_INTERCEPTOR_AUTHORIZATION_HEADER
- FLOECAT_INTERCEPTOR_ACCOUNT_HEADER
- FLOECAT_INTERCEPTOR_VALIDATE_ACCOUNT
- QUARKUS_OIDC_TOKEN_AUDIENCE
- QUARKUS_OIDC_AUTH_SERVER_URL
- QUARKUS_OIDC_PUBLIC_KEY
