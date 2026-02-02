# External Authentication

## External Session Header

The `floecat` service can accept an external session header carrying a JWT. This is validated by
Quarkus OIDC and used to authenticate gRPC calls.

Auth modes:
- `floecat.auth.mode=auto` (default): preserve legacy behavior (session/authorization if configured,
  else principal header if allowed, else dev context if allowed).
- `floecat.auth.mode=oidc`: require session or authorization header.
- `floecat.auth.mode=oidc-or-principal`: accept session/authorization header, else require
  `x-principal-bin`.
- `floecat.auth.mode=principal-header`: require `x-principal-bin`.
- `floecat.auth.mode=dev`: require dev context (for local-only use).

When `floecat.auth.mode=oidc`, Floecat will ensure an admin account exists on startup. Configure:
- `floecat.auth.admin.account=admin` (display name for the admin account)
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
- user: `admin` / password `admin`
- role: `administrator`
- hardcoded claim: `account_id=21232f29-7a57-35a7-8389-4a0e4a801fc3`

Note: the realm config uses a service-account client (`client_credentials` flow). Password grant is
disabled.

Important: the hardcoded account id is derived from the admin account display name `admin` using
`AccountIds.deterministicAccountId("admin")`. If you change `floecat.auth.admin.account`, update
`docker/keycloak/realm-floecat.json` with the new deterministic id.

Example service config (dev):
```
floecat.auth.mode=oidc
floecat.auth.admin.account=admin
floecat.interceptor.authorization.header=authorization
quarkus.oidc.auth-server-url=http://127.0.0.1:12221/realms/floecat
quarkus.oidc.token.audience=floecat-client
```

Example token request:
```
curl -s \
  -d "client_id=floecat-client" \
  -d "client_secret=floecat-secret" \
  -d "grant_type=client_credentials" \
  http://127.0.0.1:12221/realms/floecat/protocol/openid-connect/token
```

Configuration (service `application.properties`):
- `quarkus.oidc.tenant-enabled=true` to opt into OIDC validation.
- `floecat.interceptor.session.header=x-floe-session` to enable the header.
- `floecat.interceptor.account.header=x-account-id` to provide the account identifier (optional but
  recommended when multi-tenant IdP routing is enabled).
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
- FLOECAT_INTERCEPTOR_SESSION_HEADER
- FLOECAT_INTERCEPTOR_ACCOUNT_HEADER
- FLOECAT_INTERCEPTOR_VALIDATE_ACCOUNT
- QUARKUS_OIDC_TOKEN_AUDIENCE
- QUARKUS_OIDC_AUTH_SERVER_URL
- QUARKUS_OIDC_PUBLIC_KEY

Further documentation on integration to external OpenID Connect IDPs can be found here:
https://quarkus.io/guides/security-oidc-configuration-properties-reference

## Direct OIDC Authorization Header

Floecat can also validate a standard `authorization: Bearer <jwt>` header using the same Quarkus
OIDC provider configuration. This is useful when clients authenticate directly with an IdP instead
of via an upstream engine session token.

Configuration (service `application.properties`):
- `quarkus.oidc.tenant-enabled=true` to opt into OIDC validation.
- `floecat.interceptor.authorization.header=authorization` to enable the header.
- `floecat.interceptor.account.header=x-account-id` to provide the account identifier (optional but
  recommended when multi-tenant IdP routing is enabled).
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
- FLOECAT_INTERCEPTOR_AUTHORIZATION_HEADER
- FLOECAT_INTERCEPTOR_ACCOUNT_HEADER
- FLOECAT_INTERCEPTOR_VALIDATE_ACCOUNT
- QUARKUS_OIDC_TOKEN_AUDIENCE
- QUARKUS_OIDC_AUTH_SERVER_URL
- QUARKUS_OIDC_PUBLIC_KEY
