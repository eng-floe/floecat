# External Authentication

## External Session Header

The `floecat` service can accept an external session header carrying a JWT. This is validated by
Quarkus OIDC and used to authenticate gRPC calls.

Configuration (service `application.properties`):
- `floecat.interceptor.session.header=x-floe-session` to enable the header.
- `floecat.interceptor.validate.account=false` to skip account lookup when the caller supplies a
  trusted account id.
- `quarkus.oidc.auth-server-url=...` to validate with an issuer URL.
- `quarkus.oidc.public-key=...` to validate locally with a public key (tests/dev).
- `quarkus.oidc.token.audience=...` to set the `aud` claim value expected.

Environment equivalents:
- FLOECAT_INTERCEPTOR_SESSION_HEADER
- FLOECAT_INTERCEPTOR_VALIDATE_ACCOUNT
- QUARKUS_OIDC_AUTH_SERVER_URL
- QUARKUS_OIDC_PUBLIC_KEY
- QUARKUS_OIDC_TOKEN_AUDIENCE

Further documentation on integration to external OpenID Connect IDPs can be found here:
https://quarkus.io/guides/security-oidc-configuration-properties-reference
