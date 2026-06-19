# Storage Authorities

## Overview

Storage authorities are Floecat's prefix-scoped object-storage access definitions. They are used
to match a table, metadata file, or other storage location to a configured object-store prefix and
return:

- client-safe storage config such as region, endpoint, and path-style access
- credentials when a workflow needs storage access for the matched prefix

Storage authorities are generic storage infrastructure, not Iceberg-specific. Iceberg REST
credential vending is one consumer of this layer, but the same model also applies to server-side
Floecat workflows such as reconciliation and register/import flows that need to reach S3.

Storage authorities are separate from connector auth:

- connector auth is how Floecat reaches upstream catalogs or services such as Glue, Unity Catalog,
  or Iceberg REST
- storage authorities are how Floecat resolves access to object storage for a given location prefix

## When To Use One

Use a storage authority when:

- a Floecat workflow needs storage config or credentials for a specific S3 prefix
- a reconciler or import/register path needs to reach object storage
- an Iceberg REST client needs Floecat to vend S3 credentials for matching table prefixes
- you want prefix-based control over which buckets and paths are eligible for delegated access
- you want Floecat to mint temporary credentials instead of giving clients static keys

Do not use a storage authority as a substitute for connector auth. For example:

- `connector create ... --cred-type aws-assume-role ...` is for Floecat reading an upstream Glue
  or Delta source
- `storage-authority create ... --assume-role-arn ...` is for Floecat resolving object-store
  access for matching storage prefixes, including client vending when applicable

## Credential Models

Storage authorities support these common AWS patterns:

- static source credentials via `--cred-type aws`
- source role assumption via `--cred-type aws-assume-role`
- web identity via `--cred-type aws-web-identity`
- vended role assumption via `--assume-role-arn`

The `--assume-role-arn` flags describe the role Floecat should assume for matched locations when a
workflow requires temporary credentials. The `--cred-type ...` flags describe how Floecat
authenticates to AWS in order to do that work.

## Examples

### Static AWS Credentials

```bash
storage-authority create datalake-aws \
  --location-prefix s3://my-datalake-bucket/ \
  --type s3 \
  --region us-east-1 \
  --cred-type aws \
  --cred access_key_id=XXX \
  --cred secret_access_key=YYY \
  --cred session_token=ZZZ
```

`session_token` is optional when the source credentials are long-lived access keys rather than
temporary session credentials.

### Cross-Account Vended S3 Access

```bash
storage-authority create datalake-aws-prod \
  --location-prefix s3://my-datalake-prod-bucket/ \
  --type s3 \
  --region us-east-1 \
  --assume-role-arn arn:aws:iam::123456789012:role/floecat-prod-s3-readonly \
  --assume-role-external-id floecat-production
```

`--assume-role-external-id` is optional unless the target role trust policy requires it.

This example configures Floecat to assume the target role for the given S3 prefix when a matching
workflow needs storage access. That can include Iceberg REST delegated client access, but it is not
limited to that case.

## Common Uses

### Server-Side Floecat Access

Storage authorities can be used by Floecat itself when server-side flows need object-store access
for a matched location, for example:

- reconciler execution against S3-backed tables
- metadata register/import flows
- future storage-backed workflows that resolve locations by prefix

### Iceberg REST Credential Vending

Storage authorities are also used by the Iceberg REST gateway when a client asks for delegated
storage access and the table location matches an authority prefix.

## Common Patterns

### One Bucket, One Authority

Use one storage authority per bucket or per major prefix boundary when you want clean separation of
permissions and easy auditability.

### Cross-Account Read-Only Access

For a production bucket in another AWS account:

- configure `--location-prefix` for the target bucket or prefix
- configure `--assume-role-arn` for a role in that target account
- optionally configure `--assume-role-external-id` if required by the trust policy

The target role should trust the Floecat AWS identity and allow only the minimum S3 actions needed
for the intended workload, whether that is a server-side Floecat flow, an Iceberg REST client, or
both.

## Common Failure Modes

- prefix mismatch: the requested table location does not match any configured `location-prefix`
- missing trust policy: Floecat can resolve the authority but cannot assume the target role
- missing S3 permissions: temporary credentials are minted but the bucket or prefix access is too narrow
- auth confusion: connector auth is configured correctly, but no storage authority exists for the
  required storage prefix

## References

- [CLI reference](cli-reference.md)
- [Iceberg REST gateway](iceberg-rest-gateway.md)
- [Secrets manager](secrets-manager.md)
