# CLI Reference

The interactive shell surfaces most service capabilities directly. Full command list:

```
Commands:
account <id>
catalogs
catalog create <display_name> [--desc <text>] [--connector <id>] [--policy <id>] [--props k=v ...]
catalog get <display_name|id>
catalog update <display_name|id> [--display <name>] [--desc <text>] [--connector <id>] [--policy <id>] [--props k=v ...] [--etag <etag>]
catalog delete <display_name|id> [--require-empty] [--etag <etag>]
namespaces (<catalog | catalog.ns[.ns...]> | <UUID>) [--id <UUID>] [--prefix P] [--recursive]
namespace create <catalog.ns[.ns...]> [--desc <text>] [--props k=v ...]
namespace get <id | catalog.ns[.ns...]>
namespace update <id|catalog.ns[.ns...]>
    [--display <name>] [--desc <text>]
    [--policy <ref>] [--props k=v ...]
    [--path a.b[.c]] [--catalog <id|name>]
    [--etag <etag>]
namespace delete <id|fq> [--require-empty] [--etag <etag>]
tables <catalog.ns[.ns...][.prefix]>
table create <catalog.ns[.ns...].name> [--desc <text>] [--root <uri>] [--schema <json>] [--parts k1,k2,...] [--format ICEBERG|DELTA] [--props k=v ...]
    [--up-connector <id|name>] [--up-ns <a.b[.c]>] [--up-table <name>]
table get <id|catalog.ns[.ns...].table>
table update <id|fq> [--catalog <catalogName|id>] [--namespace <namespaceFQ|id>] [--name <name>] [--desc <text>]
    [--root <uri>] [--schema <json>] [--parts k=v ...] [--format ICEBERG|DELTA] [--props k=v ...] [--etag <etag>]
table delete <id|fq> [--etag <etag>]

snapshots <catalog.ns[.ns...].table>
snapshot get <table> <snapshot_id|current>
snapshot delete <table> <snapshot_id>

stats table <catalog.ns[.ns...].table> [--snapshot <id|current>]
stats columns <catalog.ns[.ns...].table> [--snapshot <id|current>] [--limit N]
stats files <catalog.ns[.ns...].table> [--snapshot <id|current>] [--limit N]

resolve table <catalog.ns[.ns...].table>
resolve view <catalog.ns[.ns...].view>
resolve catalog <name>
resolve namespace <catalog.ns[.ns...]>
describe table <catalog.ns[.ns...].table>

query begin [--ttl <seconds>] [--as-of-default <timestamp>]
    (table <catalog.ns....table> [--snapshot <id|current>] [--as-of <timestamp>]
     | table-id <uuid> [--snapshot <id|current>] [--as-of <timestamp>]
     | view-id <uuid>
     | namespace <catalog.ns[.ns...]>)+
query renew <query_id> [--ttl <seconds>]
query end <query_id> [--commit|--abort]
query get <query_id>
query fetch-scan <query_id> <table_id>

connectors
connector list [--kind <KIND>] [--page-size <N>]
connector get <display_name|id>
connector create <display_name> <source_type (ICEBERG|DELTA|GLUE|UNITY)> <uri> <source_namespace (a[.b[.c]...])> <destination_catalog (name)>
    [--source-table <name>] [--source-cols c1,#id2,...]
    [--dest-ns <a.b[.c]>] [--dest-table <name>]
    [--desc <text>] [--auth-scheme <scheme>] [--auth k=v ...]
    [--head k=v ...] [--cred-type <type>] [--cred k=v ...] [--cred-head k=v ...]
    [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
    [--policy-not-before-epoch <sec>] [--props k=v ...]
connector update <display_name|id> [--display <name>] [--kind <kind>] [--uri <uri>]
    [--dest-account <account>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>] [--dest-cols c1,#id2,...]
    [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...]
    [--cred-type <type>] [--cred k=v ...] [--cred-head k=v ...]
    [--policy-enabled true|false] [--policy-interval-sec <n>] [--policy-max-par <n>]
    [--policy-not-before-epoch <sec>] [--props k=v ...] [--etag <etag>]
connector delete <display_name|id>  [--etag <etag>]
connector validate <kind> <uri>
    [--dest-account <account>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>] [--dest-cols c1,#id2,...]
    [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...]
    [--cred-type <type>] [--cred k=v ...] [--cred-head k=v ...]
    [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
    [--policy-not-before-epoch <sec>] [--props k=v ...]
connector trigger <display_name|id> [--full]
connector job <jobId>

Credential types (`--cred-type`):
- `bearer` – required: `token` (use this for personal access tokens).
- `client` – required: `endpoint`, `client_id`, `client_secret`.
- `cli` – optional: `provider` (databricks, aws; default databricks). Provider-specific keys are
  supplied via `--cred` and stored in `AuthCredentials.properties` (for example `cache_path`,
  `profile_name`, `client_id`, `scope`).
- `token-exchange` – `endpoint`, `subject_token_type`, `requested_token_type`, `audience`, `scope`,
  `client_id`, `client_secret`.
- `token-exchange-entra` – `endpoint`, `subject_token_type`, `requested_token_type`, `audience`,
  `scope`, `client_id`, `client_secret`.
- `token-exchange-gcp` – base fields `endpoint`, `subject_token_type`, `requested_token_type`,
  `audience`, `scope`; plus optional `gcp.service_account_email`, `gcp.delegated_user`,
  `gcp.service_account_private_key_pem`, `gcp.service_account_private_key_id`,
  `jwt_lifetime_seconds`.
- `aws` – required: `access_key_id`, `secret_access_key`; optional: `session_token`.
- `aws-web-identity` – `role_arn`, `role_session_name`, `provider_id`, `duration_seconds`;
  requires `aws.web_identity_token` via `--cred`.
- `aws-assume-role` – `role_arn`, `role_session_name`, `external_id`, `duration_seconds`.

Any type can include extra `--cred k=v` entries to populate `AuthCredentials.properties` and
`--cred-head k=v` entries to populate `AuthCredentials.headers` (used by token exchange requests).

Auth properties (generic options):
- `--auth aws.profile=<name>` – use an AWS CLI/profile for SigV4 and S3 auth (for example `default`, `dev`).
- `--auth aws.profile_path=<path>` – optional shared credentials/config file path.
- `--auth oauth.mode=cli` – use the CLI cache for OAuth2 token auth.
- `--auth cache_path=<path>` – optional CLI cache path.

CLI cache examples:
- Databricks:
  `--auth-scheme oauth2 --cred-type cli --cred provider=databricks --cred cache_path=~/.databricks/token-cache.json --cred client_id=databricks-cli`
- AWS profile:
  `--auth-scheme aws-sigv4 --cred-type cli --cred provider=aws --cred profile_name=dev --cred cache_path=~/.aws/config`

Auth credential types explained (end-user view):
- `bearer`: You already have an access token (including personal access tokens); Floecat uses it as-is.
- `client`: Floecat exchanges a client ID/secret for an access token at the given endpoint.
- `cli`: Floecat uses local CLI caches. For Databricks, it reads/refreshes the token cache; for AWS,
  it selects a profile from shared config files.
- `token-exchange`: Floecat exchanges a subject token for an access token using RFC 8693.
- `token-exchange-entra`: Floecat performs an Azure Entra OBO exchange to get an access token.
- `token-exchange-gcp`: Floecat performs GCP domain-wide delegation to get an access token.
- `aws`: Floecat uses static AWS access keys (plus optional session token).
- `aws-web-identity`: Floecat assumes an AWS role using a web identity token.
- `aws-assume-role`: Floecat assumes an AWS role using existing AWS credentials.

help
quit
```
