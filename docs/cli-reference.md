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
constraints get <id|catalog.ns[.ns...].table> [--snapshot <id>] [--json]
constraints list <id|catalog.ns[.ns...].table> [--limit N] [--json]
constraints put <id|catalog.ns[.ns...].table> [--snapshot <id>] --file <snapshot_constraints_json> [--idempotency <key>] [--json]
constraints update <id|catalog.ns[.ns...].table> [--snapshot <id>] --file <snapshot_constraints_json> [--etag <etag>|--version <n>] [--json]
constraints add <id|catalog.ns[.ns...].table> [--snapshot <id>] --file <snapshot_constraints_json> [--etag <etag>|--version <n>] [--json]
constraints delete <id|catalog.ns[.ns...].table> [--snapshot <id>]
constraints add-one <id|catalog.ns[.ns...].table> [--snapshot <id>] --file <constraint_definition_json> [--etag <etag>|--version <n>] [--json]
constraints delete-one <id|catalog.ns[.ns...].table> <constraint_name> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]
constraints add-pk <id|catalog.ns[.ns...].table> <constraint_name> <columns_csv> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]
constraints add-unique <id|catalog.ns[.ns...].table> <constraint_name> <columns_csv> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]
constraints add-not-null <id|catalog.ns[.ns...].table> <constraint_name> <column_name> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]
constraints add-check <id|catalog.ns[.ns...].table> <constraint_name> <check_expression> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]
constraints add-fk <id|catalog.ns[.ns...].table> <constraint_name> <local_columns_csv> <referenced_table> <referenced_columns_csv> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]

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
    [--mode metadata-only|metadata-and-capture|capture-only]
    [--capture stats|table-stats|file-stats|column-stats|index,...]
    [--dest-ns <a.b[.c]>] [--dest-table <name>] [--dest-view <name>]
    [--snapshot <id>|--current] [--columns c1,#id2,...]
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

Trigger notes:
- `connector trigger` defaults to `metadata-and-capture`.
- `--capture` is required for capture modes (`metadata-and-capture`, `capture-only`).
- Use `--mode metadata-only` when you want a metadata-only reconcile without stats capture.

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

Constraints payload example (`--file`):

```json
{
  "constraints": [
    {
      "name": "pk_users",
      "type": "CT_PRIMARY_KEY",
      "columns": [{"columnName": "id", "ordinal": 1}]
    }
  ]
}
```

Identity normalization note:
- The command target (`constraints ... <table> [--snapshot <id>]`) is authoritative for `table_id` and
  `snapshot_id`.
- If those identity fields are present in the JSON payload, the service normalizes them to the
  command target before persisting.

Constraints command examples:

```text
constraints put demo.sales.users --snapshot 42 --file /tmp/users_constraints.json
constraints add demo.sales.users --snapshot 42 --file /tmp/users_constraints.json
constraints add-one demo.sales.users --snapshot 42 --file /tmp/users_constraint_pk.json
constraints add-pk demo.sales.users pk_users id --snapshot 42
constraints add-unique demo.sales.users uq_users_email email --snapshot 42
constraints add-not-null demo.sales.users nn_users_email email --snapshot 42
constraints add-check demo.sales.users chk_users_age "age > 0" --snapshot 42
constraints add-fk demo.sales.users fk_users_org org_id demo.sales.orgs id --snapshot 42
constraints get demo.sales.users --snapshot 42 --json
constraints list demo.sales.users --limit 50
constraints delete demo.sales.users --snapshot 42
constraints delete-one demo.sales.users pk_users --snapshot 42
```

Bundle mutation semantics:
- `constraints put`: replace the full snapshot bundle with the payload (upsert).
- `constraints update`: server-side atomic merge by `constraint.name` (incoming definitions
  overwrite same-name definitions; other constraints are preserved) and shallow-merge of bundle
  `properties` (incoming keys override existing keys).
- `constraints add`: server-side atomic append-only mutation; fails if any payload
  `constraint.name` already exists.
- `constraints update` / `constraints add` create the snapshot bundle when missing unless an
  explicit `--etag`/`--version` precondition is provided.
For atomic single-constraint mutations under concurrency, prefer
`constraints add-one` / `constraints delete-one`.
Use `constraints add-one` (JSON payload) or typed single-constraint commands
(`add-pk`, `add-unique`, `add-not-null`, `add-check`, `add-fk`) for partial mutations.
When `--snapshot` is omitted, commands default to the table's current snapshot.
