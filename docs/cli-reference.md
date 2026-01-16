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
    [--head k=v ...] [--secret <ref>]
    [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
    [--policy-not-before-epoch <sec>] [--props k=v ...]
connector update <display_name|id> [--display <name>] [--kind <kind>] [--uri <uri>]
    [--dest-account <account>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>] [--dest-cols c1,#id2,...]
    [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
    [--policy-enabled true|false] [--policy-interval-sec <n>] [--policy-max-par <n>]
    [--policy-not-before-epoch <sec>] [--props k=v ...] [--etag <etag>]
connector delete <display_name|id>  [--etag <etag>]
connector validate <kind> <uri>
    [--dest-account <account>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>] [--dest-cols c1,#id2,...]
    [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
    [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
    [--policy-not-before-epoch <sec>] [--props k=v ...]
connector trigger <display_name|id> [--full]
connector job <jobId>

help
quit
```
