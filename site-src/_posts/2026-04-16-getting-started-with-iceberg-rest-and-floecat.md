---
layout: single
title: "Getting Started with Iceberg REST Catalog and Floecat"
date: 2026-04-16
comments: true
author: mark_cusack
toc: true
toc_sticky: true
classes: wide

header:
  image: /images/icefloe.png
  image_description: "Lake"
  overlay_filter: 0.3
  caption: ""
---

The Iceberg REST catalog gives query engines a standard way to read and update table metadata.
Instead of every engine needing its own catalog-specific integration, anything that speaks the REST spec can work with the same catalog. That removes a lot of integration friction. It doesn’t solve every cross-engine compatibility issue, but it eliminates the need for engine-specific catalog integrations.

Floecat implements the [Apache Iceberg REST catalog specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) and uses it as the foundation of a control plane that can enrich metadata across Iceberg and Delta catalogs.

## What the Iceberg REST Catalog Buys You

At a practical level, the Iceberg REST catalog is about simplifying engine integration without weakening guarantees.
Instead of embedding catalog logic into engine-specific connectors,
the REST catalog becomes the control plane for table metadata:

- Engines talk HTTP/JSON
- Commits are transactional
- Metadata is immutable and versioned
- Storage credentials can be vended through the catalog

A fair question to ask is: "why another Iceberg REST catalog implementation?"
Other implementations like Gravitino, Polaris, and Nessie already exist.
Floecat exists to augment metadata from Iceberg and Delta catalogs with planning-grade statistics.

Most query engines leave a lot on the table when planning against open table formats.
We want to provide richer metadata and statistics for our SQL compute service,
[Floe](https://floedb.ai/), so it can generate better query plans.

## The Spec Isn't as Intimidating as it Looks

The Iceberg REST spec looks large, but most of it collapses into a few concerns:

- Bootstrap and config: how clients discover prefixes, defaults, and catalog capabilities
- Namespaces and tables: lifecycle operations and metadata loading
- Commits & staging: atomic updates expressed as requirements + updates
- Planning & tasks: optional server-side scan planning
- Credentials: optional storage credential vending for object storage

Once you treat the spec as a control-plane contract rather than a giant API surface,
it becomes much more approachable.

## How the Service Is Structured

The Floecat REST catalog implementation lives as a Quarkus RESTEasy service.
Each Iceberg endpoint is implemented as a JAX-RS resource. Behind that sits a gRPC-based catalog service that owns the durable catalog state:

- table lifecycle
- commit evaluation
- metadata persistence
- snapshot handling

The REST layer is primarily a translation boundary.
It also handles protocol-specific behavior like metadata hydration, credential vending, and plan/task orchestration.

## Starting a LocalStack-Based Floecat Setup

For the examples below, start the published GHCR images with LocalStack and Trino enabled in Docker Compose:

```bash
FLOECAT_SERVICE_IMAGE=ghcr.io/eng-floe/floecat-service:main \
FLOECAT_ICEBERG_REST_IMAGE=ghcr.io/eng-floe/floecat-iceberg-rest:main \
FLOECAT_CLI_IMAGE=ghcr.io/eng-floe/floecat-cli:main \
FLOECAT_PULL_POLICY=always \
FLOECAT_ENV_FILE=./env.localstack \
COMPOSE_PROFILES=localstack,trino \
docker compose -f docker/docker-compose.yml up -d --wait
```

That setup gives us:

- Floecat Iceberg REST at `http://localhost:9200`
- LocalStack at `http://localhost:4566`
- Trino at `http://localhost:8081`

LocalStack is configured with simple local development credentials:

- access key: `test`
- secret key: `test`
- region: `us-east-1`

When you're done, stop the stack with the same image settings:

```bash
FLOECAT_SERVICE_IMAGE=ghcr.io/eng-floe/floecat-service:main \
FLOECAT_ICEBERG_REST_IMAGE=ghcr.io/eng-floe/floecat-iceberg-rest:main \
FLOECAT_CLI_IMAGE=ghcr.io/eng-floe/floecat-cli:main \
FLOECAT_PULL_POLICY=always \
FLOECAT_ENV_FILE=./env.localstack \
COMPOSE_PROFILES=localstack,trino \
docker compose -f docker/docker-compose.yml down --remove-orphans
```

## Pointing DuckDB at the Catalog

For DuckDB, the easiest path is to use the bootstrap SQL we keep in the repo for the LocalStack quickstart:

```bash
duckdb -init tools/duckdb-localstack-init.sql
```

That script installs the required DuckDB extensions, configures S3 to talk to LocalStack with path-style access, and attaches the Floecat REST catalog as `iceberg_floecat`.

```sql
SHOW DATABASES;
```

At that point, `iceberg_floecat` is already attached, so you can start querying it immediately. The LocalStack stack seeds the `examples.iceberg` namespace for us, so there is no extra schema creation step here.

If you want to see the equivalent manual setup in a fresh DuckDB session without `-init`, it looks like this:

```sql
CREATE OR REPLACE SECRET floecat_localstack_s3 (
  TYPE S3,
  PROVIDER config,
  KEY_ID 'test',
  SECRET 'test',
  REGION 'us-east-1',
  ENDPOINT 'localhost:4566',
  URL_STYLE 'path',
  USE_SSL false
);

ATTACH 'examples' AS iceberg_floecat (
  TYPE iceberg,
  ENDPOINT 'http://localhost:9200/',
  AUTHORIZATION_TYPE none,
  ACCESS_DELEGATION_MODE 'none',
  PURGE_REQUESTED true
);
```

Once attached, DuckDB immediately calls `/v1/config`.
From there, it discovers the catalog prefix and uses that for all subsequent calls.

Creating and inserting into a table looks ordinary from the SQL side,
but behind the scenes DuckDB exercises a large chunk of the REST spec:

- stage-create for tables
- direct writes of parquet and manifest files to object storage
- commit requests containing Iceberg "requirements and updates"

```sql
drop table if exists iceberg_floecat.iceberg.orders;
create table iceberg_floecat.iceberg.orders (order_id int, item varchar);
insert into iceberg_floecat.iceberg.orders values (0, 'phone');

drop table iceberg_floecat.iceberg.orders;
create table iceberg_floecat.iceberg.orders (order_id int, item varchar);
insert into iceberg_floecat.iceberg.orders values (1, 'tv');
```

The catalog coordinates the commit, validates the requested table changes,
and only makes the new snapshot visible once everything succeeds.
Even single-row inserts go through this staged commit path,
which reinforces the idea that the catalog is the control plane for publishing table state,
even though the engines still participate in the write path.

Reading from the table follows the standard REST flow:

- verify namespace existence
- check table existence
- load table metadata and, if requested, storage credentials
- scan object storage directly

```sql
select * from iceberg_floecat.iceberg.orders;
┌──────────┬─────────┐
│ order_id │  item   │
│  int32   │ varchar │
├──────────┼─────────┤
│        1 │ tv      │
└──────────┴─────────┘
```

That SQL sequence is deliberate, proving that DuckDB can drop and recreate the same Iceberg table cleanly through Floecat, and it leaves the recreated table in place for Trino to query next.
The `PURGE_REQUESTED true` line in the DuckDB `ATTACH` expression tells Floecat to delete the Iceberg artifacts in S3 as well as the catalog metadata.

## Querying the Same Table with Trino

For Trino, use the catalog properties mounted into the container at `docker/trino/catalog/floecat.properties`:

```bash
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:9200
iceberg.rest-catalog.warehouse=examples
iceberg.file-format=parquet
fs.native-s3.enabled=true
s3.aws-access-key=test
s3.aws-secret-key=test
s3.region=us-east-1
s3.endpoint=http://localstack:4566
s3.path-style-access=true
```

What's really interesting here is that we can query the same table
we created earlier with DuckDB but via Trino. Since the compose stack already includes Trino, you can open the CLI directly from the container:

```bash
docker compose -f docker/docker-compose.yml exec trino trino --catalog floecat --schema iceberg
```

Then query the same table:

```sql
select * from orders;
 order_id | item
----------+------
        1 | tv
(1 row)
```

We can also mutate that table:

```sql
insert into orders values (2, 'radio');
INSERT: 1 row

select * from orders;
 order_id | item
----------+-------
        1 | tv
        2 | radio
(2 rows)
```

The key point is that Trino can query and write to the same table DuckDB created, without any engine-specific glue code.

## Where Things Stand with Floecat

At this point, the core Iceberg REST catalog surface is implemented in Floecat.
The catalog has been exercised with both Trino and DuckDB and is used in
automated CI smoke tests.

One caveat that is worth mentioning is that Trino and DuckDB do not currently make use of the plan/task
endpoint, which means they can't offload parquet file pruning to the catalog. It's not a big issue,
as Trino and DuckDB perform the pruning themselves.

## Closing Thoughts

Building an Iceberg REST catalog implementation makes multi-engine support much easier,
and avoids having to build a custom connector to Floecat for every query engine out there.
If you’re evaluating Iceberg beyond "it’s a table format,"
implementing or integrating with the REST catalog is where the control plane
for a lakehouse actually takes shape.

[Floecat](https://github.com/eng-floe/floecat) is open-sourced under the Apache 2.0 license.
