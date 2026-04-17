---
layout: post
title: "Getting Started with Iceberg REST Catalog and Floecat"
date: 2026-04-16
---

# "Getting Started with Iceberg REST Catalog and Floecat"

Floecat implements the
[Apache Iceberg REST catalog specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) which means that spec-compliant query engines, such as DuckDB and Trino, can interact with Floecat as they would any other
compliant catalog implementation.

## What the Iceberg REST Catalog Actually Buys You

At a practical level, the Iceberg REST catalog is about simplifying engine integration without weakening guarantees.
Instead of embedding catalog logic into engine-specific connectors,
the REST catalog becomes the control plane for table metadata:

- Engines talk HTTP/JSON
- Commits are transactional
- Metadata is immutable and versioned
- Credentials are short-lived and scoped

A fair question to ask is: "why another Iceberg REST catalog implementation?"
It's true that Gravitino, Polaris, Nessie and others already exist, but this Iceberg REST implementation
is part of a broader project that will augment the metadata gathered from upstream
Iceberg and Delta table catalogs to generate advanced statistics for query planning.

Many of the open source query engines leave a lot on the table when planning SQL queries
against these open table formats.
We want to provide richer metadata and statistics for our SQL compute service, 
[Floe](https://floedb.ai/), to enable it to generate very good query plans.

## The Spec Isn't as Intimidating as it Looks at First Glance

The Iceberg REST spec can look intimidating if you approach it endpoint-by-endpoint.
In practice, most of it groups cleanly into a few concerns:

- Bootstrap and config: how clients discover prefixes, defaults, and catalog capabilities
- Namespaces and tables: lifecycle operations and metadata loading
- Commits & staging: atomic updates expressed as requirements + updates
- Planning & tasks: optional server-side scan planning
- Credentials: vended, ephemeral access for object storage

Once you treat the spec as a control-plane contract rather than a giant API surface,
it becomes much more approachable.

## How the Service Is Structured

The REST catalog lives as a Quarkus RESTEasy service.
Each Iceberg endpoint is implemented as a thin JAX-RS resource that handles routing,
validation, and response shaping. Behind that sits a gRPC-based catalog service that owns all stateful behavior:

- table lifecycle
- commit evaluation
- metadata persistence
- snapshot handling

The REST layer is effectively a translation boundary.
HTTP stays stateless, while all catalog semantics live in the gRPC backend.
This separation made the implementation easier to reason about and easier to evolve.

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
- load table metadata and credentials
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

That sequence is deliberate: it proves that DuckDB can drop and recreate the same Iceberg table cleanly through Floecat, and it leaves the recreated table in place for Trino to query next.

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

What mattered wasn’t the setup but the result:
Trino could query and write to the same table that DuckDB created,
without any engine-specific glue code.

## Where Things Stand

At this point, the core Iceberg REST catalog surface is implemented in Floecat.
The catalog has been exercised with both Trino and DuckDB and behaves
the way you’d expect from a real control plane, not a demo service.

One caveat that is worth mentioning is that Trino and DuckDB do not currently make use of the plan/task
endpoint, which means they can't offload parquet file pruning to the catalog. It's not a big issue,
as Trino and DuckDB perform the pruning themselves.

## Closing Thoughts

Building an Iceberg REST catalog implementation means that multi-engine support is essentially free,
and avoids the tiresome process of building a connector to Floecat for every query engine out there.
If you’re evaluating Iceberg beyond "it’s a table format,"
implementing or integrating with the REST catalog is where the architecture really starts to come together.

As a final observation, we put a good deal of faith in query engines
to do the right thing when they write data into a data lakehouse.
Unlike a traditional transactional data warehouse,
a data lakehouse can have writers from different companies
and projects writing to the same table independently.
While the Iceberg table standard and the Iceberg REST catalog can help with
the transactional aspect, it doesn't stop software writing non-compliant files and breaking your data lake.

For example, Apache Spark is case-insensitive by default when it comes to table and column names, 
Apache Iceberg preserves case but uses field IDs internally, 
while Snowflake, Trino, and Apache Flink each apply their own casing rules (uppercasing, 
lowercasing, or SQL-standard quoting). Everything appears fine until those rules collide.

The failure mode is simple: two columns that differ only by case are valid in Iceberg but not in most engines. That mismatch might show up later on, after multiple writers or schema evolution, when queries start failing or behaving unpredictably.

That is part of the motivation for Floecat as a control plane: not just to serve table metadata over REST, but to become a place where cross-engine behavior can be observed, validated, and eventually made safer.

[Floecat](https://github.com/eng-floe/floecat) is now open-sourced under the Apache 2.0 license.
In the next post, I’ll give Floecat a proper architectural overview.
