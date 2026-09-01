# Unity Catalog client

This module is the transport boundary between Floecat and Unity Catalog. Its public API contains
only the catalog operations and normalized models used by connectors; it does not expose HTTP or
JSON types.

`HttpUnityCatalogClient` is the current adapter. It owns authentication headers, URL encoding,
pagination, status classification, JSON decoding, and Unity Catalog wire-format variations. The
metadata endpoints use the Unity Catalog 2.1 API. Temporary table credentials use the 2.0 endpoint
specified by the Databricks API reference and generated Databricks Java SDK.

The open-source `io.unitycatalog:unitycatalog-client` remains a possible future adapter. It is not
the application boundary because its generated models and exceptions would couple connector code
to a much broader API and a separately versioned schema.

HTTP contract tests belong in this module. Connector tests should mock `UnityCatalogClient` and
exercise domain behavior without starting an HTTP server.

## Credential vending route

Databricks serves temporary-table-credential vending under `/api/2.0/unity-catalog/...`, while OSS
Unity Catalog 0.6.0 and later serve the otherwise-compatible operation under `/api/2.1/...`. Every
other operation this client uses is 2.1 on both. The default is the Databricks route; pass the
five-argument constructor (or set `unity.temporary-table-vend-path` on a Delta connector) to select
`OSS_TEMPORARY_TABLE_CREDENTIALS_PATH` or the route a proxy exposes. Whichever route is configured
is the one whose response bodies are kept out of exception messages.
