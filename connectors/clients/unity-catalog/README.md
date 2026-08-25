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
