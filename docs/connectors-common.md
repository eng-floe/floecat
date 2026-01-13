# Connector Common

## Overview

`core/connectors/common/` provides shared utilities used by connector implementations (Delta,
Iceberg, and others). It includes stats engines, NDV helpers, planners, and auth helpers that
implementations reuse rather than duplicate.

## Where It Fits

- **Connector SPI** – Interfaces live in [`docs/connectors-spi.md`](connectors-spi.md).
- **Connector Implementations** – Iceberg and Delta implementations call into the shared helpers
  when generating stats or planning scans.
