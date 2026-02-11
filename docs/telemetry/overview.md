# Telemetry Hub Overview

This directory hosts documentation for the telemetry hub that lives under `telemetry-hub/`. The hub is composed of:

- `telemetry-hub-core` – the framework-neutral contract, registry, and helper APIs.
- `telemetry-hub-backend-micrometer` – the Micrometer-backed implementation (strict/lenient enforcement).
- `telemetry-hub-integration-grpc` – gRPC interceptors and helpers that re-use the core APIs.
- `telemetry-hub-tool-docgen` – generates the canonical metric catalog (Markdown/JSON) from the registry.

Refer to the submodule README files (coming in later PRs) for usage details.
