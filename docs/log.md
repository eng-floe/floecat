# Logging Directory

## Overview
The top-level `log/` directory is where the Quarkus service writes its rotating log files and audit
trail. By default the repository includes placeholder files so the directory exists in source
control (`log/audit.json`). Runtime logs are created automatically when the service starts.

## Architecture & Responsibilities
- **Service log (`log/metacat-service.log`)** – Configured via `quarkus.log.file.*` properties.
  Contains structured JSON or plain text (depending on config) capturing application logs, stack
  traces, and diagnostics.
- **Audit log (`log/audit.json`)** – Dedicated handler for RPC-level audit entries. The service routes
  specific categories (catalog, namespace, directory, table, view, tenant, planning, connector RPCs)
  to this handler via `quarkus.log.category.*.handlers=audit`.

Log rotation is configured in `service/src/main/resources/application.properties` (max file size,
backup count, suffix).

## Public API / Surface Area
There is no executable code in `log/`; it is purely an output directory. Operators interact with it
by tailing files (`make logs`), shipping them to log sinks, or truncating them between runs.

## Important Internal Details
- **JSON format** – Both service and audit logs use Quarkus JSON logging by default
  (`quarkus.log.console.json.enabled=true`). Each entry includes MDC fields such as `plan_id`,
  `correlation_id`, `tenant_id`, and `subject`, populated by `InboundContextInterceptor`.
- **Rotation** – Controlled by `quarkus.log.file.rotation.*`. Files rotate daily (`.yyyy-MM-dd`) or
  when they exceed `20M`, keeping 14 backups.

## Data Flow & Lifecycle
```
Inbound RPC → InboundContextInterceptor populates MDC → Logger writes structured entry
  → File handlers append to log/metacat-service.log & log/audit.json
  → Rotation policies rename/compress files as needed
```
`make logs` tails both files for convenience.

## Configuration & Extensibility
Adjust logging via `application.properties`:
- `quarkus.log.console.json.enabled` – Toggle JSON logging.
- `quarkus.log.file.*` – Enable/disable file logging, control rotation.
- `quarkus.log.handler.file."audit".*` – Configure audit handler path, rotation, levels.
- `quarkus.log.category."ai.floedb.metacat.*".level` – Per-package log levels.

Forward logs to external systems by pointing Quarkus to syslog/OTLP targets or by shipping files
from `log/` via sidecars.

## Examples & Scenarios
- **Local debugging** – Run `make run` and tail `log/metacat-service.log` for structured traces.
- **Auditing** – Inspect `log/audit.json` to review CRUD requests per tenant with correlation IDs for
  compliance.

## Cross-References
- Service observability configuration: [`docs/service.md`](service.md#configuration--extensibility)
