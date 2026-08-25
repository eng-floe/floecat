# Fixed Roles

This document defines the fixed role names recognized by Floecat and the permissions each role grants.

Source of truth: `service/src/main/java/ai/floedb/floecat/service/security/RolePermissions.java`.

## Role Matrix

| Role name | Purpose | Granted permissions |
|-----------|---------|---------------------|
| `default` | Baseline read-only tenant access. Used when no roles are provided in normal (`oidc`) mode. | `account.read`, `catalog.read`, `namespace.read`, `table.read`, `view.read`, `catalog-integration.read`, `catalog-overlay.read` |
| `administrator` | Full tenant-scoped administration of metadata, catalog integrations, catalog overlays, and legacy connectors. | `account.read`, `catalog.read`, `catalog.write`, `namespace.read`, `namespace.write`, `table.read`, `table.write`, `view.read`, `view.write`, `connector.manage`, `catalog-integration.read`, `catalog-integration.write`, `catalog-integration.use`, `catalog-overlay.read`, `catalog-overlay.write`, `system-objects.read`, `account.delete` |
| `developer` | Development-role equivalent of `administrator`. | `account.read`, `catalog.read`, `catalog.write`, `namespace.read`, `namespace.write`, `table.read`, `table.write`, `view.read`, `view.write`, `connector.manage`, `catalog-integration.read`, `catalog-integration.write`, `catalog-integration.use`, `catalog-overlay.read`, `catalog-overlay.write`, `system-objects.read`, `account.delete` |
| `platform-admin` (or configured value of `floecat.auth.platform-admin.role`) | Platform-level account management role from IdP. | `account.read`, `account.write`, `account.delete` |
| `init-account` | Bootstrap role used to initialize account + initial resources. | `account.read`, `account.write`, `catalog.read`, `catalog.write`, `namespace.read`, `namespace.write`, `connector.create`, `catalog-integration.read`, `catalog-integration.write`, `catalog-integration.use`, `catalog-overlay.read`, `catalog-overlay.write` |
| `delete-account` | Narrow internal role used to trigger account teardown. Floecat performs the implied cleanup internally. | `account.delete` |
| `system-objects` | Minimal role for SystemObjects/GetSystemObjects access. | `system-objects.read` |
| `reconcile-worker` | Dedicated machine principal for reconciler background gRPC work. | `account.read`, `catalog.read`, `catalog.write`, `namespace.read`, `namespace.write`, `table.read`, `table.write`, `view.read`, `view.write`, `connector.manage`, `catalog-integration.read`, `catalog-integration.use`, `catalog-overlay.read`, `system-objects.read`, `storage-authority.resolve-internal`, `reconcile-executor-control.internal` |

## Behavior Notes

- Role comparison is case-insensitive.
- Unknown roles are ignored.
- If no effective roles are present:
  - `oidc` mode falls back to `default`.
  - `dev` mode falls back to `developer`.
- In `dev` mode, `account.write` is always added.
- `init-account` also bypasses strict account existence validation during inbound context building.
- `account.delete` is the dedicated destructive permission for account teardown. It is intentionally
  separate from broad catalog/table/connector management permissions.
- `catalog-integration.write` administers integration records, while `catalog-integration.use`
  permits an integration to be bound to an overlay without granting integration-management
  authority. Cascading integration deletion additionally requires `catalog-overlay.write` because
  it deletes the dependent overlay resources.
- `catalog-overlay.write` administers overlay records. Creating an overlay additionally requires
  `catalog-integration.use` and `catalog.write`, because it binds an integration to an existing
  writable target Catalog. Renaming or deleting an overlay does not rename or delete that Catalog.
