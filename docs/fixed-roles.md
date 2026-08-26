# Fixed Roles

This document defines the fixed role names recognized by Floecat and the permissions each role grants.

Source of truth: `service/src/main/java/ai/floedb/floecat/service/security/RolePermissions.java`.

## Role Matrix

| Role name | Purpose | Granted permissions |
|-----------|---------|---------------------|
| `default` | Baseline read-only tenant access. Used when no roles are provided in normal (`oidc`) mode. | `account.read`, `catalog.read`, `namespace.read`, `table.read`, `view.read`, `catalog-integration.read`, `catalog-overlay.read` |
| `administrator` | Full tenant-scoped administration of metadata, catalog integrations, catalog overlays, and legacy connectors. | `account.read`, `catalog.read`, `catalog.write`, `namespace.read`, `namespace.write`, `table.read`, `table.write`, `view.read`, `view.write`, `connector.manage`, `catalog-integration.read`, `catalog-integration.write`, `catalog-integration.use`, `catalog-overlay.read`, `catalog-overlay.write`, `catalog-overlay.reconcile`, `catalog-overlay.delete`, `system-objects.read`, `account.delete` |
| `developer` | Development-role equivalent of `administrator`. | `account.read`, `catalog.read`, `catalog.write`, `namespace.read`, `namespace.write`, `table.read`, `table.write`, `view.read`, `view.write`, `connector.manage`, `catalog-integration.read`, `catalog-integration.write`, `catalog-integration.use`, `catalog-overlay.read`, `catalog-overlay.write`, `catalog-overlay.reconcile`, `catalog-overlay.delete`, `system-objects.read`, `account.delete` |
| `platform-admin` (or configured value of `floecat.auth.platform-admin.role`) | Platform-level account management role from IdP. | `account.read`, `account.write`, `account.delete` |
| `init-account` | Bootstrap role used to initialize account + initial resources. | `account.read`, `account.write`, `catalog.read`, `catalog.write`, `namespace.read`, `namespace.write`, `connector.create`, `catalog-integration.read`, `catalog-integration.write`, `catalog-integration.use`, `catalog-overlay.read`, `catalog-overlay.write` |
| `delete-account` | Narrow internal role used to trigger account teardown. Floecat performs the implied cleanup internally. | `account.delete` |
| `system-objects` | Minimal role for SystemObjects/GetSystemObjects access. | `system-objects.read` |
| `reconcile-worker` | Dedicated machine principal for reconciler background gRPC work. | `account.read`, `catalog.read`, `catalog.write`, `namespace.read`, `namespace.write`, `table.read`, `table.write`, `view.read`, `view.write`, `connector.manage`, `catalog-integration.read`, `catalog-integration.use`, `catalog-overlay.read`, `catalog-overlay.reconcile`, `system-objects.read`, `storage-authority.resolve-internal`, `reconcile-executor-control.internal` |

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
  authority. Cascading integration deletion additionally requires `catalog-overlay.delete` because
  it deletes the dependent overlay resources.
- `catalog-overlay.write` administers overlay records. Creating an overlay additionally requires
  `catalog-integration.use` and `catalog.write`, because it binds an integration to an existing
  writable target Catalog. Renaming an overlay does not rename that Catalog.
- `catalog-overlay.reconcile` authorizes materializing upstream metadata into an overlay and also
  requires `catalog-integration.use`. It is not implied by `catalog-overlay.write` or the
  namespace/table/view write permissions.
- `catalog-overlay.delete` authorizes deletion of an overlay and its managed contributions. It does
  not delete the target Catalog and is not implied by `catalog-overlay.write`.
