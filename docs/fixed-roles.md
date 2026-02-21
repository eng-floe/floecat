# Fixed Roles

This document defines the fixed role names recognized by Floecat and the permissions each role grants.

Source of truth: `service/src/main/java/ai/floedb/floecat/service/security/RolePermissions.java`.

## Role Matrix

| Role name | Purpose | Granted permissions |
|-----------|---------|---------------------|
| `default` | Baseline read-only tenant access. Used when no roles are provided in normal (`oidc`) mode. | `account.read`, `catalog.read`, `namespace.read`, `table.read`, `view.read` |
| `administrator` | Full tenant-scoped administration of metadata and connectors. | `account.read`, `catalog.read`, `catalog.write`, `namespace.read`, `namespace.write`, `table.read`, `table.write`, `view.read`, `view.write`, `connector.manage`, `system-objects.read` |
| `developer` | Development-role equivalent of `administrator`. | `account.read`, `catalog.read`, `catalog.write`, `namespace.read`, `namespace.write`, `table.read`, `table.write`, `view.read`, `view.write`, `connector.manage`, `system-objects.read` |
| `platform-admin` (or configured value of `floecat.auth.platform-admin.role`) | Platform-level account management role from IdP. | `account.read`, `account.write` |
| `init-account` | Bootstrap role used to initialize account/catalog/namespace state. | `account.write`, `catalog.read`, `catalog.write`, `namespace.read`, `namespace.write` |
| `system-objects` | Minimal role for SystemObjects/GetSystemObjects access. | `system-objects.read` |

## Behavior Notes

- Role comparison is case-insensitive.
- Unknown roles are ignored.
- If no effective roles are present:
  - `oidc` mode falls back to `default`.
  - `dev` mode falls back to `developer`.
- In `dev` mode, `account.write` is always added.
- `init-account` also bypasses strict account existence validation during inbound context building.
