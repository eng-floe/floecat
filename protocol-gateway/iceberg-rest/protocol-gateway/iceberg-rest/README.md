# Iceberg REST Gateway

REST facade over Metacat gRPC for Iceberg clients.

## Endpoints
- Tables: `GET/POST /v1/{catalog}/namespaces/{namespace}/tables`, `GET/PUT/DELETE /v1/{catalog}/namespaces/{namespace}/tables/{table}`
- Namespaces: `GET/POST /v1/{catalog}/namespaces`, `GET/PUT/DELETE /v1/{catalog}/namespaces/{namespace}`
- Views: `GET/POST /v1/{catalog}/namespaces/{namespace}/views`, `GET/PUT/DELETE /v1/{catalog}/namespaces/{namespace}/views/{view}`
- Snapshots: `GET/POST /v1/{catalog}/namespaces/{namespace}/tables/{table}/snapshots`, `GET/DELETE /v1/{catalog}/namespaces/{namespace}/tables/{table}/snapshots/{snapshotId}`, rollback stub at `POST .../rollback`
- Config: `GET /v1/config`

## Headers
- `x-tenant-id` (required)
- `authorization` (required) — forwarded upstream.

## Responses (DTOs)
- Table list: `TableIdentifiersResponse` (identifier + paging)
- Table get/create/update: `LoadTableResultDto` / `CommitTableResponseDto` (metadata + storage config)
- Namespace list: `NamespaceListResponse` (namespace path parts + paging)
- Namespace get/create/update: `NamespaceInfoDto` (namespace parts, description, properties)
- Views: `ViewDto` and `ViewsResponse`
- Snapshots: `SnapshotDto` and `SnapshotsResponse`; rollback currently returns 501
- Errors: `ErrorDto { message, code, details }`
- Config: `ConfigDto` includes endpoints, warehouses, supportedFormats, authTypes

## Notes
- Authentication/tenant headers are enforced by `TenantHeaderFilter` except for `/v1/config`.
- Protobuf payloads are mapped to DTOs to avoid serialization issues.
