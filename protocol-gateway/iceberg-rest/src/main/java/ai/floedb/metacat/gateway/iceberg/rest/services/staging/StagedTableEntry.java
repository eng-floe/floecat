package ai.floedb.metacat.gateway.iceberg.rest.services.staging;

import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.TableRequests;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Holds staged table metadata until a transaction commit materializes it. */
public record StagedTableEntry(
    StagedTableKey key,
    ResourceId catalogId,
    ResourceId namespaceId,
    TableRequests.Create request,
    TableSpec spec,
    List<Map<String, Object>> requirements,
    StageState state,
    Instant createdAt,
    Instant updatedAt,
    String idempotencyKey) {

  public StagedTableEntry {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(catalogId, "catalogId");
    Objects.requireNonNull(namespaceId, "namespaceId");
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(spec, "spec");
    requirements = requirements == null ? List.of() : List.copyOf(requirements);
    state = state == null ? StageState.STAGED : state;
  }

  public StagedTableEntry initializeTimestamps(Instant now) {
    Instant created = createdAt == null ? now : createdAt;
    return new StagedTableEntry(
        key,
        catalogId,
        namespaceId,
        request,
        spec,
        requirements,
        state,
        created,
        now,
        idempotencyKey);
  }

  public StagedTableEntry withState(StageState newState) {
    return new StagedTableEntry(
        key,
        catalogId,
        namespaceId,
        request,
        spec,
        requirements,
        newState,
        createdAt,
        Instant.now(),
        idempotencyKey);
  }
}
