package ai.floedb.metacat.gateway.iceberg.rest.services.staging;

import java.util.List;

/** Compound identifier for staged table metadata. */
public record StagedTableKey(
    String tenantId, String catalogName, List<String> namespace, String tableName, String stageId) {

  public StagedTableKey {
    namespace = namespace == null ? List.of() : List.copyOf(namespace);
  }
}
