package ai.floedb.floecat.gateway.iceberg.rest.services.staging;

import java.util.List;

public record StagedTableKey(
    String accountId, String catalogName, List<String> namespace, String tableName, String stageId) {

  public StagedTableKey {
    namespace = namespace == null ? List.of() : List.copyOf(namespace);
  }
}
