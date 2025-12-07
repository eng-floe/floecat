package ai.floedb.floecat.service.query.graph;

import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.query.graph.model.TableNode;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Helper factories for graph tests. */
public final class TestNodes {

  private TestNodes() {}

  public static TableNode tableNode(ResourceId tableId, String schemaJson) {
    return new TableNode(
        tableId,
        1L,
        Instant.EPOCH,
        rid(tableId.getTenantId(), "cat-" + tableId.getId()),
        rid(tableId.getTenantId(), "ns-" + tableId.getId()),
        tableId.getId(),
        TableFormat.TF_ICEBERG,
        schemaJson,
        Map.of(),
        List.of(),
        Map.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(),
        Map.of());
  }

  static ResourceId rid(String tenantId, String id) {
    return ResourceId.newBuilder()
        .setTenantId(tenantId)
        .setId(id)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }
}
