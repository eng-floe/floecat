package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Helper factories for graph tests. */
public final class TestNodes {

  private TestNodes() {}

  public static UserTableNode tableNode(ResourceId tableId, String schemaJson) {
    return new UserTableNode(
        tableId,
        1L,
        Instant.EPOCH,
        rid(tableId.getAccountId(), "cat-" + tableId.getId()),
        rid(tableId.getAccountId(), "ns-" + tableId.getId()),
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

  static ResourceId rid(String accountId, String id) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setId(id)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }
}
