package ai.floedb.floecat.gateway.iceberg.rest.support.mapper.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableMetadataBuilderTest {

  @Test
  void latestSnapshotBecomesCurrentReference() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setId("catalog:ns:orders"))
            .build();
    Map<String, String> props = new LinkedHashMap<>();
    props.put("current-snapshot-id", "1");
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setMetadataLocation("s3://warehouse/orders/metadata/00000.json")
            .putRefs("main", IcebergRef.newBuilder().setSnapshotId(1).setType("branch").build())
            .build();
    Snapshot firstSnapshot = Snapshot.newBuilder().setSnapshotId(1).setSequenceNumber(1).build();
    Snapshot secondSnapshot = Snapshot.newBuilder().setSnapshotId(2).setSequenceNumber(2).build();

    TableMetadataView view =
        TableMetadataBuilder.fromCatalog(
            "orders", table, props, metadata, List.of(firstSnapshot, secondSnapshot));

    assertEquals(2L, view.currentSnapshotId());
    assertEquals("2", view.properties().get("current-snapshot-id"));
    @SuppressWarnings("unchecked")
    Map<String, Object> mainRef = (Map<String, Object>) view.refs().get("main");
    assertNotNull(mainRef);
    assertEquals(2L, mainRef.get("snapshot-id"));
  }
}
