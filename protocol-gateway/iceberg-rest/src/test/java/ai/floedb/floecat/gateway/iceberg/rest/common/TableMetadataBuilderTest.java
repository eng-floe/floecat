package ai.floedb.floecat.gateway.iceberg.rest.common;

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
    TrinoFixtureTestSupport.Fixture fixture = TrinoFixtureTestSupport.simpleFixture();
    Table table =
        fixture.table().toBuilder()
            .setResourceId(ResourceId.newBuilder().setId("catalog:ns:orders"))
            .build();
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    List<Snapshot> snapshots = fixture.snapshots();
    long earliest = snapshots.stream().mapToLong(Snapshot::getSequenceNumber).min().orElse(0L);
    long latest = snapshots.stream().mapToLong(Snapshot::getSequenceNumber).max().orElse(0L);
    long earliestSnapshotId =
        snapshots.stream()
            .filter(s -> s.getSequenceNumber() == earliest)
            .findFirst()
            .map(Snapshot::getSnapshotId)
            .orElse(0L);
    long latestSnapshotId =
        snapshots.stream()
            .filter(s -> s.getSequenceNumber() == latest)
            .findFirst()
            .map(Snapshot::getSnapshotId)
            .orElse(0L);
    props.put("current-snapshot-id", Long.toString(earliestSnapshotId));
    IcebergMetadata metadata =
        fixture.metadata().toBuilder()
            .setCurrentSnapshotId(earliestSnapshotId)
            .putRefs(
                "main",
                IcebergRef.newBuilder().setSnapshotId(earliestSnapshotId).setType("branch").build())
            .build();

    TableMetadataView view =
        TableMetadataBuilder.fromCatalog("orders", table, props, metadata, snapshots);

    assertEquals(latestSnapshotId, view.currentSnapshotId());
    assertEquals(Long.toString(latestSnapshotId), view.properties().get("current-snapshot-id"));
    @SuppressWarnings("unchecked")
    Map<String, Object> mainRef = (Map<String, Object>) view.refs().get("main");
    assertNotNull(mainRef);
    assertEquals(latestSnapshotId, mainRef.get("snapshot-id"));
  }
}
