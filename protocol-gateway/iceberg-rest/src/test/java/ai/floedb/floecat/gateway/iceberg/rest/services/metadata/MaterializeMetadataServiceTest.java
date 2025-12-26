package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMetadataBuilder;
import ai.floedb.floecat.gateway.iceberg.rest.common.TrinoFixtureTestSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService.MaterializeResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Test;

class MaterializeMetadataServiceTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TrinoFixtureTestSupport.Fixture FIXTURE =
      TrinoFixtureTestSupport.simpleFixture();

  @Test
  void materializeCreatesSequentialMetadataFiles() throws Exception {
    TestMaterializeMetadataService service = new TestMaterializeMetadataService();
    service.setMapper(MAPPER);

    TableMetadataView metadata = fixtureMetadata("s3://warehouse/orders/metadata/00000.seed.json");

    MaterializeResult first =
        service.materialize("sales.us", "orders", metadata, metadata.metadataLocation());
    assertTrue(first.metadataLocation().contains("/metadata/00000-"));
    assertTrue(readFile(service.fileIo(), first.metadataLocation()).contains("\"table-uuid\""));

    MaterializeResult second =
        service.materialize(
            "sales.us", "orders", first.metadata(), first.metadata().metadataLocation());
    assertTrue(second.metadataLocation().contains("/metadata/"));
    assertTrue(
        !second.metadataLocation().equals(first.metadataLocation()),
        "Expected subsequent materialization to produce a new metadata file");
    assertTrue(readFile(service.fileIo(), second.metadataLocation()).contains("\"table-uuid\""));
  }

  @Test
  void materializeUsesTableLocationWhenMetadataLocationMissing() throws Exception {
    TestMaterializeMetadataService service = new TestMaterializeMetadataService();
    service.setMapper(MAPPER);

    TableMetadataView base =
        TableMetadataBuilder.fromCatalog(
            "orders",
            FIXTURE.table(),
            new LinkedHashMap<>(FIXTURE.table().getPropertiesMap()),
            FIXTURE.metadata(),
            FIXTURE.snapshots());
    Map<String, String> props = new LinkedHashMap<>(base.properties());
    props.remove("metadata-location");
    props.put("location", "s3://warehouse/db/orders");
    TableMetadataView metadata =
        new TableMetadataView(
            base.formatVersion(),
            base.tableUuid(),
            "s3://warehouse/db/orders",
            null,
            base.lastUpdatedMs(),
            props,
            base.lastColumnId(),
            base.currentSchemaId(),
            base.defaultSpecId(),
            base.lastPartitionId(),
            base.defaultSortOrderId(),
            base.currentSnapshotId(),
            base.lastSequenceNumber(),
            base.schemas(),
            base.partitionSpecs(),
            base.sortOrders(),
            base.refs(),
            base.snapshotLog(),
            base.metadataLog(),
            base.statistics(),
            base.partitionStatistics(),
            base.snapshots());

    MaterializeResult result = service.materialize("sales.us", "orders", metadata, null);

    assertTrue(
        result.metadataLocation().contains("/orders/metadata/"),
        "Expected metadata location to include table metadata directory");
  }

  @Test
  void materializeUpdatesLastSequenceNumberFromSnapshots() throws Exception {
    TestMaterializeMetadataService service = new TestMaterializeMetadataService();
    service.setMapper(MAPPER);

    TableMetadataView base =
        TableMetadataBuilder.fromCatalog(
            "orders",
            FIXTURE.table(),
            new LinkedHashMap<>(FIXTURE.table().getPropertiesMap()),
            null,
            List.of());
    Map<String, Object> snapshot = new LinkedHashMap<>();
    snapshot.put("snapshot-id", 101L);
    snapshot.put("sequence-number", 1L);
    snapshot.put("timestamp-ms", System.currentTimeMillis());
    snapshot.put("manifest-list", "s3://warehouse/orders/metadata/snap-101.avro");
    snapshot.put("schema-id", 0);
    Map<String, String> props = new LinkedHashMap<>(base.properties());
    props.put("format-version", "2");
    props.put("current-snapshot-id", "101");
    Map<String, Object> refs = Map.of("main", Map.of("snapshot-id", 101L, "type", "branch"));
    List<Map<String, Object>> snapshotLog =
        List.of(Map.of("snapshot-id", 101L, "timestamp-ms", System.currentTimeMillis()));
    TableMetadataView withSnapshot =
        new TableMetadataView(
            2,
            base.tableUuid(),
            base.location(),
            "s3://warehouse/orders/metadata/00000-seed.metadata.json",
            base.lastUpdatedMs(),
            props,
            base.lastColumnId(),
            base.currentSchemaId(),
            base.defaultSpecId(),
            base.lastPartitionId(),
            base.defaultSortOrderId(),
            101L,
            0L,
            base.schemas(),
            base.partitionSpecs(),
            base.sortOrders(),
            refs,
            snapshotLog,
            List.of(),
            List.of(),
            List.of(),
            List.of(snapshot));

    MaterializeResult result =
        service.materialize("sales.us", "orders", withSnapshot, withSnapshot.metadataLocation());
    String payload = readFile(service.fileIo(), result.metadataLocation());
    TableMetadata parsed =
        TableMetadataParser.fromJson(result.metadataLocation(), MAPPER.readTree(payload));
    long maxSnapshotSeq =
        parsed.snapshots().stream().mapToLong(snap -> snap.sequenceNumber()).max().orElse(0L);
    assertEquals(1L, maxSnapshotSeq, "Expected snapshot sequence to be 1");
    assertTrue(
        parsed.lastSequenceNumber() >= maxSnapshotSeq,
        "Expected last-sequence-number to be >= snapshot sequence");
  }

  private TableMetadataView fixtureMetadata(String location) {
    TableMetadataView base =
        TableMetadataBuilder.fromCatalog(
            "orders",
            FIXTURE.table(),
            new LinkedHashMap<>(FIXTURE.table().getPropertiesMap()),
            FIXTURE.metadata(),
            FIXTURE.snapshots());
    return base.withMetadataLocation(location);
  }

  private String readFile(InMemoryFileIO io, String location) throws IOException {
    InputFile input = io.newInputFile(location);
    try (SeekableInputStream stream = input.newStream()) {
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  private static final class TestMaterializeMetadataService extends MaterializeMetadataService {
    private final ReopenableInMemoryFileIO fileIo = new ReopenableInMemoryFileIO();

    @Override
    protected FileIO newFileIo(Map<String, String> props) {
      return fileIo;
    }

    InMemoryFileIO fileIo() {
      return fileIo;
    }
  }

  private static final class ReopenableInMemoryFileIO extends InMemoryFileIO {
    @Override
    public void close() {
      // keep data accessible for subsequent reads in tests
    }
  }
}
