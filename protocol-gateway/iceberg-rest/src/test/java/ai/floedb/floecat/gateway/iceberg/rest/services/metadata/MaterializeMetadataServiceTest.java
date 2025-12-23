package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMetadataBuilder;
import ai.floedb.floecat.gateway.iceberg.rest.common.TrinoFixtureTestSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService.MaterializeResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
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
