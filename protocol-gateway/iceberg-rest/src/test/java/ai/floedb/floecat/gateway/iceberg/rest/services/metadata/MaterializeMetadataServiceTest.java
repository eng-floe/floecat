package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService.MaterializeResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Test;

class MaterializeMetadataServiceTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void materializeCreatesSequentialMetadataFiles() throws Exception {
    TestMaterializeMetadataService service = new TestMaterializeMetadataService();
    service.setMapper(MAPPER);

    TableMetadataView metadata = sampleMetadata("s3://warehouse/orders/metadata/00000.seed.json");

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

  private TableMetadataView sampleMetadata(String location) throws JsonProcessingException {
    return MAPPER.readValue(
        """
        {
          "format-version": 2,
          "table-uuid": "tbl-uuid",
          "location": "s3://warehouse/orders",
          "metadata-location": "%s",
          "last-updated-ms": 1,
          "properties": {
            "io-impl": "org.apache.iceberg.inmemory.InMemoryFileIO"
          },
          "last-column-id": 1,
          "current-schema-id": 1,
          "default-spec-id": 0,
          "last-partition-id": 0,
          "default-sort-order-id": 0,
          "last-sequence-number": 0,
          "schemas": [
            {
              "type": "struct",
              "schema-id": 1,
              "last-column-id": 1,
              "identifier-field-ids": [],
              "fields": [
                {
                  "id": 1,
                  "name": "id",
                  "required": true,
                  "type": "long"
                }
              ]
            }
          ],
          "partition-specs": [
            {
              "spec-id": 0,
              "fields": []
            }
          ],
          "sort-orders": [
            {
              "order-id": 0,
              "fields": []
            }
          ],
          "refs": {},
          "snapshot-log": [],
          "metadata-log": [],
          "statistics": [],
          "partition-statistics": [],
          "snapshots": []
        }
        """
            .formatted(location),
        TableMetadataView.class);
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
