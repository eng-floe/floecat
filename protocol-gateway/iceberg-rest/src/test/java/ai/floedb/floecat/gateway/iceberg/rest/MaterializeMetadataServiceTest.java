package ai.floedb.floecat.gateway.iceberg.rest;

import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Test;

class MaterializeMetadataServiceTest {

  @Test
  void mirrorsMetadataAndPointerFiles() throws IOException {
    MaterializeMetadataService service = new MaterializeMetadataService();
    ObjectMapper mapper = new ObjectMapper();
    service.setMapper(mapper);

    TableMetadataView metadata =
        mapper.readValue(
            """
            {
              "format-version": 2,
              "table-uuid": "tbl-uuid",
              "location": "s3://bucket/db/table",
              "metadata-location": "s3://bucket/db/table/metadata/metadata-1.json",
              "last-updated-ms": 1,
              "properties": {
                "io-impl": "org.apache.iceberg.inmemory.InMemoryFileIO",
                "s3.region": "us-east-1"
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
            """,
            TableMetadataView.class);

    MaterializeMetadataService.MaterializeResult mirrorResult =
        service.materialize("ns", "table", metadata, metadata.metadataLocation());

    InMemoryFileIO io = new InMemoryFileIO();
    String metadataJson = readFile(io, mirrorResult.metadataLocation());
    assertTrue(metadataJson.contains("\"table-uuid\":\"tbl-uuid\""));

    io.deleteFile(mirrorResult.metadataLocation());
  }

  private String readFile(InMemoryFileIO io, String location) throws IOException {
    InputFile input = io.newInputFile(location);
    try (SeekableInputStream stream = input.newStream()) {
      return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
