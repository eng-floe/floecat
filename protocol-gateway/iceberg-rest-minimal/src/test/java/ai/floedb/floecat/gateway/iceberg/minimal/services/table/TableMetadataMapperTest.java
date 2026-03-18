/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.gateway.iceberg.minimal.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.floedb.floecat.catalog.rpc.Table;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableMetadataMapperTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void hidesInternalFileIoAndStoragePropertiesFromMetadataView() {
    Table table =
        Table.newBuilder()
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}")
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties("storage_location", "s3://warehouse/db/orders")
            .putProperties("format-version", "2")
            .putProperties("last-updated-ms", "123")
            .putProperties("io-impl", "org.apache.iceberg.inmemory.InMemoryFileIO")
            .putProperties("fs.floecat.test-root", "/tmp/floecat")
            .putProperties("write.format.default", "PARQUET")
            .putProperties("metadata-location", "s3://warehouse/db/orders/metadata/00001.json")
            .build();

    Map<String, Object> metadata = TableMetadataMapper.loadMetadata(table, null, mapper);
    @SuppressWarnings("unchecked")
    Map<String, String> properties = (Map<String, String>) metadata.get("properties");

    assertEquals("PARQUET", properties.get("write.format.default"));
    assertFalse(properties.containsKey("metadata-location"));
    assertFalse(properties.containsKey("storage_location"));
    assertFalse(properties.containsKey("last-updated-ms"));
    assertFalse(properties.containsKey("io-impl"));
    assertFalse(properties.containsKey("fs.floecat.test-root"));
  }
}
