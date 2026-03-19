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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class MaterializeMetadataServiceTest {

  @Test
  void canonicalizeResultUpdatesEmbeddedMetadataLocationProperty() {
    MaterializeMetadataService service = new MaterializeMetadataService();
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            schema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            "s3://warehouse/db/orders",
            Map.of(
                "metadata-location",
                "s3://warehouse/db/orders/metadata/00000-old.metadata.json",
                "owner",
                "alice"));
    String newLocation = "s3://warehouse/db/orders/metadata/00001-new.metadata.json";

    MaterializeMetadataService.MaterializeResult result =
        service.canonicalizeResult(newLocation, metadata);

    assertEquals(newLocation, result.metadataLocation());
    assertEquals(newLocation, result.tableMetadata().metadataFileLocation());
    assertEquals(newLocation, result.tableMetadata().properties().get("metadata-location"));
    assertEquals("alice", result.tableMetadata().properties().get("owner"));
  }
}
