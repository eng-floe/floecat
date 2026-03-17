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

package ai.floedb.floecat.gateway.iceberg.minimal.services.compat;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import org.junit.jupiter.api.Test;

class DeltaIcebergMetadataTranslatorTest {
  private final DeltaIcebergMetadataTranslator translator = new DeltaIcebergMetadataTranslator();

  @Test
  void emitsUnpartitionedSpecWhenSnapshotHasNoPartitionSpec() {
    Table table =
        Table.newBuilder()
            .setResourceId(ResourceId.newBuilder().setAccountId("acct").setId("tbl"))
            .setSchemaJson(
                "{\"schema-id\":0,\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"long\",\"nullable\":true}],\"last-column-id\":1}")
            .build();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(10L)
            .setSchemaId(0)
            .setSchemaJson(table.getSchemaJson())
            .build();

    var metadata = translator.translate(table, java.util.List.of(snapshot));

    assertEquals(1, metadata.getPartitionSpecsCount());
    assertEquals(0, metadata.getPartitionSpecs(0).getSpecId());
    assertEquals(0, metadata.getDefaultSpecId());
    assertEquals(0, metadata.getCurrentSchemaId());
  }
}
