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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableMetadataBuilderDeltaTest {

  @Test
  void fromCatalogNormalizesDeltaSnapshotSchemaWithoutIcebergIds() {
    String deltaSchemaJson =
        "{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},"
            + "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(11L)
            .setSequenceNumber(11L)
            .setSchemaJson(deltaSchemaJson)
            .build();
    Table table =
        Table.newBuilder()
            .setDisplayName("call_center")
            .setSchemaJson(deltaSchemaJson)
            .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_DELTA).build())
            .build();
    Map<String, String> props = new LinkedHashMap<>();

    TableMetadataView metadata =
        TableMetadataBuilder.fromCatalog("call_center", table, props, List.of(snapshot), null);

    assertEquals(2, metadata.lastColumnId());
    assertEquals(11L, metadata.currentSnapshotId());
    @SuppressWarnings("unchecked")
    Map<String, Object> mainRef = (Map<String, Object>) metadata.refs().get("main");
    assertNotNull(mainRef);
    assertEquals(11L, mainRef.get("snapshot-id"));
    assertFalse(metadata.schemas().isEmpty());
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> fields =
        (List<Map<String, Object>>) metadata.schemas().get(0).get("fields");
    assertEquals(1, fields.get(0).get("id"));
    assertEquals(2, fields.get(1).get("id"));
  }
}
