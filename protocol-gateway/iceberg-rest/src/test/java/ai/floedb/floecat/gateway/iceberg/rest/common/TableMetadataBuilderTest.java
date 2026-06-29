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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableMetadataBuilderTest {

  @Test
  void fromCatalogUsesExplicitCurrentSnapshotIdInsteadOfPropertyOrLatestSnapshot() {
    Snapshot current = Snapshot.newBuilder().setSnapshotId(100L).setSequenceNumber(100L).build();
    Snapshot newer = Snapshot.newBuilder().setSnapshotId(200L).setSequenceNumber(200L).build();
    Table table = Table.newBuilder().setDisplayName("orders").build();
    Map<String, String> props = new LinkedHashMap<>();
    props.put("current-snapshot-id", "200");

    TableMetadataView metadata =
        TableMetadataBuilder.fromCatalog(
            "orders",
            table,
            props,
            List.of(current, newer),
            "s3://warehouse/orders/metadata/00001-current.metadata.json",
            100L);

    assertEquals(100L, metadata.currentSnapshotId());
    assertEquals("100", metadata.properties().get("current-snapshot-id"));
    assertEquals(
        "s3://warehouse/orders/metadata/00001-current.metadata.json", metadata.metadataLocation());
    @SuppressWarnings("unchecked")
    Map<String, Object> main = (Map<String, Object>) metadata.refs().get("main");
    assertNotNull(main);
    assertEquals(100L, main.get("snapshot-id"));
  }
}
