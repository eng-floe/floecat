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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema;
import java.lang.reflect.Method;
import org.apache.iceberg.Schema;
import org.junit.jupiter.api.Test;

class DeltaManifestMaterializerTest {

  @Test
  void prefersTranslatedIcebergSchemaOverRawDeltaSchema() throws Exception {
    DeltaManifestMaterializer materializer = new DeltaManifestMaterializer();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(1L)
            .setSchemaJson(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\"}]}")
            .build();
    Table table =
        Table.newBuilder()
            .setSchemaJson(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\"}]}")
            .build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .addSchemas(
                IcebergSchema.newBuilder()
                    .setSchemaId(0)
                    .setLastColumnId(1)
                    .setSchemaJson(
                        "{\"schema-id\":0,\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":false}],\"last-column-id\":1}")
                    .build())
            .build();

    Method method =
        DeltaManifestMaterializer.class.getDeclaredMethod(
            "parseSnapshotSchema", Snapshot.class, Table.class, IcebergMetadata.class);
    method.setAccessible(true);
    Schema parsed = (Schema) method.invoke(materializer, snapshot, table, metadata);

    assertNotNull(parsed);
    assertEquals("id", parsed.columns().getFirst().name());
    assertEquals(1, parsed.columns().getFirst().fieldId());
  }

  @Test
  void failsWhenTranslatedIcebergSchemaMetadataIsMissing() throws Exception {
    DeltaManifestMaterializer materializer = new DeltaManifestMaterializer();
    Snapshot snapshot =
        Snapshot.newBuilder()
            .setSnapshotId(1L)
            .setSchemaJson(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\"}]}")
            .build();
    Table table =
        Table.newBuilder()
            .setSchemaJson(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\"}]}")
            .build();

    Method method =
        DeltaManifestMaterializer.class.getDeclaredMethod(
            "parseSnapshotSchema", Snapshot.class, Table.class, IcebergMetadata.class);
    method.setAccessible(true);

    var thrown =
        assertThrows(
            Exception.class,
            () ->
                method.invoke(materializer, snapshot, table, IcebergMetadata.getDefaultInstance()));
    assertNotNull(thrown.getCause());
    assertEquals(IllegalStateException.class, thrown.getCause().getClass());
  }
}
