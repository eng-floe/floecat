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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class DeltaIcebergMetadataServiceTest {
  @Test
  void prefersRefSnapshotsWhenMetadataHasRefs() {
    DeltaIcebergMetadataService service = new DeltaIcebergMetadataService();
    service.config = enabledConfig();
    service.tableFormatSupport = mock(TableFormatSupport.class);
    service.translator = mock(DeltaIcebergMetadataTranslator.class);
    service.manifestMaterializer = mock(DeltaManifestMaterializer.class);

    Table table = Table.newBuilder().build();
    Snapshot snapshot = Snapshot.newBuilder().setSnapshotId(10L).build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .putRefs(
                "main",
                ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef.newBuilder()
                    .setSnapshotId(10L)
                    .setType("branch")
                    .build())
            .build();
    when(service.translator.translate(table, List.of(snapshot))).thenReturn(metadata);
    when(service.manifestMaterializer.materialize(eq(table), anyList(), eq(metadata)))
        .thenReturn(List.of(snapshot));

    var result = service.load(table, List.of(snapshot));

    assertEquals(1, result.snapshots().size());
    assertEquals(10L, result.snapshots().getFirst().getSnapshotId());
  }

  @Test
  void disabledWhenConfigIsMissing() {
    DeltaIcebergMetadataService service = new DeltaIcebergMetadataService();
    service.config = mock(MinimalGatewayConfig.class);
    service.tableFormatSupport = mock(TableFormatSupport.class);
    when(service.config.deltaCompat()).thenReturn(Optional.empty());

    assertFalse(service.enabledFor(Table.newBuilder().build()));
  }

  @Test
  void passesNormalizedTranslatedMetadataToManifestMaterializer() {
    DeltaIcebergMetadataService service = new DeltaIcebergMetadataService();
    service.config = enabledConfig();
    service.tableFormatSupport = mock(TableFormatSupport.class);
    service.translator = mock(DeltaIcebergMetadataTranslator.class);
    service.manifestMaterializer = mock(DeltaManifestMaterializer.class);

    Table table = Table.newBuilder().build();
    Snapshot snapshot = Snapshot.newBuilder().setSnapshotId(10L).build();
    IcebergMetadata metadata =
        IcebergMetadata.newBuilder()
            .setCurrentSnapshotId(10L)
            .setCurrentSchemaId(0)
            .addSchemas(
                ai.floedb.floecat.gateway.iceberg.rpc.IcebergSchema.newBuilder()
                    .setSchemaId(0)
                    .setLastColumnId(2)
                    .setSchemaJson(
                        "{\"schema-id\":0,\"type\":\"struct\",\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"int\",\"required\":false}],\"last-column-id\":1}")
                    .build())
            .putRefs(
                "main",
                ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef.newBuilder()
                    .setSnapshotId(10L)
                    .setType("branch")
                    .build())
            .build();
    when(service.translator.translate(table, List.of(snapshot))).thenReturn(metadata);
    when(service.manifestMaterializer.materialize(eq(table), anyList(), eq(metadata)))
        .thenReturn(List.of(snapshot));

    service.load(table, List.of(snapshot));

    ArgumentCaptor<IcebergMetadata> captor = ArgumentCaptor.forClass(IcebergMetadata.class);
    verify(service.manifestMaterializer).materialize(eq(table), anyList(), captor.capture());
    IcebergMetadata passed = captor.getValue();
    assertEquals(1, passed.getSchemasCount());
    assertFalse(passed.getSchemas(0).getSchemaJson().isBlank());
    assertEquals(10L, passed.getCurrentSnapshotId());
  }

  private static MinimalGatewayConfig enabledConfig() {
    MinimalGatewayConfig config = mock(MinimalGatewayConfig.class);
    when(config.deltaCompat())
        .thenReturn(
            Optional.of(
                new MinimalGatewayConfig.DeltaCompatConfig() {
                  @Override
                  public boolean enabled() {
                    return true;
                  }

                  @Override
                  public boolean readOnly() {
                    return true;
                  }
                }));
    return config;
  }
}
