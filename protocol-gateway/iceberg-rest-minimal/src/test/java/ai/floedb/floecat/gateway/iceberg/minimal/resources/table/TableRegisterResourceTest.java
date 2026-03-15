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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.TableRegisterRequest;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.ConnectorCleanupService;
import ai.floedb.floecat.gateway.iceberg.minimal.services.table.TableBackend;
import ai.floedb.floecat.gateway.iceberg.minimal.services.transaction.TransactionCommitService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TableRegisterResourceTest {
  private final TableBackend backend = Mockito.mock(TableBackend.class);
  private final TableMetadataImportService importer =
      Mockito.mock(TableMetadataImportService.class);
  private final TransactionCommitService transactionCommitService =
      Mockito.mock(TransactionCommitService.class);
  private final ConnectorCleanupService connectorCleanupService =
      Mockito.mock(ConnectorCleanupService.class);
  private final MinimalGatewayConfig config = Mockito.mock(MinimalGatewayConfig.class);
  private final TableResource resource =
      new TableResource(
          backend,
          new ObjectMapper(),
          importer,
          transactionCommitService,
          connectorCleanupService,
          config);

  @Test
  void registersTableFromImportedMetadata() throws Exception {
    when(importer.importMetadata(eq("s3://bucket/db/orders/metadata/00001.metadata.json"), any()))
        .thenReturn(
            new TableMetadataImportService.ImportedMetadata(
                "{\"type\":\"struct\",\"fields\":[]}",
                Map.of("metadata-location", "s3://bucket/db/orders/metadata/00001.metadata.json"),
                "s3://bucket/db/orders",
                IcebergMetadata.newBuilder()
                    .setFormatVersion(2)
                    .setMetadataLocation("s3://bucket/db/orders/metadata/00001.metadata.json")
                    .setCurrentSnapshotId(77L)
                    .build()));
    when(backend.create(
            eq("foo"),
            eq(java.util.List.of("db")),
            eq("orders"),
            any(),
            eq("s3://bucket/db/orders"),
            any(),
            eq("idem-1")))
        .thenReturn(
            Table.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat:db:orders"))
                .setDisplayName("orders")
                .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
                .putProperties(
                    "metadata-location", "s3://bucket/db/orders/metadata/00001.metadata.json")
                .build());

    LoadTableResultDto dto =
        (LoadTableResultDto)
            resource
                .register(
                    "foo",
                    "db",
                    "idem-1",
                    new TableRegisterRequest(
                        "orders",
                        "s3://bucket/db/orders/metadata/00001.metadata.json",
                        false,
                        Map.of()))
                .getEntity();

    assertEquals("s3://bucket/db/orders/metadata/00001.metadata.json", dto.metadataLocation());
    assertEquals(77L, dto.metadata().get("current-snapshot-id"));
  }

  @Test
  void registerRequiresMetadataLocation() {
    assertEquals(
        400,
        resource
            .register("foo", "db", null, new TableRegisterRequest("orders", null, false, Map.of()))
            .getStatus());
  }
}
