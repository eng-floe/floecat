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

package ai.floedb.floecat.gateway.iceberg.minimal.services.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.TableMetadataImportService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class IcebergMetadataCommitServiceTest {
  private final MinimalGatewayConfig config = Mockito.mock(MinimalGatewayConfig.class);
  private final TableMetadataImportService importService = new TableMetadataImportService(config);
  private final IcebergMetadataCommitService service =
      new IcebergMetadataCommitService(importService, config, new ObjectMapper());

  @Test
  void bootstrapsInitialMetadataWhenTableHasNoMetadataLocation() {
    when(config.metadataFileIo())
        .thenReturn(Optional.of("org.apache.iceberg.inmemory.InMemoryFileIO"));
    when(config.metadataFileIoRoot()).thenReturn(Optional.empty());
    when(config.metadataS3Endpoint()).thenReturn(Optional.empty());
    when(config.metadataS3Region()).thenReturn(Optional.empty());
    when(config.metadataClientRegion()).thenReturn(Optional.empty());
    when(config.metadataS3AccessKeyId()).thenReturn(Optional.empty());
    when(config.metadataS3SecretAccessKey()).thenReturn(Optional.empty());
    when(config.metadataS3PathStyleAccess()).thenReturn(true);

    Table currentTable =
        Table.newBuilder()
            .setDisplayName("orders")
            .setSchemaJson("{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}")
            .putProperties("location", "s3://warehouse/db/orders")
            .putProperties("format-version", "2")
            .putProperties("table-uuid", "uuid-123")
            .build();

    IcebergMetadataCommitService.PlannedCommit planned =
        service.plan(
            currentTable,
            ResourceId.newBuilder().setAccountId("acct-1").setId("tbl-1").build(),
            List.of(),
            List.of(
                Map.of(
                    "action",
                    "add-snapshot",
                    "snapshot",
                    Map.of(
                        "snapshot-id",
                        101L,
                        "sequence-number",
                        1L,
                        "timestamp-ms",
                        123456789L,
                        "manifest-list",
                        "s3://warehouse/db/orders/metadata/snap-101.avro",
                        "summary",
                        Map.of("operation", "append"))),
                Map.of(
                    "action",
                    "set-snapshot-ref",
                    "ref-name",
                    "main",
                    "snapshot-id",
                    101L,
                    "type",
                    "branch"),
                Map.of(
                    "action",
                    "set-statistics",
                    "statistics",
                    Map.of(
                        "snapshot-id",
                        101L,
                        "statistics-path",
                        "s3://warehouse/db/orders/stats/101.puffin"))));

    String metadataLocation = planned.table().getPropertiesOrThrow("metadata-location");
    assertNotNull(metadataLocation);
    assertFalse(metadataLocation.isBlank());
    assertEquals("101", planned.table().getPropertiesOrThrow("current-snapshot-id"));
    assertEquals("1", planned.table().getPropertiesOrThrow("last-sequence-number"));
    assertEquals("uuid-123", planned.table().getPropertiesOrThrow("table-uuid"));
  }
}
