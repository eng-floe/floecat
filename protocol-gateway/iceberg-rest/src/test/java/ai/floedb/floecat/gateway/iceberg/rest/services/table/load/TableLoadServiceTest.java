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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.load;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.storage.rpc.ResolveSnapshotCompatStorageResponse;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.VendedStorageCredential;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TableLoadServiceTest {

  @Test
  void loadResolvedTableAppendsCompatStorageCredentials() {
    TableLoadService service = new TableLoadService();
    service.tableLifecycleService = mock(TableLifecycleService.class);
    service.loadSupport = mock(TableLoadSupport.class);
    service.grpcClient = mock(GrpcServiceFacade.class);

    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct-1")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("tbl-1")
                    .build())
            .putProperties("storage_location", "s3://upstream-bucket/orders")
            .build();
    Snapshot snapshot = Snapshot.newBuilder().setSnapshotId(249933L).build();

    when(service.loadSupport.loadData(table, SnapshotLister.Mode.ALL, tableSupport))
        .thenReturn(new TableLoadSupport.LoadData(null, List.of(snapshot)));
    when(service.loadSupport.deltaCompatEnabled(table)).thenReturn(true);
    when(tableSupport.credentialsForAccessDelegation(table, "vended-credentials"))
        .thenReturn(
            List.of(new StorageCredentialDto("s3://upstream-bucket/orders", Map.of("base", "1"))));
    when(tableSupport.defaultTableConfig(table)).thenReturn(Map.of());
    when(service.grpcClient.resolveSnapshotCompatStorage(any()))
        .thenReturn(
            ResolveSnapshotCompatStorageResponse.newBuilder()
                .setLocationPrefix(
                    "s3://floecat-dev/accounts/acct-1/tables/tbl-1/snapshots/0000000000000249933/compat/iceberg-rest/")
                .setStorage(
                    ResolveStorageAuthorityResponse.newBuilder()
                        .addStorageCredentials(
                            VendedStorageCredential.newBuilder()
                                .setPrefix("s3://floecat-dev/")
                                .putConfig("s3.endpoint", "http://localstack:4566")
                                .putConfig("s3.path-style-access", "true")
                                .build())
                        .build())
                .build());

    Response response =
        service.loadResolvedTable("orders", table, "vended-credentials", tableSupport);

    LoadTableResultDto entity = (LoadTableResultDto) response.getEntity();
    assertNotNull(entity);
    assertNotNull(entity.storageCredentials());
    assertEquals(2, entity.storageCredentials().size());
    StorageCredentialDto compatCredential =
        entity.storageCredentials().stream()
            .filter(credential -> "s3://floecat-dev/".equals(credential.prefix()))
            .findFirst()
            .orElseThrow();
    assertEquals("http://localstack:4566", compatCredential.config().get("s3.endpoint"));
    assertEquals("true", compatCredential.config().get("s3.path-style-access"));
  }

  @Test
  void loadResolvedTableFallsBackToCompatOnlyWhenUpstreamVendingUnavailable() {
    TableLoadService service = new TableLoadService();
    service.tableLifecycleService = mock(TableLifecycleService.class);
    service.loadSupport = mock(TableLoadSupport.class);
    service.grpcClient = mock(GrpcServiceFacade.class);

    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct-1")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("tbl-1")
                    .build())
            .putProperties("storage_location", "s3://upstream-bucket/orders")
            .build();
    Snapshot snapshot = Snapshot.newBuilder().setSnapshotId(249950L).build();

    when(service.loadSupport.loadData(table, SnapshotLister.Mode.ALL, tableSupport))
        .thenReturn(new TableLoadSupport.LoadData(null, List.of(snapshot)));
    when(service.loadSupport.deltaCompatEnabled(table)).thenReturn(true);
    when(tableSupport.usesVendedCredentials("vended_credentials")).thenReturn(true);
    when(tableSupport.credentialsForAccessDelegation(table, "vended_credentials"))
        .thenThrow(
            new IllegalArgumentException(
                "Credential vending was requested but no credentials are available"));
    when(tableSupport.defaultTableConfig(table)).thenReturn(Map.of());
    when(service.grpcClient.resolveSnapshotCompatStorage(any()))
        .thenReturn(
            ResolveSnapshotCompatStorageResponse.newBuilder()
                .setStorage(
                    ResolveStorageAuthorityResponse.newBuilder()
                        .addStorageCredentials(
                            VendedStorageCredential.newBuilder()
                                .setPrefix("s3://floecat-dev/")
                                .putConfig("s3.endpoint", "http://localstack:4566")
                                .build())
                        .build())
                .build());

    Response response =
        service.loadResolvedTable("orders", table, "vended_credentials", tableSupport);

    LoadTableResultDto entity = (LoadTableResultDto) response.getEntity();
    assertNotNull(entity);
    assertNotNull(entity.storageCredentials());
    assertEquals(1, entity.storageCredentials().size());
    assertEquals(
        "http://localstack:4566", entity.storageCredentials().get(0).config().get("s3.endpoint"));
    verify(tableSupport).usesVendedCredentials("vended_credentials");
  }

  @Test
  void loadResolvedTableRewritesDeltaVariantMetadataWhenEnabled() {
    TableLoadService service = new TableLoadService();
    service.tableLifecycleService = mock(TableLifecycleService.class);
    service.loadSupport = mock(TableLoadSupport.class);
    service.grpcClient = mock(GrpcServiceFacade.class);
    service.config = mock(IcebergGatewayConfig.class);

    IcebergGatewayConfig.DeltaCompatConfig deltaCompatConfig =
        mock(IcebergGatewayConfig.DeltaCompatConfig.class);
    when(service.config.deltaCompat()).thenReturn(java.util.Optional.of(deltaCompatConfig));
    when(deltaCompatConfig.rewriteVariantAsStruct()).thenReturn(true);

    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    String variantSchemaJson =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"payload\",\"type\":\"variant\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":4}}]}";
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct-1")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("tbl-1")
                    .build())
            .setSchemaJson(variantSchemaJson)
            .putProperties("storage_location", "s3://upstream-bucket/orders")
            .build();
    Snapshot snapshot =
        Snapshot.newBuilder().setSnapshotId(1L).setSchemaJson(variantSchemaJson).build();

    when(service.loadSupport.loadData(table, SnapshotLister.Mode.ALL, tableSupport))
        .thenReturn(new TableLoadSupport.LoadData(null, List.of(snapshot)));
    when(service.loadSupport.deltaCompatEnabled(table)).thenReturn(true);
    when(tableSupport.credentialsForAccessDelegation(table, "none")).thenReturn(null);
    when(tableSupport.defaultTableConfig(table)).thenReturn(Map.of());

    Response response = service.loadResolvedTable("orders", table, "none", tableSupport);

    LoadTableResultDto entity = (LoadTableResultDto) response.getEntity();
    assertNotNull(entity);
    @SuppressWarnings("unchecked")
    Map<String, Object> schema = entity.metadata().schemas().get(0);
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> fields = (List<Map<String, Object>>) schema.get("fields");
    @SuppressWarnings("unchecked")
    Map<String, Object> payload = fields.get(0);
    @SuppressWarnings("unchecked")
    Map<String, Object> payloadType = (Map<String, Object>) payload.get("type");
    assertEquals("struct", payloadType.get("type"));
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> payloadFields = (List<Map<String, Object>>) payloadType.get("fields");
    assertEquals(
        List.of("metadata", "value"), payloadFields.stream().map(f -> f.get("name")).toList());
  }
}
