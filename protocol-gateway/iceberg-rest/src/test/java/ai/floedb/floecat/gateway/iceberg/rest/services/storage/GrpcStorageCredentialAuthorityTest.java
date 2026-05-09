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

package ai.floedb.floecat.gateway.iceberg.rest.services.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.VendedStorageCredential;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class GrpcStorageCredentialAuthorityTest {

  @Test
  void resolveLocationPrefixPrefersMetadataLocationOverNonStorageUpstreamUri() {
    Table table =
        Table.newBuilder()
            .putProperties(
                "metadata-location", "s3://warehouse/orders/metadata/00001.metadata.json")
            .setUpstream(UpstreamRef.newBuilder().setUri("https://polaris:8181/api/catalog"))
            .build();

    assertEquals(
        "s3://warehouse/orders", GrpcStorageCredentialAuthority.resolveLocationPrefix(table));
  }

  @Test
  void resolveLocationPrefixUsesStorageUpstreamUriWhenNoConcreteTableLocationExists() {
    Table table =
        Table.newBuilder()
            .setUpstream(UpstreamRef.newBuilder().setUri("s3://warehouse/orders"))
            .build();

    assertEquals(
        "s3://warehouse/orders", GrpcStorageCredentialAuthority.resolveLocationPrefix(table));
  }

  @Test
  void resolveLocationPrefixUsesDeltaStorageLocation() {
    Table table =
        Table.newBuilder()
            .putProperties("storage_location", "s3://floecat-delta/call_center")
            .build();

    assertEquals(
        "s3://floecat-delta/call_center",
        GrpcStorageCredentialAuthority.resolveLocationPrefix(table));
  }

  @Test
  void resolveLocationPrefixUsesDeltaTableRoot() {
    Table table =
        Table.newBuilder()
            .putProperties("delta.table-root", "s3://floecat-delta/call_center")
            .build();

    assertEquals(
        "s3://floecat-delta/call_center",
        GrpcStorageCredentialAuthority.resolveLocationPrefix(table));
  }

  @Test
  void isStorageUriRejectsCatalogEndpoints() {
    assertFalse(GrpcStorageCredentialAuthority.isStorageUri("https://polaris:8181/api/catalog"));
    assertNull(GrpcStorageCredentialAuthority.resolveLocationPrefix(Table.getDefaultInstance()));
  }

  @Test
  void resolveServerSideFileIoConfigForTableUsesServerSideAuthorityLookup() {
    GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);
    when(grpcClient.resolveStorageAuthority(any()))
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .putClientSafeConfig("s3.endpoint", "http://localhost:4566")
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .setPrefix("s3://warehouse/orders")
                        .putConfig("s3.access-key-id", "key")
                        .putConfig("s3.secret-access-key", "secret")
                        .build())
                .build());

    GrpcStorageCredentialAuthority authority = new GrpcStorageCredentialAuthority(grpcClient);
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct-1")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("tbl-1")
                    .build())
            .putProperties("location", "s3://warehouse/orders")
            .build();

    Map<String, String> config = authority.resolveServerSideFileIoConfig(table, false);

    assertEquals("http://localhost:4566", config.get("s3.endpoint"));
    assertEquals("key", config.get("s3.access-key-id"));
    assertEquals("secret", config.get("s3.secret-access-key"));

    ArgumentCaptor<ResolveStorageAuthorityRequest> requestCaptor =
        ArgumentCaptor.forClass(ResolveStorageAuthorityRequest.class);
    verify(grpcClient).resolveStorageAuthority(requestCaptor.capture());
    ResolveStorageAuthorityRequest request = requestCaptor.getValue();
    assertEquals("acct-1", request.getTableId().getAccountId());
    assertEquals("tbl-1", request.getTableId().getId());
    assertEquals("s3://warehouse/orders", request.getLocationPrefix());
    assertEquals(true, request.getServerSide());
  }

  @Test
  void resolveServerSideFileIoConfigForDeltaTableUsesStorageLocationForAuthorityLookup() {
    GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);
    when(grpcClient.resolveStorageAuthority(any()))
        .thenReturn(
            ResolveStorageAuthorityResponse.newBuilder()
                .putClientSafeConfig("s3.endpoint", "http://localhost:4566")
                .putClientSafeConfig("s3.path-style-access", "true")
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .setPrefix("s3://floecat-delta/call_center")
                        .putConfig("s3.access-key-id", "key")
                        .putConfig("s3.secret-access-key", "secret")
                        .build())
                .build());

    GrpcStorageCredentialAuthority authority = new GrpcStorageCredentialAuthority(grpcClient);
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct-1")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("tbl-1")
                    .build())
            .putProperties("storage_location", "s3://floecat-delta/call_center")
            .build();

    Map<String, String> config = authority.resolveServerSideFileIoConfig(table, false);

    assertEquals("http://localhost:4566", config.get("s3.endpoint"));
    assertEquals("true", config.get("s3.path-style-access"));
    assertEquals("key", config.get("s3.access-key-id"));
    assertEquals("secret", config.get("s3.secret-access-key"));

    ArgumentCaptor<ResolveStorageAuthorityRequest> requestCaptor =
        ArgumentCaptor.forClass(ResolveStorageAuthorityRequest.class);
    verify(grpcClient).resolveStorageAuthority(requestCaptor.capture());
    ResolveStorageAuthorityRequest request = requestCaptor.getValue();
    assertEquals("s3://floecat-delta/call_center", request.getLocationPrefix());
    assertEquals(true, request.getServerSide());
  }

  @Test
  void clientSafeConfigReturnsEmptyWhenStageCreateTableHasNoResourceId() {
    GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);
    GrpcStorageCredentialAuthority authority = new GrpcStorageCredentialAuthority(grpcClient);
    Table table =
        Table.newBuilder().putProperties("location", "s3://warehouse/stage-create/orders").build();

    Map<String, String> config = authority.clientSafeConfig(table);

    assertEquals(Map.of(), config);
    verify(grpcClient, never()).resolveStorageAuthority(any());
  }

  @Test
  void clientSafeConfigReturnsEmptyWhenPersistedTableIsNotFound() {
    GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);
    when(grpcClient.resolveStorageAuthority(any()))
        .thenThrow(new StatusRuntimeException(Status.NOT_FOUND));

    GrpcStorageCredentialAuthority authority = new GrpcStorageCredentialAuthority(grpcClient);
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct-1")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("reserved-real-id")
                    .build())
            .putProperties("location", "s3://warehouse/stage-create/orders")
            .build();

    Map<String, String> config = authority.clientSafeConfig(table);

    assertEquals(Map.of(), config);
    verify(grpcClient).resolveStorageAuthority(any());
  }

  @Test
  void resolveForTableRejectsStageCreateTableWithoutPersistedIdWhenRequired() {
    GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);
    GrpcStorageCredentialAuthority authority = new GrpcStorageCredentialAuthority(grpcClient);
    Table table =
        Table.newBuilder().putProperties("location", "s3://warehouse/stage-create/orders").build();

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> authority.resolveForTable(table, true));

    assertEquals("Credential vending requires a persisted table resource", ex.getMessage());
    verify(grpcClient, never()).resolveStorageAuthority(any());
  }

  @Test
  void resolveServerSideFileIoConfigForUnpersistedTableReturnsEmptyWhenNotRequired() {
    GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);
    GrpcStorageCredentialAuthority authority = new GrpcStorageCredentialAuthority(grpcClient);
    Table table =
        Table.newBuilder()
            .putProperties("location", "s3://warehouse/orders/metadata/00001.metadata.json")
            .build();

    Map<String, String> config = authority.resolveServerSideFileIoConfig(table, false);

    assertEquals(Map.of(), config);
    verify(grpcClient, never()).resolveStorageAuthority(any());
  }

  @Test
  void resolveServerSideFileIoConfigRejectsUnpersistedTableWhenRequired() {
    GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);
    GrpcStorageCredentialAuthority authority = new GrpcStorageCredentialAuthority(grpcClient);
    Table table =
        Table.newBuilder()
            .putProperties("location", "s3://warehouse/orders/metadata/00001.metadata.json")
            .build();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> authority.resolveServerSideFileIoConfig(table, true));

    assertEquals("Credential vending requires a persisted table resource", ex.getMessage());
    verify(grpcClient, never()).resolveStorageAuthority(any());
  }

  @Test
  void resolveServerSideFileIoConfigReturnsEmptyWhenPersistedTableIsNotFound() {
    GrpcServiceFacade grpcClient = mock(GrpcServiceFacade.class);
    when(grpcClient.resolveStorageAuthority(any()))
        .thenThrow(new StatusRuntimeException(Status.NOT_FOUND));

    GrpcStorageCredentialAuthority authority = new GrpcStorageCredentialAuthority(grpcClient);
    Table table =
        Table.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct-1")
                    .setKind(ResourceKind.RK_TABLE)
                    .setId("reserved-real-id")
                    .build())
            .putProperties("location", "s3://warehouse/orders/metadata/00001.metadata.json")
            .build();

    Map<String, String> config = authority.resolveServerSideFileIoConfig(table, false);

    assertEquals(Map.of(), config);
    verify(grpcClient).resolveStorageAuthority(any());
  }
}
