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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.GetConnectorResponse;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcWithHeaders;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Timestamps;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ConnectorProvisioningServiceTest {
  private final MinimalGatewayConfig config = Mockito.mock(MinimalGatewayConfig.class);
  private final GrpcWithHeaders grpc = Mockito.mock(GrpcWithHeaders.class);
  private final GrpcClients clients = Mockito.mock(GrpcClients.class);
  private final ConnectorsGrpc.ConnectorsBlockingStub connectorStub =
      Mockito.mock(ConnectorsGrpc.ConnectorsBlockingStub.class);

  private final ConnectorProvisioningService service =
      new ConnectorProvisioningService(config, grpc);

  @Test
  void refreshesExistingConnectorMetadataLocationOnCommit() {
    when(config.metadataS3PathStyleAccess()).thenReturn(true);
    when(grpc.raw()).thenReturn(clients);
    when(clients.connector()).thenReturn(connectorStub);
    when(grpc.withHeaders(connectorStub)).thenReturn(connectorStub);

    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct-1")
            .setId("conn-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct-1")
            .setId("tbl-1")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    ResourceId namespaceId = ResourceId.newBuilder().setAccountId("acct-1").setId("ns-1").build();
    ResourceId catalogId = ResourceId.newBuilder().setAccountId("acct-1").setId("cat-1").build();

    Connector existingConnector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setDisplayName("register:minimal:iceberg.orders")
            .setUri("s3://floecat/iceberg/duckdb_mutation_smoke")
            .setState(ConnectorState.CS_ACTIVE)
            .setAuth(AuthConfig.newBuilder().setScheme("none").build())
            .setSource(
                SourceSelector.newBuilder()
                    .setNamespace(NamespacePath.newBuilder().addSegments("iceberg").build())
                    .setTable("orders")
                    .build())
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(catalogId)
                    .setNamespaceId(namespaceId)
                    .setTableId(tableId)
                    .setTableDisplayName("orders")
                    .build())
            .setCreatedAt(Timestamps.fromMillis(1))
            .setUpdatedAt(Timestamps.fromMillis(1))
            .putProperties(
                "external.metadata-location",
                "s3://floecat/iceberg/duckdb_mutation_smoke/metadata/00001-old.metadata.json")
            .putProperties("iceberg.source", "filesystem")
            .build();
    when(connectorStub.getConnector(any()))
        .thenReturn(GetConnectorResponse.newBuilder().setConnector(existingConnector).build());

    Table table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("duckdb_mutation_smoke")
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setConnectorId(connectorId)
                    .addNamespacePath("iceberg")
                    .setTableDisplayName("duckdb_mutation_smoke")
                    .setUri("s3://floecat/iceberg/duckdb_mutation_smoke"))
            .putAllProperties(
                Map.of(
                    "location", "s3://floecat/iceberg/duckdb_mutation_smoke",
                    "metadata-location",
                        "s3://floecat/iceberg/duckdb_mutation_smoke/metadata/00003-new.metadata.json"))
            .build();

    var result =
        service.resolveOrCreateForCommit(
            "acct-1",
            "tx-1",
            List.of("iceberg"),
            namespaceId,
            catalogId,
            "duckdb_mutation_smoke",
            tableId,
            table);

    assertEquals(connectorId, result.connectorId());
    assertFalse(result.connectorTxChanges().isEmpty());
    Connector refreshed =
        parseConnector(result.connectorTxChanges().get(0).getPayload().toByteArray());
    assertEquals(
        "s3://floecat/iceberg/duckdb_mutation_smoke/metadata/00003-new.metadata.json",
        refreshed.getPropertiesMap().get("external.metadata-location"));
    assertEquals("s3://floecat/iceberg/duckdb_mutation_smoke", refreshed.getUri());
  }

  private Connector parseConnector(byte[] payload) {
    try {
      return Connector.parseFrom(payload);
    } catch (InvalidProtocolBufferException e) {
      throw new AssertionError(e);
    }
  }
}
