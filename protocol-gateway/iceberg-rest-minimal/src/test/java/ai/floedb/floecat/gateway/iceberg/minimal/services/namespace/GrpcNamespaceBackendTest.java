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

package ai.floedb.floecat.gateway.iceberg.minimal.services.namespace;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcWithHeaders;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class GrpcNamespaceBackendTest {
  private final GrpcWithHeaders grpc = Mockito.mock(GrpcWithHeaders.class);
  private final GrpcClients clients = Mockito.mock(GrpcClients.class);
  private final MinimalGatewayConfig config = Mockito.mock(MinimalGatewayConfig.class);
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directory =
      Mockito.mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
  private final NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace =
      Mockito.mock(NamespaceServiceGrpc.NamespaceServiceBlockingStub.class);

  private final GrpcNamespaceBackend backend = new GrpcNamespaceBackend(grpc, config);

  @Test
  void createUsesOnlyParentPathInNamespaceSpec() {
    when(config.catalogMapping()).thenReturn(Map.of());
    when(config.defaultPrefix()).thenReturn(java.util.Optional.empty());
    when(grpc.raw()).thenReturn(clients);
    when(clients.directory()).thenReturn(directory);
    when(clients.namespace()).thenReturn(namespace);
    when(grpc.withHeaders(directory)).thenReturn(directory);
    when(grpc.withHeaders(namespace)).thenReturn(namespace);
    when(directory.resolveCatalog(any()))
        .thenReturn(
            ResolveCatalogResponse.newBuilder()
                .setResourceId(
                    ResourceId.newBuilder().setAccountId("acct-1").setId("cat-1").build())
                .build());
    when(namespace.createNamespace(any()))
        .thenReturn(
            CreateNamespaceResponse.newBuilder()
                .setNamespace(Namespace.newBuilder().addParents("parent").setDisplayName("child"))
                .build());

    backend.create("examples", List.of("parent", "child"), Map.of("owner", "team-a"), "idem-1");

    ArgumentCaptor<CreateNamespaceRequest> captor =
        ArgumentCaptor.forClass(CreateNamespaceRequest.class);
    Mockito.verify(namespace).createNamespace(captor.capture());
    CreateNamespaceRequest request = captor.getValue();
    assertEquals(List.of("parent"), request.getSpec().getPathList());
    assertEquals("child", request.getSpec().getDisplayName());
    assertEquals("team-a", request.getSpec().getPropertiesOrThrow("owner"));
  }
}
