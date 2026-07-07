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

package ai.floedb.floecat.service.connector.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.connector.rpc.ConnectorDiscoveryTarget;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.DiscoverCatalogsRequest;
import ai.floedb.floecat.connector.rpc.DiscoverNamespacesRequest;
import ai.floedb.floecat.connector.rpc.DiscoverObjectsRequest;
import ai.floedb.floecat.connector.rpc.DiscoveryObjectKind;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import java.lang.reflect.Field;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class ConnectorsDiscoveryImplTest {

  private ConnectorsDiscoveryImpl service;
  private ConnectorDiscoveryTarget target;
  private FloecatConnector fakeConnector;

  @BeforeEach
  void setUp() throws Exception {
    service = new ConnectorsDiscoveryImpl();
    service.connectorRepo = mock(ConnectorRepository.class);
    service.catalogRepo = mock(CatalogRepository.class);
    service.namespaceRepo = mock(NamespaceRepository.class);
    service.tableRepo = mock(TableRepository.class);
    service.principalProvider = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.credentialResolver = mock(CredentialResolver.class);
    installBasePrincipal(service, service.principalProvider);

    when(service.principalProvider.get())
        .thenReturn(
            PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setCorrelationId("corr")
                .addPermissions("connector.manage")
                .build());

    target =
        ConnectorDiscoveryTarget.newBuilder()
            .setSpec(
                ConnectorSpec.newBuilder()
                    .setDisplayName("conn")
                    .setKind(ConnectorKind.CK_ICEBERG)
                    .setUri("s3://bucket/path"))
            .build();

    fakeConnector = mock(FloecatConnector.class);
    when(fakeConnector.listCatalogs()).thenReturn(List.of("prod_catalog", "dev_catalog"));
    when(fakeConnector.listNamespaces()).thenReturn(List.of("prod_catalog.orders"));
    when(fakeConnector.listTables("prod_catalog.orders")).thenReturn(List.of("line_items"));
    when(fakeConnector.listViews("prod_catalog.orders")).thenReturn(List.of("orders_view"));
  }

  @Test
  void discoverCatalogsReturnsCatalogsFromTheConnector() {
    try (MockedStatic<ConnectorFactory> factory = mockStatic(ConnectorFactory.class)) {
      factory.when(() -> ConnectorFactory.create(any())).thenReturn(fakeConnector);

      var response =
          service
              .discoverCatalogs(DiscoverCatalogsRequest.newBuilder().setTarget(target).build())
              .await()
              .indefinitely();

      assertTrue(response.getStatus().getOk());
      assertEquals(2, response.getCatalogsCount());
      assertEquals("prod_catalog", response.getCatalogs(0).getObjectName());
      assertEquals("dev_catalog", response.getCatalogs(1).getObjectName());
    }
  }

  @Test
  void discoverNamespacesReturnsNamespacesFromTheConnector() {
    try (MockedStatic<ConnectorFactory> factory = mockStatic(ConnectorFactory.class)) {
      factory.when(() -> ConnectorFactory.create(any())).thenReturn(fakeConnector);

      var response =
          service
              .discoverNamespaces(DiscoverNamespacesRequest.newBuilder().setTarget(target).build())
              .await()
              .indefinitely();

      assertTrue(response.getStatus().getOk());
      assertEquals(1, response.getNamespacesCount());
      var namespace = response.getNamespaces(0);
      assertEquals("prod_catalog", namespace.getCatalogName());
      assertEquals("orders", namespace.getDisplayName());
      assertEquals(List.of("orders"), namespace.getNamespaceSegmentsList());
    }
  }

  @Test
  void discoverObjectsReturnsTablesAndViewsFromTheConnector() {
    try (MockedStatic<ConnectorFactory> factory = mockStatic(ConnectorFactory.class)) {
      factory.when(() -> ConnectorFactory.create(any())).thenReturn(fakeConnector);

      var response =
          service
              .discoverObjects(
                  DiscoverObjectsRequest.newBuilder()
                      .setTarget(target)
                      .addNamespaceSegments("prod_catalog")
                      .addNamespaceSegments("orders")
                      .build())
              .await()
              .indefinitely();

      assertTrue(response.getStatus().getOk());
      assertEquals(2, response.getObjectsCount());
      var kinds = response.getObjectsList().stream().map(o -> o.getObjectKind()).toList();
      assertTrue(kinds.contains(DiscoveryObjectKind.DOK_TABLE));
      assertTrue(kinds.contains(DiscoveryObjectKind.DOK_VIEW));
    }
  }

  @Test
  void discoverObjectsResolvesFullyQualifiedNamespaceFromCatalogNameAndSetsCatalogNameOnObjects() {
    try (MockedStatic<ConnectorFactory> factory = mockStatic(ConnectorFactory.class)) {
      factory.when(() -> ConnectorFactory.create(any())).thenReturn(fakeConnector);

      var response =
          service
              .discoverObjects(
                  DiscoverObjectsRequest.newBuilder()
                      .setTarget(target)
                      .setCatalogName("prod_catalog")
                      .addNamespaceSegments("orders")
                      .build())
              .await()
              .indefinitely();

      assertTrue(response.getStatus().getOk());
      assertEquals(2, response.getObjectsCount());
      var objectNames = response.getObjectsList().stream().map(o -> o.getObjectName()).toList();
      assertTrue(objectNames.contains("line_items"));
      assertTrue(objectNames.contains("orders_view"));
      response
          .getObjectsList()
          .forEach(
              o -> {
                assertEquals("prod_catalog", o.getCatalogName());
                assertEquals(List.of("orders"), o.getNamespaceSegmentsList());
              });
    }
  }

  private static void installBasePrincipal(
      ConnectorsDiscoveryImpl service, PrincipalProvider principalProvider) {
    try {
      Field field =
          ai.floedb.floecat.service.common.BaseServiceImpl.class.getDeclaredField("principal");
      field.setAccessible(true);
      field.set(service, principalProvider);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError("Failed to inject BaseServiceImpl principal provider", e);
    }
  }
}
