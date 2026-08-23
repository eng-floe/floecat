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

package ai.floedb.floecat.tools.transfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.LookupCatalogResponse;
import ai.floedb.floecat.catalog.rpc.LookupNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.LookupTableResponse;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import org.junit.jupiter.api.Test;

class PortableConnectorSpecsTest {
  @Test
  void convertsResolvedDestinationIdsToPortableNames() {
    var directory = mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
    when(directory.lookupCatalog(any()))
        .thenReturn(LookupCatalogResponse.newBuilder().setDisplayName("catalog").build());
    when(directory.lookupNamespace(any()))
        .thenReturn(
            LookupNamespaceResponse.newBuilder()
                .setRef(NameRef.newBuilder().addPath("parent").setName("namespace"))
                .build());
    when(directory.lookupTable(any()))
        .thenReturn(
            LookupTableResponse.newBuilder()
                .setName(NameRef.newBuilder().setName("table"))
                .build());
    var destination =
        DestinationTarget.newBuilder()
            .setCatalogId(ResourceId.newBuilder().setId("catalog-id"))
            .setNamespaceId(ResourceId.newBuilder().setId("namespace-id"))
            .setTableId(ResourceId.newBuilder().setId("table-id"))
            .build();

    var portable =
        PortableConnectorSpecs.from(connectorWithDestination(destination), directory)
            .getDestination();

    assertThat(portable.getCatalogDisplayName()).isEqualTo("catalog");
    assertThat(portable.getNamespace().getSegmentsList()).containsExactly("parent", "namespace");
    assertThat(portable.getTableDisplayName()).isEqualTo("table");
  }

  @Test
  void rejectsUnresolvedCatalogId() {
    var directory = mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
    when(directory.lookupCatalog(any())).thenReturn(LookupCatalogResponse.getDefaultInstance());

    assertThatThrownBy(
            () ->
                PortableConnectorSpecs.from(
                    connectorWithDestination(
                        DestinationTarget.newBuilder()
                            .setCatalogId(ResourceId.newBuilder().setId("catalog-id"))
                            .build()),
                    directory))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("catalog-id");
  }

  @Test
  void rejectsUnresolvedNamespaceId() {
    var directory = mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
    when(directory.lookupNamespace(any())).thenReturn(LookupNamespaceResponse.getDefaultInstance());

    assertThatThrownBy(
            () ->
                PortableConnectorSpecs.from(
                    connectorWithDestination(
                        DestinationTarget.newBuilder()
                            .setCatalogDisplayName("catalog")
                            .setNamespaceId(ResourceId.newBuilder().setId("namespace-id"))
                            .build()),
                    directory))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("namespace-id");
  }

  @Test
  void rejectsUnresolvedTableId() {
    var directory = mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
    when(directory.lookupTable(any())).thenReturn(LookupTableResponse.getDefaultInstance());

    assertThatThrownBy(
            () ->
                PortableConnectorSpecs.from(
                    connectorWithDestination(
                        DestinationTarget.newBuilder()
                            .setCatalogDisplayName("catalog")
                            .setTableId(ResourceId.newBuilder().setId("table-id"))
                            .build()),
                    directory))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("table-id");
  }

  @Test
  void validatesResolvableCatalogBeforeImport() {
    var directory = mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
    when(directory.resolveCatalog(any()))
        .thenReturn(
            ResolveCatalogResponse.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("catalog-id"))
                .build());
    var spec =
        ConnectorSpec.newBuilder()
            .setDestination(DestinationTarget.newBuilder().setCatalogDisplayName("catalog"))
            .build();

    PortableConnectorSpecs.validateForImport(spec, directory);
  }

  @Test
  void rejectsUnresolvedCatalogBeforeImport() {
    var directory = mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
    when(directory.resolveCatalog(any())).thenReturn(ResolveCatalogResponse.getDefaultInstance());
    var spec =
        ConnectorSpec.newBuilder()
            .setDestination(DestinationTarget.newBuilder().setCatalogDisplayName("missing"))
            .build();

    assertThatThrownBy(() -> PortableConnectorSpecs.validateForImport(spec, directory))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing");
  }

  private static Connector connectorWithDestination(DestinationTarget destination) {
    return Connector.newBuilder().setDestination(destination).build();
  }
}
