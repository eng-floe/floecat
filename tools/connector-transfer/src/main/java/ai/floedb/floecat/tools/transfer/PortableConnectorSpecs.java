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

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.floecat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.LookupTableRequest;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import java.util.ArrayList;

final class PortableConnectorSpecs {
  private PortableConnectorSpecs() {}

  static ConnectorSpec from(
      Connector connector, DirectoryServiceGrpc.DirectoryServiceBlockingStub directory) {
    var spec =
        ConnectorSpec.newBuilder()
            .setDisplayName(connector.getDisplayName())
            .setKind(connector.getKind())
            .setUri(connector.getUri())
            .setState(connector.getState())
            .putAllProperties(connector.getPropertiesMap());
    if (connector.hasDescription()) spec.setDescription(connector.getDescription());
    if (connector.hasSource()) spec.setSource(connector.getSource());
    if (connector.hasPolicy()) spec.setPolicy(connector.getPolicy());
    if (connector.hasAuth()) spec.setAuth(connector.getAuth());
    if (connector.hasDestination()) {
      spec.setDestination(portableDestination(connector.getDestination(), directory));
    }
    return spec.build();
  }

  private static DestinationTarget portableDestination(
      DestinationTarget destination, DirectoryServiceGrpc.DirectoryServiceBlockingStub directory) {
    var portable = DestinationTarget.newBuilder();
    if (destination.hasCatalogId()) {
      portable.setCatalogDisplayName(
          directory
              .lookupCatalog(
                  LookupCatalogRequest.newBuilder()
                      .setResourceId(destination.getCatalogId())
                      .build())
              .getDisplayName());
    } else if (destination.hasCatalogDisplayName()) {
      portable.setCatalogDisplayName(destination.getCatalogDisplayName());
    }

    if (destination.hasNamespaceId()) {
      var ref =
          directory
              .lookupNamespace(
                  LookupNamespaceRequest.newBuilder()
                      .setResourceId(destination.getNamespaceId())
                      .build())
              .getRef();
      var segments = new ArrayList<>(ref.getPathList());
      if (!ref.getName().isBlank()) segments.add(ref.getName());
      portable.setNamespace(NamespacePath.newBuilder().addAllSegments(segments));
    } else if (destination.hasNamespace()) {
      portable.setNamespace(destination.getNamespace());
    }

    if (destination.hasTableId()) {
      portable.setTableDisplayName(
          directory
              .lookupTable(
                  LookupTableRequest.newBuilder().setResourceId(destination.getTableId()).build())
              .getName()
              .getName());
    } else if (destination.hasTableDisplayName()) {
      portable.setTableDisplayName(destination.getTableDisplayName());
    }
    return portable.build();
  }
}
