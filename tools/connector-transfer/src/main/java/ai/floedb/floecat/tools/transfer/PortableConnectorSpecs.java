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
      var response =
          directory.lookupCatalog(
              LookupCatalogRequest.newBuilder().setResourceId(destination.getCatalogId()).build());
      String displayName = response.getDisplayName().trim();
      if (displayName.isBlank()) {
        throw unresolved("catalog", destination.getCatalogId().getId());
      }
      portable.setCatalogDisplayName(displayName);
    } else if (destination.hasCatalogDisplayName()) {
      String displayName = destination.getCatalogDisplayName().trim();
      if (displayName.isBlank()) {
        throw new IllegalArgumentException("destination catalog display name is blank");
      }
      portable.setCatalogDisplayName(displayName);
    }

    if (destination.hasNamespaceId()) {
      var response =
          directory.lookupNamespace(
              LookupNamespaceRequest.newBuilder()
                  .setResourceId(destination.getNamespaceId())
                  .build());
      if (!response.hasRef()) {
        throw unresolved("namespace", destination.getNamespaceId().getId());
      }
      var ref = response.getRef();
      var segments = new ArrayList<>(ref.getPathList());
      if (!ref.getName().isBlank()) segments.add(ref.getName());
      if (segments.isEmpty() || segments.stream().anyMatch(String::isBlank)) {
        throw unresolved("namespace", destination.getNamespaceId().getId());
      }
      portable.setNamespace(NamespacePath.newBuilder().addAllSegments(segments));
    } else if (destination.hasNamespace()) {
      if (destination.getNamespace().getSegmentsList().stream().anyMatch(String::isBlank)) {
        throw new IllegalArgumentException("destination namespace contains a blank segment");
      }
      portable.setNamespace(destination.getNamespace());
    }

    if (destination.hasTableId()) {
      var response =
          directory.lookupTable(
              LookupTableRequest.newBuilder().setResourceId(destination.getTableId()).build());
      if (!response.hasName() || response.getName().getName().isBlank()) {
        throw unresolved("table", destination.getTableId().getId());
      }
      portable.setTableDisplayName(response.getName().getName().trim());
    } else if (destination.hasTableDisplayName()) {
      String displayName = destination.getTableDisplayName().trim();
      if (displayName.isBlank()) {
        throw new IllegalArgumentException("destination table display name is blank");
      }
      portable.setTableDisplayName(displayName);
    }
    return portable.build();
  }

  private static IllegalArgumentException unresolved(String kind, String id) {
    return new IllegalArgumentException(
        "cannot export connector: destination " + kind + " id could not be resolved: " + id);
  }
}
