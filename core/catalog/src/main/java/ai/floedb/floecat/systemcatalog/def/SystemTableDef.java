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

package ai.floedb.floecat.systemcatalog.def;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.FlightEndpointRef;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public record SystemTableDef(
    NameRef name,
    String displayName,
    List<SystemColumnDef> columns,
    TableBackendKind backendKind,
    String scannerId,
    String storagePath,
    List<EngineSpecificRule> engineSpecific,
    FlightEndpointRef flightEndpoint)
    implements SystemObjectDef {

  public SystemTableDef {
    name = Objects.requireNonNull(name, "name");
    columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
    backendKind = Objects.requireNonNull(backendKind, "backendKind");
    scannerId = Objects.requireNonNull(scannerId, "scannerId");
    displayName = displayName == null ? "" : displayName;
    storagePath = storagePath == null ? "" : storagePath;
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
    Set<String> columnNames = new HashSet<>();
    for (SystemColumnDef column : columns) {
      if (!columnNames.add(column.name())) {
        throw new IllegalArgumentException(
            "duplicate column name '" + column.name() + "' in table " + name);
      }
    }
    if (backendKind == TableBackendKind.TABLE_BACKEND_KIND_FLOECAT && scannerId.isBlank()) {
      throw new IllegalArgumentException("scannerId is required for TABLE_BACKEND_KIND_FLOECAT");
    }
    if (backendKind == TableBackendKind.TABLE_BACKEND_KIND_STORAGE) {
      boolean hasPath = !storagePath.isBlank();
      boolean hasEndpoint = flightEndpoint != null;
      if (hasPath == hasEndpoint) {
        throw new IllegalArgumentException(
            "specify exactly one of storagePath or flightEndpoint for TABLE_BACKEND_KIND_STORAGE");
      }
    }
  }

  @Override
  public ResourceKind kind() {
    return ResourceKind.RK_TABLE;
  }
}
