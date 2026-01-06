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
import ai.floedb.floecat.metagraph.model.TableBackendKind;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import java.util.List;
import java.util.Objects;

public record SystemTableDef(
    NameRef name,
    String displayName,
    List<SchemaColumn> columns,
    TableBackendKind backendKind,
    String scannerId,
    List<EngineSpecificRule> engineSpecific)
    implements SystemObjectDef {

  public SystemTableDef {
    name = Objects.requireNonNull(name, "name");
    columns = List.copyOf(Objects.requireNonNull(columns, "columns"));
    backendKind = Objects.requireNonNull(backendKind, "backendKind");
    scannerId = Objects.requireNonNull(scannerId, "scannerId");
    displayName = displayName == null ? "" : displayName;
    engineSpecific = List.copyOf(engineSpecific == null ? List.of() : engineSpecific);
  }

  @Override
  public ResourceKind kind() {
    return ResourceKind.RK_TABLE;
  }
}
