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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.NamespaceInfoDto;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class NamespaceResponseMapper {
  private NamespaceResponseMapper() {}

  public static NamespaceInfoDto toInfo(Namespace ns) {
    List<String> path = toPath(ns);
    Map<String, String> props = new LinkedHashMap<>(ns.getPropertiesMap());
    if (ns.hasDescription() && ns.getDescription() != null && !ns.getDescription().isBlank()) {
      props.putIfAbsent("description", ns.getDescription());
    }
    if (ns.hasPolicyRef() && ns.getPolicyRef() != null && !ns.getPolicyRef().isBlank()) {
      props.putIfAbsent("policy_ref", ns.getPolicyRef());
    }
    return new NamespaceInfoDto(path, Map.copyOf(props));
  }

  public static List<String> toPath(Namespace ns) {
    return ns.getParentsList().isEmpty()
        ? List.of(ns.getDisplayName())
        : concat(ns.getParentsList(), ns.getDisplayName());
  }

  private static List<String> concat(List<String> parents, String name) {
    List<String> out = new ArrayList<>(parents);
    out.add(name);
    return out;
  }
}
