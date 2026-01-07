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

package ai.floedb.floecat.systemcatalog.spi.types;

import ai.floedb.floecat.metagraph.model.TypeNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Lookup helper that indexes {@link TypeNode}s by their canonical name. */
public final class SystemTypeLookup implements TypeLookup {

  private final Map<String, TypeNode> types;

  public SystemTypeLookup(List<TypeNode> nodes) {
    Objects.requireNonNull(nodes, "nodes");
    Map<String, TypeNode> index = new HashMap<>();
    for (TypeNode node : nodes) {
      String canonical = canonical(node.displayName());
      if (!canonical.isEmpty()) {
        index.put(canonical, node);
      }
    }
    this.types = Map.copyOf(index);
  }

  @Override
  public Optional<TypeNode> findByName(String namespace, String name) {
    String key = canonical(namespace, name);
    if (key.isEmpty()) {
      return Optional.empty();
    }
    return Optional.ofNullable(types.get(key));
  }

  private static String canonical(String namespace, String name) {
    String normalizedName = canonical(name);
    if (normalizedName.isEmpty()) {
      return "";
    }
    String ns = canonical(namespace);
    return ns.isEmpty() ? normalizedName : ns + "." + normalizedName;
  }

  private static String canonical(String value) {
    if (value == null) {
      return "";
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? "" : trimmed.toLowerCase();
  }
}
