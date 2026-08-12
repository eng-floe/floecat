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

package ai.floedb.floecat.catalog.access;

import java.util.Objects;

public record CatalogObjectName(NamespacePath namespace, String name)
    implements Comparable<CatalogObjectName> {
  public CatalogObjectName {
    namespace = Objects.requireNonNull(namespace, "namespace");
    name = Objects.requireNonNull(name, "name").trim();
    if (name.isEmpty()) {
      throw new IllegalArgumentException("name must not be blank");
    }
  }

  @Override
  public int compareTo(CatalogObjectName other) {
    int namespaceComparison = namespace.compareTo(other.namespace);
    return namespaceComparison == 0 ? name.compareTo(other.name) : namespaceComparison;
  }

  @Override
  public String toString() {
    return namespace.segments().isEmpty() ? name : namespace + "." + name;
  }
}
