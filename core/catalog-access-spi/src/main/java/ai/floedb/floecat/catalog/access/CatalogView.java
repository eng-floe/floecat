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

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Provider-neutral view discovery metadata. */
public record CatalogView(
    CatalogObjectName name,
    ExternalObjectIdentity identity,
    String outputSchemaJson,
    List<CatalogViewDefinition> definitions,
    NamespacePath defaultNamespace,
    Map<String, String> properties) {
  public CatalogView {
    name = Objects.requireNonNull(name, "name");
    identity = Objects.requireNonNull(identity, "identity");
    outputSchemaJson = Objects.requireNonNull(outputSchemaJson, "outputSchemaJson").trim();
    if (outputSchemaJson.isEmpty()) {
      throw new IllegalArgumentException("outputSchemaJson must not be blank");
    }
    definitions = List.copyOf(Objects.requireNonNull(definitions, "definitions"));
    defaultNamespace = Objects.requireNonNull(defaultNamespace, "defaultNamespace");
    properties = Map.copyOf(Objects.requireNonNull(properties, "properties"));
  }
}
