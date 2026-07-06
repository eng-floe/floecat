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

package ai.floedb.floecat.service.metagraph.resolver;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Per-call memo for the (catalog, namespace) scope a batch of relation names resolves within.
 * Shared by {@link FullyQualifiedResolver} and {@link NameResolver} so the same batch optimization
 * has one implementation rather than two that drift.
 *
 * <p>Catalogs are cached by name and namespaces by (catalog id + path), in separate maps, so a
 * catalog shared by many namespaces in the batch is resolved once. Construct one per top-level call
 * with lookups already bound to the correlation id and account; it is not thread-safe.
 */
final class ScopeMemo {

  private static final String DELIM = "\u001F";

  private final Function<String, Optional<Catalog>> catalogByName;
  private final BiFunction<Catalog, List<String>, Optional<Namespace>> namespaceByPath;

  private final Map<String, Optional<Catalog>> catalogs = new HashMap<>();
  private final Map<String, Optional<Namespace>> namespaces = new HashMap<>();

  ScopeMemo(
      Function<String, Optional<Catalog>> catalogByName,
      BiFunction<Catalog, List<String>, Optional<Namespace>> namespaceByPath) {
    this.catalogByName = catalogByName;
    this.namespaceByPath = namespaceByPath;
  }

  Optional<Catalog> catalog(String name) {
    return catalogs.computeIfAbsent(name, catalogByName);
  }

  Optional<Namespace> namespace(Catalog catalog, List<String> path) {
    String key = catalog.getResourceId().getId() + DELIM + String.join(DELIM, path);
    return namespaces.computeIfAbsent(key, k -> namespaceByPath.apply(catalog, path));
  }
}
