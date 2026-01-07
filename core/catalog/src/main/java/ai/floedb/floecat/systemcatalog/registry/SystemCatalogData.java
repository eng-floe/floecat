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

package ai.floedb.floecat.systemcatalog.registry;

import ai.floedb.floecat.systemcatalog.def.*;
import java.util.List;

/** Immutable representation of a builtin catalog after parsing from protobuf. */
public record SystemCatalogData(
    List<SystemFunctionDef> functions,
    List<SystemOperatorDef> operators,
    List<SystemTypeDef> types,
    List<SystemCastDef> casts,
    List<SystemCollationDef> collations,
    List<SystemAggregateDef> aggregates,
    List<SystemNamespaceDef> namespaces,
    List<SystemTableDef> tables,
    List<SystemViewDef> views) {

  public SystemCatalogData {
    functions = copy(functions);
    operators = copy(operators);
    types = copy(types);
    casts = copy(casts);
    collations = copy(collations);
    aggregates = copy(aggregates);
    namespaces = copy(namespaces);
    tables = copy(tables);
    views = copy(views);
  }

  private static <T> List<T> copy(List<T> values) {
    return List.copyOf(values == null ? List.of() : values);
  }

  public static SystemCatalogData empty() {
    return new SystemCatalogData(
        List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(),
        List.of());
  }
}
