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

package ai.floedb.floecat.connector.common;

import ai.floedb.floecat.connector.common.ndv.NdvProvider;
import ai.floedb.floecat.types.LogicalType;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public interface Planner<K> extends AutoCloseable, Iterable<PlannedFile<K>> {
  Map<K, String> columnNamesByKey();

  Map<K, LogicalType> logicalTypesByKey();

  NdvProvider ndvProvider();

  Set<K> columns();

  default Function<K, String> nameOf() {
    var byKey = columnNamesByKey();
    return byKey::get;
  }

  default Function<K, LogicalType> typeOf() {
    var byKey = logicalTypesByKey();
    return byKey::get;
  }

  @Override
  default void close() {}
}
