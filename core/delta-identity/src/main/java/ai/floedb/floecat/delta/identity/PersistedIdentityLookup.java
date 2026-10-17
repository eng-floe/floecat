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

package ai.floedb.floecat.delta.identity;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import java.util.Map;
import java.util.Optional;

/** Lazy, bounded access to persisted identity state for one table. */
public interface PersistedIdentityLookup {
  Optional<ColumnIdentityMap> exact(long version);

  Optional<Map.Entry<Long, ColumnIdentityMap>> floor(long version);

  Optional<Map.Entry<Long, ColumnIdentityMap>> ceiling(long version);

  Optional<ColumnIdentityMap> tableMap();

  long highWaterMark();
}
