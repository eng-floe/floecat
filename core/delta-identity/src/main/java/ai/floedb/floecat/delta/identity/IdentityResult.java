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
import java.util.List;
import java.util.Map;

/** Identity maps produced for one table together with table-scoped failures. */
public record IdentityResult(Map<Long, ColumnIdentityMap> maps, List<Failure> failures) {
  public IdentityResult {
    maps = Map.copyOf(maps);
    failures = List.copyOf(failures);
  }

  public boolean succeeded() {
    return failures.isEmpty();
  }

  public record Failure(String table, Long snapshotVersion, Kind kind, String message) {
    public enum Kind {
      INVALID_PERSISTED_STATE,
      UNSUPPORTED,
      SOURCE_ERROR
    }
  }
}
