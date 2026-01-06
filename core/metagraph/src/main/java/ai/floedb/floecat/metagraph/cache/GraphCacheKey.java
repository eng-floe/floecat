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

package ai.floedb.floecat.metagraph.cache;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.Objects;

/**
 * Cache key combining a resource identifier and its pointer version.
 *
 * <p>Pointer versions are monotonically increasing, so this key automatically invalidates cached
 * nodes when underlying metadata changes.
 */
public record GraphCacheKey(ResourceId id, long version) {

  public GraphCacheKey {
    Objects.requireNonNull(id, "id");
  }
}
