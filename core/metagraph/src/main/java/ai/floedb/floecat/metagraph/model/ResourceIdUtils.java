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

package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;

/** Utility methods for working with {@link ResourceId} instances. */
public final class ResourceIdUtils {

  private ResourceIdUtils() {}

  /**
   * Returns {@code true} if the given {@link ResourceId} carries a non-blank string identity.
   * A default-instance or null id is considered to have no identity.
   */
  public static boolean hasIdentity(ResourceId id) {
    return id != null && id.getId() != null && !id.getId().isBlank();
  }
}
