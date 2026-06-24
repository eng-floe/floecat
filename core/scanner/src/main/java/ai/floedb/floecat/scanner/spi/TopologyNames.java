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

package ai.floedb.floecat.scanner.spi;

import java.util.ArrayList;
import java.util.List;

/** Shared name rendering for lightweight topology refs. */
public final class TopologyNames {

  private TopologyNames() {}

  /**
   * Builds the information_schema namespace name from path segments and the leaf display name.
   *
   * <p>The leaf is appended only when not already present as the last path segment.
   */
  public static String namespaceName(List<String> pathSegments, String displayName) {
    List<String> segments = new ArrayList<>(pathSegments == null ? List.of() : pathSegments);
    if (displayName != null
        && !displayName.isBlank()
        && (segments.isEmpty() || !displayName.equals(segments.get(segments.size() - 1)))) {
      segments.add(displayName);
    }
    return String.join(".", segments);
  }
}
