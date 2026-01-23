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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ReservedPropertyUtil {
  private static final List<String> RESERVED_PREFIXES = List.of("polaris.");

  private ReservedPropertyUtil() {}

  public static Map<String, String> validateAndFilter(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Map.of();
    }
    Map<String, String> filtered = new LinkedHashMap<>();
    for (var entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key == null) {
        continue;
      }
      for (String prefix : RESERVED_PREFIXES) {
        if (key.startsWith(prefix)) {
          throw new IllegalArgumentException(
              "Property '" + key + "' matches reserved prefix '" + prefix + "'");
        }
      }
      filtered.put(key, entry.getValue());
    }
    return filtered;
  }
}
