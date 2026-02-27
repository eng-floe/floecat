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
import java.util.Map;

public final class TableMappingUtil {
  private TableMappingUtil() {}

  public static Map<String, Object> asObjectMap(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return null;
    }
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      out.put(entry.getKey().toString(), entry.getValue());
    }
    return out;
  }

  public static Long asLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    String text = value.toString();
    if (text.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(text);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static Integer asInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.intValue();
    }
    String text = value.toString();
    if (text.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static Integer maybeInt(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  public static Long maybeLong(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  public static Object firstNonNull(Object first, Object second) {
    return first != null ? first : second;
  }

  public static String asString(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  public static String firstNonBlank(String first, String second) {
    if (first != null && !first.isBlank()) {
      return first;
    }
    return (second == null || second.isBlank()) ? null : second;
  }

  public static Integer normalizeFormatVersion(Integer candidate, Integer fallback) {
    Integer resolved = candidate;
    if (resolved == null || resolved < 1) {
      resolved = fallback;
    }
    if (resolved == null || resolved < 1) {
      return 2;
    }
    return resolved;
  }

  public static Integer normalizeFormatVersionForSnapshots(
      Integer formatVersion, Long sequenceNumber) {
    Integer resolved = normalizeFormatVersion(formatVersion, null);
    if (sequenceNumber != null && sequenceNumber > 0 && resolved < 2) {
      return 2;
    }
    return resolved;
  }
}
