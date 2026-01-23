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

package ai.floedb.floecat.gateway.iceberg.rest.services.resolution;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class NamespacePaths {
  private static final char DELIMITER = 0x1F;

  private NamespacePaths() {}

  public static List<String> split(String namespace) {
    if (namespace == null || namespace.isBlank()) {
      return List.of();
    }

    String normalized = normalizeDelimiter(namespace);
    if (normalized.indexOf(DELIMITER) >= 0) {
      return splitOnDelimiter(normalized, DELIMITER);
    }
    return List.of(normalized);
  }

  public static String encode(List<String> parts) {
    if (parts == null || parts.isEmpty()) {
      return "";
    }
    return String.join(String.valueOf(DELIMITER), parts);
  }

  private static List<String> splitOnDelimiter(String text, char delimiter) {
    String[] raw = text.split(String.valueOf(delimiter), -1);
    return new ArrayList<>(Arrays.asList(raw));
  }

  private static String normalizeDelimiter(String value) {
    if (value.indexOf('%') >= 0) {
      try {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
      } catch (IllegalArgumentException e) {
        return value;
      }
    }
    return value;
  }
}
