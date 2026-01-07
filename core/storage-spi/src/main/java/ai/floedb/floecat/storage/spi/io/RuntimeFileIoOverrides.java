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

package ai.floedb.floecat.storage.spi.io;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public final class RuntimeFileIoOverrides {
  private static final String ENV_OVERRIDE_PREFIX = "FLOECAT_FILEIO_OVERRIDE_";
  private static final String SYS_OVERRIDE_PREFIX = "floecat.fileio.override.";

  private RuntimeFileIoOverrides() {}

  public static void mergeInto(Map<String, String> target) {
    if (target == null) {
      return;
    }
    Map<String, String> overrides = resolveOverrides();
    if (overrides.isEmpty()) {
      return;
    }
    overrides.forEach(
        (k, v) -> {
          if (k != null && !k.isBlank() && v != null && !v.isBlank()) {
            target.putIfAbsent(k, v);
          }
        });
  }

  private static Map<String, String> resolveOverrides() {
    Map<String, String> overrides = new LinkedHashMap<>();
    collectSystemOverrides(overrides);
    collectEnvironmentOverrides(overrides);
    return overrides;
  }

  private static void collectSystemOverrides(Map<String, String> overrides) {
    Properties props = System.getProperties();
    for (String name : props.stringPropertyNames()) {
      if (name.startsWith(SYS_OVERRIDE_PREFIX)) {
        String key = name.substring(SYS_OVERRIDE_PREFIX.length());
        addOverride(overrides, key, props.getProperty(name));
      } else if (isDirectFileIoProperty(name)) {
        addOverride(overrides, name, props.getProperty(name));
      }
    }
  }

  private static void collectEnvironmentOverrides(Map<String, String> overrides) {
    System.getenv()
        .forEach(
            (key, value) -> {
              if (key.startsWith(ENV_OVERRIDE_PREFIX)) {
                String property = key.substring(ENV_OVERRIDE_PREFIX.length());
                property = property.replace("__", "/").replace('_', '.').toLowerCase(Locale.ROOT);
                addOverride(overrides, property, value);
              }
            });
  }

  private static boolean isDirectFileIoProperty(String name) {
    return name.startsWith("s3.")
        || name.startsWith("s3a.")
        || name.startsWith("s3n.")
        || name.startsWith("fs.")
        || name.startsWith("client.")
        || name.startsWith("aws.")
        || name.startsWith("hadoop.")
        || "io-impl".equals(name);
  }

  private static void addOverride(Map<String, String> overrides, String key, String value) {
    if (key == null || key.isBlank() || value == null || value.isBlank()) {
      return;
    }
    overrides.putIfAbsent(key.trim(), value.trim());
  }
}
