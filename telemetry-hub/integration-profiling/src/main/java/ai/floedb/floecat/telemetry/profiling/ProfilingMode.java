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

package ai.floedb.floecat.telemetry.profiling;

import java.util.Locale;

public enum ProfilingMode {
  JFR("jfr");

  private final String value;

  ProfilingMode(String value) {
    this.value = value;
  }

  public String value() {
    return value;
  }

  public static ProfilingMode from(String raw) {
    String normalized = normalize(raw);
    return JFR.value.equals(normalized) ? JFR : null;
  }

  public static String normalize(String raw) {
    if (raw == null) {
      return JFR.value;
    }
    String trimmed = raw.trim().toLowerCase(Locale.ROOT);
    return trimmed.isEmpty() ? JFR.value : trimmed;
  }
}
