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

package ai.floedb.floecat.types;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class LogicalTypeFormat {
  private static final Pattern DECIMAL_RE =
      Pattern.compile(
          "^\\s*DECIMAL\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)\\s*$", Pattern.CASE_INSENSITIVE);

  public static String format(LogicalType t) {
    Objects.requireNonNull(t, "LogicalType");
    if (t.isDecimal()) {
      return "DECIMAL(" + t.precision() + "," + t.scale() + ")";
    }
    return t.kind().name();
  }

  public static LogicalType parse(String s) {
    Objects.requireNonNull(s, "logical type string");
    String trimmed = s.trim();

    Matcher m = DECIMAL_RE.matcher(trimmed);
    if (m.matches()) {
      int p = Integer.parseInt(m.group(1));
      int sc = Integer.parseInt(m.group(2));
      return LogicalType.decimal(p, sc);
    }

    String upper = trimmed.toUpperCase(Locale.ROOT);
    try {
      LogicalKind k = LogicalKind.valueOf(upper);
      return LogicalType.of(k);
    } catch (IllegalArgumentException ignore) {
      // ignore
    }

    throw new IllegalArgumentException("Unrecognized logical type: \"" + s + "\"");
  }
}
