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

package ai.floedb.floecat.client.cli.util;

/** Quoting/unquoting helpers used by command parsing and output formatting. */
public final class Quotes {
  private Quotes() {}

  public static boolean needsQuotes(String s) {
    if (s == null || s.isEmpty()) {
      return true;
    }
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (Character.isWhitespace(c) || c == '.' || c == '"' || c == '\'' || c == '\\') {
        return true;
      }
    }
    return false;
  }

  public static String quoteIfNeeded(String s) {
    if (s == null) {
      return null;
    }
    if (!needsQuotes(s)) {
      return s;
    }
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }

  public static String unquote(String s) {
    if (s == null) {
      return null;
    }
    s = s.trim();
    if (s.isEmpty()) {
      return s;
    }
    if (!((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'")))) {
      return s;
    }
    char q = s.charAt(0);
    String body = s.substring(1, s.length() - 1);
    StringBuilder out = new StringBuilder(body.length());
    for (int i = 0; i < body.length(); i++) {
      char c = body.charAt(i);
      if (c == '\\' && i + 1 < body.length()) {
        char n = body.charAt(++i);
        if (n == '\\' || n == q) {
          out.append(n);
          continue;
        }
        out.append('\\').append(n);
        continue;
      }
      out.append(c);
    }
    return out.toString();
  }
}
