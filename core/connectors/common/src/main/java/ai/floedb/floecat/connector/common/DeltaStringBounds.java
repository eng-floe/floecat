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
package ai.floedb.floecat.connector.common;

/** String bounds from a Delta log, as bounds that hold in code point order. */
public final class DeltaStringBounds {
  private DeltaStringBounds() {}

  /**
   * {@code max}, or, when it ends in U+FFFD, the least string above every string starting with what
   * precedes it; null when there is none.
   *
   * <p>Delta before 3.3.0 cuts a long maximum and ends it with U+FFFD, comparing UTF-16 code units,
   * so a character past the Basic Multilingual Plane, above U+FFFD in code point order, can follow
   * the cut. A surrogate pair the cut splits is written as '?', so a '?' before the U+FFFD goes
   * too.
   */
  public static String max(String max) {
    if (!max.endsWith("\uFFFD")) {
      return max;
    }
    int end = max.length() - 1;
    if (end > 0 && max.charAt(end - 1) == '?') {
      end--;
    }
    while (end > 0) {
      int codePoint = max.codePointBefore(end);
      end -= Character.charCount(codePoint);
      if (codePoint < Character.MAX_CODE_POINT && !isSurrogate(codePoint)) {
        int next =
            codePoint + 1 == Character.MIN_SURROGATE ? Character.MAX_SURROGATE + 1 : codePoint + 1;
        return max.substring(0, end) + Character.toString(next);
      }
    }
    return null;
  }

  private static boolean isSurrogate(int codePoint) {
    return codePoint >= Character.MIN_SURROGATE && codePoint <= Character.MAX_SURROGATE;
  }
}
