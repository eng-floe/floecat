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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;

/** Shared hashing helpers for cross-module stable digest generation. */
public final class Hashing {
  private Hashing() {}

  /**
   * Stable hex SHA-256 of a string's UTF-8 bytes. The argument must be non-null: this hashes cache
   * keys and identity tokens, where coercing null to "" would silently collide a "missing" input
   * with a genuinely-empty one. Callers that mean "empty" must pass "".
   */
  public static String sha256Hex(String value) {
    Objects.requireNonNull(value, "sha256Hex value must not be null");
    return sha256Hex(value.getBytes(StandardCharsets.UTF_8));
  }

  public static String sha256Hex(byte[] bytes) {
    try {
      var md = MessageDigest.getInstance("SHA-256");
      var dig = md.digest(bytes);
      var sb = new StringBuilder(dig.length * 2);
      for (byte x : dig) {
        sb.append(String.format("%02x", x));
      }
      return sb.toString();
    } catch (Exception e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}
