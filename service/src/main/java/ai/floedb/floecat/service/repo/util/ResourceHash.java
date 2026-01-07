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

package ai.floedb.floecat.service.repo.util;

import java.security.MessageDigest;

public final class ResourceHash {
  private ResourceHash() {}

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
