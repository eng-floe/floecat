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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class IcebergHttpUtil {
  private IcebergHttpUtil() {}

  public static String etagForMetadataLocation(String metadataLocation) {
    if (metadataLocation == null) {
      throw new IllegalArgumentException("Unable to generate etag for null metadataLocation");
    }
    return "W/\"" + sha256Hex(metadataLocation) + "\"";
  }

  private static String sha256Hex(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hashed = digest.digest(value.getBytes(StandardCharsets.UTF_8));
      StringBuilder out = new StringBuilder(hashed.length * 2);
      for (byte b : hashed) {
        out.append(Character.forDigit((b >> 4) & 0xF, 16));
        out.append(Character.forDigit(b & 0xF, 16));
      }
      return out.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }
}
