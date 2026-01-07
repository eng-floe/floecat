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

import ai.floedb.floecat.catalog.rpc.TableStats;
import java.security.MessageDigest;
import java.util.TreeMap;

public final class TableStatsNormalizer {
  private TableStatsNormalizer() {}

  public static TableStats normalize(TableStats in) {
    var b = in.toBuilder();

    if (b.hasUpstream()) {
      var up = b.getUpstream().toBuilder();
      up.clearFetchedAt();
      up.clearCommitRef();
      b.setUpstream(up);
    }

    if (!b.getPropertiesMap().isEmpty()) {
      var sorted = new TreeMap<>(b.getPropertiesMap());
      b.clearProperties();
      b.putAllProperties(sorted);
    }

    return b.build();
  }

  public static String sha256Hex(byte[] bytes) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] dig = md.digest(bytes);
      StringBuilder sb = new StringBuilder(dig.length * 2);
      for (byte x : dig) {
        sb.append(String.format("%02x", x));
      }

      return sb.toString();
    } catch (Exception e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }
}
