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

package ai.floedb.floecat.stats.identity;

import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

/** String codec used to carry explicit stats targets through reconcile scope payloads. */
public final class StatsTargetScopeCodec {

  private static final String TABLE = "table";
  private static final String COLUMN_PREFIX = "column:";
  private static final String FILE_PREFIX = "file:";
  private static final String EXPRESSION_PREFIX = "expression:";
  private static final String SEP = ":";

  private StatsTargetScopeCodec() {}

  public static String encode(StatsTarget target) {
    return switch (target.getTargetCase()) {
      case TABLE -> TABLE;
      case COLUMN -> COLUMN_PREFIX + target.getColumn().getColumnId();
      case FILE -> FILE_PREFIX + b64(StatsTargetIdentity.filePath(target.getFile().getFilePath()));
      case EXPRESSION -> {
        EngineExpressionStatsTarget normalized =
            StatsTargetIdentity.normalizeExpressionTarget(target.getExpression());
        yield EXPRESSION_PREFIX
            + b64(normalized.getEngineKind())
            + SEP
            + b64(normalized.getEngineExpressionKey().toByteArray())
            + SEP
            + b64(normalized.getEngineVersion());
      }
      case TARGET_NOT_SET -> throw new IllegalArgumentException("target is not set");
    };
  }

  public static Optional<StatsTarget> decode(String encoded) {
    if (encoded == null || encoded.isBlank()) {
      return Optional.empty();
    }
    if (TABLE.equals(encoded)) {
      return Optional.of(StatsTargetIdentity.tableTarget());
    }
    if (encoded.startsWith(COLUMN_PREFIX)) {
      try {
        long columnId = Long.parseLong(encoded.substring(COLUMN_PREFIX.length()));
        if (columnId <= 0L) {
          return Optional.empty();
        }
        return Optional.of(StatsTargetIdentity.columnTarget(columnId));
      } catch (RuntimeException ignored) {
        return Optional.empty();
      }
    }
    if (encoded.startsWith(FILE_PREFIX)) {
      try {
        String filePath = b64String(encoded.substring(FILE_PREFIX.length()));
        return Optional.of(StatsTargetIdentity.fileTarget(filePath));
      } catch (RuntimeException ignored) {
        return Optional.empty();
      }
    }
    if (!encoded.startsWith(EXPRESSION_PREFIX)) {
      return Optional.empty();
    }
    String payload = encoded.substring(EXPRESSION_PREFIX.length());
    String[] parts = payload.split(SEP, -1);
    if (parts.length < 2 || parts.length > 3) {
      return Optional.empty();
    }
    try {
      String kind = b64String(parts[0]);
      byte[] expressionKey = b64Bytes(parts[1]);
      String version = parts.length == 3 ? b64String(parts[2]) : "";
      EngineExpressionStatsTarget.Builder expressionBuilder =
          EngineExpressionStatsTarget.newBuilder()
              .setEngineKind(kind)
              .setEngineExpressionKey(ByteString.copyFrom(expressionKey));
      if (!version.isBlank()) {
        expressionBuilder.setEngineVersion(version);
      }
      return Optional.of(StatsTargetIdentity.expressionTarget(expressionBuilder.build()));
    } catch (RuntimeException ignored) {
      return Optional.empty();
    }
  }

  private static String b64(String value) {
    return b64(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String b64(byte[] value) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
  }

  private static String b64String(String value) {
    return new String(b64Bytes(value), StandardCharsets.UTF_8);
  }

  private static byte[] b64Bytes(String value) {
    return Base64.getUrlDecoder().decode(value);
  }
}
