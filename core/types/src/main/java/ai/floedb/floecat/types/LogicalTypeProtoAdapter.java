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

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import com.google.protobuf.Timestamp;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class LogicalTypeProtoAdapter {

  private LogicalTypeProtoAdapter() {}

  public static String encodeLogicalType(LogicalType t) {
    Objects.requireNonNull(t, "logical type");
    if (t.kind == LogicalKind.DECIMAL) {
      int p = t.precision != null ? t.precision : 38;
      int s = t.scale != null ? t.scale : 0;
      return "DECIMAL(" + p + "," + s + ")";
    }
    return t.kind.name();
  }

  public static LogicalType decodeLogicalType(String s) {
    if (s == null || s.isBlank()) {
      return LogicalType.of(LogicalKind.STRING);
    }

    String u = s.trim().toUpperCase(Locale.ROOT);
    if (u.startsWith("DECIMAL")) {
      int l = u.indexOf('(');
      int r = u.indexOf(')');
      if (l > 0 && r > l) {
        String body = u.substring(l + 1, r);
        String[] ps = body.split(",");
        int p = Integer.parseInt(ps[0].trim());
        int sc = (ps.length > 1) ? Integer.parseInt(ps[1].trim()) : 0;
        return LogicalType.decimal(p, sc);
      }
      return LogicalType.decimal(38, 0);
    }
    try {
      return LogicalType.of(LogicalKind.valueOf(u));
    } catch (IllegalArgumentException e) {
      return LogicalType.of(LogicalKind.STRING);
    }
  }

  public static String encodeValue(LogicalType type, Object value) {
    if (value == null) {
      return "";
    }

    return ValueEncoders.encodeToString(type, value);
  }

  public static Object decodeValue(LogicalType type, String encoded) {
    if (encoded == null || encoded.isBlank()) {
      return null;
    }

    return ValueEncoders.decodeFromString(type, encoded);
  }

  public static UpstreamStamp upstreamStamp(
      TableFormat system,
      String tableNativeId,
      String commitRef,
      Timestamp fetchedAt,
      Map<String, String> properties) {

    UpstreamStamp.Builder b = UpstreamStamp.newBuilder().setSystem(system);
    if (tableNativeId != null) {
      b.setTableNativeId(tableNativeId);
    }

    if (commitRef != null) {
      b.setCommitRef(commitRef);
    }

    if (fetchedAt != null) {
      b.setFetchedAt(fetchedAt);
    }

    if (properties != null && !properties.isEmpty()) {
      b.putAllProperties(properties);
    }

    return b.build();
  }

  public static LogicalType columnLogicalType(ColumnStats cs) {
    return decodeLogicalType(cs.getLogicalType());
  }

  public static Object columnMin(ColumnStats cs) {
    LogicalType t = columnLogicalType(cs);
    return decodeValue(t, cs.getMin());
  }

  public static Object columnMax(ColumnStats cs) {
    LogicalType t = columnLogicalType(cs);
    return decodeValue(t, cs.getMax());
  }

  public static int compareEncoded(LogicalType type, String a, String b) {
    Object va = decodeValue(type, a);
    Object vb = decodeValue(type, b);
    return LogicalComparators.compare(type, va, vb);
  }
}
