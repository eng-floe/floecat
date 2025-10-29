package ai.floedb.metacat.types;

import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.UpstreamStamp;
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
      ai.floedb.metacat.catalog.rpc.TableFormat system,
      String tableNativeId,
      String commitRef,
      Timestamp fetchedAt,
      Map<String, String> extras) {

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

    if (extras != null && !extras.isEmpty()) {
      b.putAllExtras(extras);
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
