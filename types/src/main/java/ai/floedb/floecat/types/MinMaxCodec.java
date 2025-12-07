package ai.floedb.floecat.types;

public final class MinMaxCodec {
  private MinMaxCodec() {}

  public static String encode(LogicalType t, Object value) {
    return ValueEncoders.encodeToString(t, value);
  }

  public static Object decode(LogicalType t, String encoded) {
    return ValueEncoders.decodeFromString(t, encoded);
  }
}
