package ai.floedb.metacat.connector.common.ndv;

public final class NdvHash {
  private NdvHash() {}

  public static long hash64(int v) {
    return mix64(v);
  }

  public static long hash64(long v) {
    return mix64(v);
  }

  public static long hash64(double d) {
    return mix64(Double.doubleToLongBits(d));
  }

  public static long hash64(float f) {
    return mix64(Float.floatToIntBits(f));
  }

  private static long mix64(long z) {
    z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
    z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
    return z ^ (z >>> 33);
  }
}
