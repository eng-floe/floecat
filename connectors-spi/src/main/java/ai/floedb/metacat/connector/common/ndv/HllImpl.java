package ai.floedb.metacat.connector.common.ndv;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.io.IOException;

public final class HllImpl implements Hll {
  private final HyperLogLog hll;

  public HllImpl(int precision) {
    this.hll = new HyperLogLog(precision);
  }

  @Override
  public void addLong(long v) {
    hll.offerHashed(v);
  }

  @Override
  public void addString(String s) {
    hll.offer(s);
  }

  @Override
  public long estimate() {
    return hll.cardinality();
  }

  @Override
  public byte[] toBytes() {
    try {
      return hll.getBytes();
    } catch (IOException ioe) {
      return null;
    }
  }

  public static long hash64(long x) {
    x ^= (x >>> 33);
    x *= 0xff51afd7ed558ccdL;
    x ^= (x >>> 33);
    x *= 0xc4ceb9fe1a85ec53L;
    x ^= (x >>> 33);
    return x;
  }
}
