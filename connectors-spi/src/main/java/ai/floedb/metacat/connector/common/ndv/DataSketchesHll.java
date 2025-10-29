package ai.floedb.metacat.connector.common.ndv;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;

public final class DataSketchesHll implements Hll {

  private final HllSketch sketch;

  public DataSketchesHll() {
    this(12, TgtHllType.HLL_8);
  }

  public DataSketchesHll(int lgK, TgtHllType type) {
    this.sketch = new HllSketch(lgK, type);
  }

  private DataSketchesHll(HllSketch sketch) {
    this.sketch = sketch;
  }

  @Override
  public void addLong(long v) {
    sketch.update(v);
  }

  @Override
  public void addString(String s) {
    if (s != null) sketch.update(s);
  }

  @Override
  public long estimate() {
    return Math.round(sketch.getEstimate());
  }

  public byte[] toBytes() {
    return sketch.toCompactByteArray();
  }

  public static DataSketchesHll fromBytes(byte[] bytes) {
    return new DataSketchesHll(HllSketch.heapify(bytes));
  }

  public HllSketch raw() {
    return sketch;
  }
}
