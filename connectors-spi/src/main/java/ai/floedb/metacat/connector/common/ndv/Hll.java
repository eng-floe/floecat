package ai.floedb.metacat.connector.common.ndv;

public interface Hll {
  void addLong(long v);

  void addString(String s);

  long estimate();

  byte[] toBytes();
}
