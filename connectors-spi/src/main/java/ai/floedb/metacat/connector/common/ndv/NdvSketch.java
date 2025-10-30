package ai.floedb.metacat.connector.common.ndv;

import java.util.HashMap;
import java.util.Map;

public final class NdvSketch {
  public String type;
  public byte[] data;
  public String encoding;
  public String compression;
  public Integer version;
  public Map<String, String> params = new HashMap<>();
}
