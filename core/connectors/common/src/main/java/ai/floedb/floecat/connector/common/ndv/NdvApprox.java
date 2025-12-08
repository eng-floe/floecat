package ai.floedb.floecat.connector.common.ndv;

import java.util.HashMap;
import java.util.Map;

public final class NdvApprox {
  public Double estimate;
  public Double rse;
  public Double ciLower;
  public Double ciUpper;
  public Double ciLevel;
  public Long rowsSeen;
  public Long rowsTotal;
  public String method;
  public Map<String, String> params = new HashMap<>();
}
