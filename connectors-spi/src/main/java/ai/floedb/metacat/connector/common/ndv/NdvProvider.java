package ai.floedb.metacat.connector.common.ndv;

import java.util.Map;

public interface NdvProvider {
  void contributeNdv(String filePath, Map<String, Hll> sinks) throws Exception;
}
