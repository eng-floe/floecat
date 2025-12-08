package ai.floedb.floecat.connector.common.ndv;

import java.util.Map;

public interface NdvProvider {
  void contributeNdv(String filePath, Map<String, ColumnNdv> sinks) throws Exception;
}
