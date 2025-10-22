package ai.floedb.metacat.connector.spi;

import java.util.Map;

public interface AuthProvider {
  String scheme();

  Map<String, String> apply(Map<String, String> baseProps);

  default Map<String, String> applyHeaders(Map<String, String> baseHeaders) {
    return baseHeaders;
  }
}
