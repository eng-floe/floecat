package ai.floedb.metacat.connector.auth;

import ai.floedb.metacat.connector.spi.AuthProvider;
import java.util.Map;

public final class NoAuthProvider implements AuthProvider {
  @Override
  public String scheme() {
    return "none";
  }

  @Override
  public Map<String, String> apply(Map<String, String> baseProps) {
    return baseProps;
  }

  @Override
  public Map<String, String> applyHeaders(Map<String, String> baseHeaders) {
    return baseHeaders;
  }
}
