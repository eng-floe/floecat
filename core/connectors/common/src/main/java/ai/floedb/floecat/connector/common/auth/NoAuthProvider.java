package ai.floedb.floecat.connector.common.auth;

import ai.floedb.floecat.connector.spi.AuthProvider;
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
