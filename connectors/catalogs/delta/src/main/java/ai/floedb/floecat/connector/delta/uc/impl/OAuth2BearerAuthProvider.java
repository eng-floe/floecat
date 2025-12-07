package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.connector.spi.AuthProvider;
import java.util.HashMap;
import java.util.Map;

@FunctionalInterface
interface AccessTokenProvider extends AutoCloseable {
  String accessToken();

  @Override
  default void close() {}
}

public final class OAuth2BearerAuthProvider implements AuthProvider, AutoCloseable {
  private final AccessTokenProvider tokens;

  public OAuth2BearerAuthProvider(AccessTokenProvider tokens) {
    this.tokens = tokens;
  }

  @Override
  public String scheme() {
    return "oauth2";
  }

  @Override
  public Map<String, String> apply(Map<String, String> baseProps) {
    var p = new HashMap<>(baseProps);
    p.put("rest.auth.type", "oauth");
    p.put("rest.oauth2.token", tokens.accessToken());
    return p;
  }

  @Override
  public Map<String, String> applyHeaders(Map<String, String> baseHeaders) {
    var h = new HashMap<>(baseHeaders);
    h.put("Authorization", "Bearer " + tokens.accessToken());
    return h;
  }

  @Override
  public void close() {
    try {
      tokens.close();
    } catch (Exception ignore) {
    }
  }
}
