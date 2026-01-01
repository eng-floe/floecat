package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.connector.common.auth.NoAuthProvider;
import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import java.util.Map;

public final class DatabricksAuthFactory {

  public static AuthProvider from(ConnectorConfig.Auth auth) {
    String scheme = auth.scheme() == null ? "none" : auth.scheme().toLowerCase();
    switch (scheme) {
      case "oauth2" -> {
        var props = auth.props();
        String mode = props.getOrDefault("mode", "static").toLowerCase();
        return switch (mode) {
          case "cli" ->
              new OAuth2BearerAuthProvider(
                  new DatabricksCliTokenProvider(
                      require(props, "host"),
                      props.getOrDefault(
                          "oauth.cache",
                          System.getProperty("user.home") + "/.databricks/token-cache.json"),
                      props.getOrDefault("oauth.client_id", ""),
                      props.getOrDefault("oauth.scope", "all-apis offline_access")));
          case "sp" ->
              new OAuth2BearerAuthProvider(
                  new DatabricksSpTokenProvider(
                      require(props, "host"),
                      require(props, "oauth.client_id"),
                      require(props, "oauth.client_secret"),
                      props.getOrDefault("oauth.scope", "all-apis")));
          case "wif" ->
              new OAuth2BearerAuthProvider(
                  new DatabricksWifTokenProvider(
                      require(props, "host"),
                      require(props, "oauth.client_id"),
                      props.getOrDefault("oauth.scope", "all-apis"),
                      props.get("oauth.subject_token"),
                      props.get("oauth.subject_token_file"),
                      props.getOrDefault(
                          "oauth.subject_token_type", "urn:ietf:params:oauth:token-type:jwt"),
                      props.get("oauth.requested_token_type"),
                      props.get("oauth.audience")));
          default -> new OAuth2BearerAuthProvider(() -> require(props, "token"));
        };
      }
      case "none" -> {
        return new NoAuthProvider();
      }
      default -> throw new IllegalArgumentException("Unsupported auth.scheme=" + scheme);
    }
  }

  private static String require(Map<String, String> m, String k) {
    String v = m.get(k);
    if (v == null || v.isBlank()) {
      throw new IllegalArgumentException("Missing auth property: " + k);
    }
    return v;
  }
}
