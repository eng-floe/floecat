package ai.floedb.metacat.connector.delta.uc.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

final class DatabricksSpTokenProvider implements AccessTokenProvider {
  private static final ObjectMapper M = new ObjectMapper();
  private static final long SKEW_SECONDS = 60;

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofMillis(15000)).build();
  private final String host;
  private final String clientId;
  private final String clientSecret;
  private final String scope;
  private final int timeoutMs = 15000;
  private volatile String cachedAccess;
  private volatile Instant cachedExpiry;

  DatabricksSpTokenProvider(String host, String clientId, String clientSecret, String scope) {
    this.host = (host == null ? "" : host.trim()).replaceAll("/+$", "");
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.scope = (scope == null || scope.isBlank()) ? "all-apis" : scope;
  }

  @Override
  public synchronized String accessToken() {
    if (cachedAccess != null && !expiringSoon(cachedExpiry)) {
      return cachedAccess;
    }

    try {
      var tok = fetchNewAccessToken();
      this.cachedAccess = tok.value;
      this.cachedExpiry = tok.expiresAt;
      return cachedAccess;
    } catch (Exception e) {
      if (cachedAccess != null) {
        return cachedAccess;
      }

      throw new RuntimeException("Failed to obtain Databricks SP access token", e);
    }
  }

  private static boolean expiringSoon(Instant when) {
    return when == null || Instant.now().isAfter(when.minusSeconds(SKEW_SECONDS));
  }

  private Token fetchNewAccessToken() throws Exception {
    String form =
        "grant_type=client_credentials"
            + "&client_id="
            + enc(clientId)
            + "&client_secret="
            + enc(clientSecret)
            + "&scope="
            + enc(scope);

    HttpRequest req =
        HttpRequest.newBuilder(URI.create(host + "/oidc/v1/token"))
            .timeout(Duration.ofMillis(timeoutMs))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(form))
            .build();

    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() / 100 != 2) {
      throw new RuntimeException(
          "Databricks SP token request failed: HTTP " + resp.statusCode() + " " + resp.body());
    }
    JsonNode j = M.readTree(resp.body());
    String access = j.path("access_token").asText(null);
    long expiresIn = j.path("expires_in").asLong(3600);
    if (access == null || access.isBlank()) {
      throw new IllegalStateException("No access_token in Databricks SP token response");
    }
    return new Token(access, Instant.now().plusSeconds(Math.max(1, expiresIn)));
  }

  private record Token(String value, Instant expiresAt) {}

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }
}
