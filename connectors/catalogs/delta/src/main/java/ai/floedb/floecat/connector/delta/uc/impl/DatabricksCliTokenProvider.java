/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.connector.delta.uc.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;

final class DatabricksCliTokenProvider implements AccessTokenProvider {
  private static final ObjectMapper M = new ObjectMapper();
  private static final long SKEW_SECONDS = 60;

  private final HttpClient http;
  private final String host;
  private final Path cachePath;
  private final String clientId;
  private final String scope;
  private final int timeoutMs;

  private volatile String cachedAccess;
  private volatile Instant cachedExpiry;
  private volatile String cachedRefresh;

  DatabricksCliTokenProvider(String host, String cachePath, String clientId, String scope) {
    this.host = (host == null ? "" : host.trim()).replaceAll("/+$", "");
    this.cachePath =
        cachePath == null
            ? Path.of(System.getProperty("user.home"), ".databricks", "token-cache.json")
            : Path.of(cachePath);
    this.clientId = (clientId == null || clientId.isBlank()) ? "databricks-cli" : clientId;
    this.scope = (scope == null) ? "" : scope;
    this.timeoutMs = 15000;
    this.http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeoutMs)).build();
  }

  @Override
  public synchronized String accessToken() {
    String env = System.getenv("DATABRICKS_TOKEN");
    if (env != null && !env.isBlank()) {
      return env.trim();
    }
    if (cachedAccess != null && !expiringSoon(cachedExpiry)) {
      return cachedAccess;
    }

    try {
      var ct =
          readCacheForHost()
              .orElseThrow(() -> new IllegalStateException("No token. Run: databricks auth login"));
      cachedAccess = ct.accessToken;
      cachedRefresh = ct.refreshToken;
      cachedExpiry = ct.expiry;
    } catch (Exception e) {
      throw new RuntimeException("Failed to read Databricks token cache", e);
    }

    if (expiringSoon(cachedExpiry) && cachedRefresh != null && !cachedRefresh.isBlank()) {
      try {
        var r = refreshAccessToken(cachedRefresh);
        cachedAccess = r.accessToken;
        cachedRefresh = r.refreshToken;
        cachedExpiry = r.expiry;
        tryPersist(r);
      } catch (Exception ignore) {
      }
    }
    return cachedAccess;
  }

  private static boolean expiringSoon(Instant when) {
    return when == null || Instant.now().isAfter(when.minusSeconds(SKEW_SECONDS));
  }

  private record CacheTok(String accessToken, String refreshToken, Instant expiry) {}

  private Optional<CacheTok> readCacheForHost() throws Exception {
    if (!Files.exists(cachePath)) {
      return Optional.empty();
    }

    JsonNode root = M.readTree(Files.readString(cachePath));
    JsonNode tokens = root.path("tokens");
    if (!tokens.isObject()) {
      return Optional.empty();
    }

    JsonNode t = tokens.path(host);
    if (t.isMissingNode()) {
      t = tokens.path(host + "/");
    }

    if (t.isMissingNode()) {
      return Optional.empty();
    }

    String access = t.path("access_token").asText(null);
    String refresh = t.path("refresh_token").asText(null);
    Instant expiry = parseExpiry(t);
    if (access == null || access.isBlank()) {
      return Optional.empty();
    }

    return Optional.of(new CacheTok(access, refresh, expiry));
  }

  private CacheTok refreshAccessToken(String refreshToken) throws Exception {
    String body =
        "grant_type=refresh_token"
            + "&refresh_token="
            + enc(refreshToken)
            + "&client_id="
            + enc(clientId)
            + (scope.isBlank() ? "" : "&scope=" + enc(scope));

    HttpRequest req =
        HttpRequest.newBuilder(URI.create(host + "/oidc/v1/token"))
            .timeout(Duration.ofMillis(timeoutMs))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() / 100 != 2) {
      throw new RuntimeException("Refresh failed: HTTP " + resp.statusCode() + " " + resp.body());
    }

    JsonNode j = M.readTree(resp.body());
    String access = j.path("access_token").asText(null);
    long expiresIn = j.path("expires_in").asLong(3600);
    String newRefresh = j.path("refresh_token").asText(refreshToken);
    if (access == null || access.isBlank())
      throw new IllegalStateException("No access_token in refresh response");
    return new CacheTok(access, newRefresh, Instant.now().plusSeconds(expiresIn));
  }

  private void tryPersist(CacheTok t) {
    try {
      if (!Files.exists(cachePath)) {
        return;
      }

      JsonNode root = M.readTree(Files.readString(cachePath));
      if (!(root.get("tokens") instanceof com.fasterxml.jackson.databind.node.ObjectNode obj)) {
        return;
      }

      var me = obj.with(host);
      me.put("access_token", t.accessToken);
      if (t.refreshToken != null) {
        me.put("refresh_token", t.refreshToken);
      }

      if (t.expiry != null) {
        me.put("expiry", t.expiry.toString());
        long seconds = Math.max(1, Duration.between(Instant.now(), t.expiry).toSeconds());
        me.put("expires_in", seconds);
      }

      Files.createDirectories(cachePath.getParent());
      Files.writeString(cachePath, M.writerWithDefaultPrettyPrinter().writeValueAsString(root));
    } catch (Exception ignore) {
    }
  }

  private static Instant parseExpiry(JsonNode tokNode) {
    String expStr = tokNode.path("expiry").asText(null);
    if (expStr != null) {
      try {
        return OffsetDateTime.parse(expStr).toInstant();
      } catch (DateTimeParseException ignore) {
      }
    }
    long sec = tokNode.path("expires_in").asLong(0);
    return sec > 0 ? Instant.now().plusSeconds(sec) : null;
  }

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }
}
