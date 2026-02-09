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

package ai.floedb.floecat.connector.common.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public final class OAuthRefreshSupport {
  private static final ObjectMapper M = new ObjectMapper();

  private OAuthRefreshSupport() {}

  public static TokenResponse refreshAccessToken(
      HttpClient http,
      String endpoint,
      String clientId,
      String scope,
      String refreshToken,
      int timeoutMs)
      throws Exception {
    String body =
        "grant_type=refresh_token"
            + "&refresh_token="
            + enc(refreshToken)
            + "&client_id="
            + enc(clientId)
            + (scope == null || scope.isBlank() ? "" : "&scope=" + enc(scope));

    HttpRequest req =
        HttpRequest.newBuilder(URI.create(endpoint))
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
    if (access == null || access.isBlank()) {
      throw new IllegalStateException("No access_token in refresh response");
    }
    return new TokenResponse(access, newRefresh, expiresIn);
  }

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }

  public record TokenResponse(String accessToken, String refreshToken, long expiresInSeconds) {}
}
