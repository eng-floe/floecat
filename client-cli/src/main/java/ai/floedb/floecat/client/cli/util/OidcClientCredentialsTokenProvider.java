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
package ai.floedb.floecat.client.cli.util;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** OIDC client credentials provider used by the shell for authorization token retrieval. */
public final class OidcClientCredentialsTokenProvider {
  private static final Pattern ACCESS_TOKEN_PATTERN =
      Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");
  private static final Pattern EXPIRES_IN_PATTERN =
      Pattern.compile("\"expires_in\"\\s*:\\s*(\\d+)");

  private final String tokenUrl;
  private final String clientId;
  private final String clientSecret;
  private final int refreshSkewSeconds;
  private final HttpClient http;
  private volatile String tokenValue = "";
  private volatile long expiresAtEpochSecond = 0L;
  private final boolean debug;

  public OidcClientCredentialsTokenProvider(
      String tokenUrl,
      String clientId,
      String clientSecret,
      int refreshSkewSeconds,
      boolean debug) {
    this.tokenUrl = tokenUrl;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.refreshSkewSeconds = refreshSkewSeconds;
    this.http = HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(5)).build();
    this.debug = debug;
  }

  public String resolveToken() {
    long now = Instant.now().getEpochSecond();
    String current = tokenValue == null ? "" : tokenValue;
    if (!current.isBlank() && now + refreshSkewSeconds < expiresAtEpochSecond) {
      return current;
    }
    synchronized (this) {
      now = Instant.now().getEpochSecond();
      current = tokenValue == null ? "" : tokenValue;
      if (!current.isBlank() && now + refreshSkewSeconds < expiresAtEpochSecond) {
        return current;
      }
      try {
        refreshToken(now);
      } catch (Exception e) {
        if (debug) {
          System.out.println("! [debug] oidc token refresh failed: " + e);
        }
        return "";
      }
    }
    return tokenValue;
  }

  private void refreshToken(long now) throws Exception {
    String body =
        "grant_type=client_credentials&client_id="
            + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
            + "&client_secret="
            + URLEncoder.encode(clientSecret, StandardCharsets.UTF_8);
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(tokenUrl))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() / 100 != 2) {
      throw new IllegalStateException(
          "OIDC token endpoint returned status " + response.statusCode());
    }
    String respBody = response.body();
    Matcher ma = ACCESS_TOKEN_PATTERN.matcher(respBody);
    Matcher me = EXPIRES_IN_PATTERN.matcher(respBody);
    if (!ma.find() || !me.find()) {
      throw new IllegalStateException("Invalid oidc token response");
    }
    tokenValue = ma.group(1);
    expiresAtEpochSecond = now + Long.parseLong(me.group(1));
  }
}
