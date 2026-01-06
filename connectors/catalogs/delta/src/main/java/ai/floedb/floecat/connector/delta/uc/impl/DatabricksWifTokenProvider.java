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
import java.io.IOException;
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

final class DatabricksWifTokenProvider implements AccessTokenProvider {
  private static final ObjectMapper M = new ObjectMapper();
  private static final long SKEW_SECONDS = 60;
  private static final String DEFAULT_SUBJECT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt";

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(Duration.ofMillis(15000)).build();
  private final String host;
  private final String clientId;
  private final String scope;
  private final String subjectToken;
  private final String subjectTokenFile;
  private final String subjectTokenType;
  private final String requestedTokenType;
  private final String audience;
  private final int timeoutMs = 15000;
  private volatile String cachedAccess;
  private volatile Instant cachedExpiry;

  DatabricksWifTokenProvider(
      String host,
      String clientId,
      String scope,
      String subjectToken,
      String subjectTokenFile,
      String subjectTokenType,
      String requestedTokenType,
      String audience) {
    this.host = (host == null ? "" : host.trim()).replaceAll("/+$", "");
    this.clientId = clientId;
    this.scope = (scope == null || scope.isBlank()) ? "all-apis" : scope;
    this.subjectToken = subjectToken;
    this.subjectTokenFile = subjectTokenFile;
    this.subjectTokenType =
        (subjectTokenType == null || subjectTokenType.isBlank())
            ? DEFAULT_SUBJECT_TOKEN_TYPE
            : subjectTokenType;
    this.requestedTokenType = requestedTokenType;
    this.audience = audience;
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
      throw new RuntimeException("Failed to obtain Databricks WIF access token", e);
    }
  }

  private static boolean expiringSoon(Instant when) {
    return when == null || Instant.now().isAfter(when.minusSeconds(SKEW_SECONDS));
  }

  private Token fetchNewAccessToken() throws Exception {
    String subject = resolveSubjectToken();
    if (subject == null || subject.isBlank()) {
      throw new IllegalStateException(
          "No subject token for WIF. Provide oauth.subject_token, "
              + "oauth.subject_token_file, DATABRICKS_WIF_SUBJECT_TOKEN, or AWS_WEB_IDENTITY_TOKEN_FILE.");
    }

    String form =
        "grant_type="
            + enc("urn:ietf:params:oauth:grant-type:token-exchange")
            + "&client_id="
            + enc(clientId)
            + "&subject_token="
            + enc(subject)
            + "&subject_token_type="
            + enc(subjectTokenType);

    if (scope != null && !scope.isBlank()) {
      form += "&scope=" + enc(scope);
    }
    if (requestedTokenType != null && !requestedTokenType.isBlank()) {
      form += "&requested_token_type=" + enc(requestedTokenType);
    }
    if (audience != null && !audience.isBlank()) {
      form += "&audience=" + enc(audience);
    }

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
          "Databricks WIF token request failed: HTTP " + resp.statusCode() + " " + resp.body());
    }
    JsonNode j = M.readTree(resp.body());
    String access = j.path("access_token").asText(null);
    long expiresIn = j.path("expires_in").asLong(3600);
    if (access == null || access.isBlank()) {
      throw new IllegalStateException("No access_token in Databricks WIF token response");
    }
    return new Token(access, Instant.now().plusSeconds(Math.max(1, expiresIn)));
  }

  private String resolveSubjectToken() {
    if (subjectToken != null && !subjectToken.isBlank()) {
      return subjectToken;
    }
    String env = System.getenv("DATABRICKS_WIF_SUBJECT_TOKEN");
    if (env != null && !env.isBlank()) {
      return env.trim();
    }
    String awsWebIdentity = System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE");
    if (awsWebIdentity != null && !awsWebIdentity.isBlank()) {
      String token = readFileQuietly(Path.of(awsWebIdentity));
      if (token != null && !token.isBlank()) {
        return token.trim();
      }
    }
    if (subjectTokenFile != null && !subjectTokenFile.isBlank()) {
      String token = readFileQuietly(Path.of(subjectTokenFile));
      if (token != null && !token.isBlank()) {
        return token.trim();
      }
    }
    return null;
  }

  private static String readFileQuietly(Path path) {
    try {
      return Files.readString(path);
    } catch (IOException ignore) {
      return null;
    }
  }

  private record Token(String value, Instant expiresAt) {}

  private static String enc(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }
}
