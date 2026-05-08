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

package ai.floedb.floecat.reconciler.auth;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class OidcReconcileWorkerAuthProvider implements ReconcileWorkerAuthProvider {
  private static final Logger LOG = Logger.getLogger(OidcReconcileWorkerAuthProvider.class);

  private final Optional<String> issuer;
  private final Optional<String> clientId;
  private final Optional<String> clientSecret;
  private final long refreshSkewSeconds;
  private final Duration connectTimeout;
  private final Clock clock;
  private final TokenEndpointClient tokenEndpointClient;

  private volatile CachedToken cachedToken;

  @Inject
  public OidcReconcileWorkerAuthProvider(
      @ConfigProperty(name = "floecat.reconciler.oidc.issuer") Optional<String> issuer,
      @ConfigProperty(name = "floecat.reconciler.oidc.client-id") Optional<String> clientId,
      @ConfigProperty(name = "floecat.reconciler.oidc.client-secret") Optional<String> clientSecret,
      @ConfigProperty(
              name = "floecat.reconciler.oidc.token-refresh-skew-seconds",
              defaultValue = "30")
          long refreshSkewSeconds,
      @ConfigProperty(name = "floecat.reconciler.oidc.connect-timeout", defaultValue = "10s")
          Duration connectTimeout) {
    this(
        issuer,
        clientId,
        clientSecret,
        refreshSkewSeconds,
        connectTimeout,
        Clock.systemUTC(),
        new HttpTokenEndpointClient());
  }

  public OidcReconcileWorkerAuthProvider(
      Optional<String> issuer,
      Optional<String> clientId,
      Optional<String> clientSecret,
      long refreshSkewSeconds,
      Duration connectTimeout,
      Clock clock,
      TokenEndpointClient tokenEndpointClient) {
    this.issuer = normalize(issuer);
    this.clientId = normalize(clientId);
    this.clientSecret = normalize(clientSecret);
    this.refreshSkewSeconds = Math.max(0L, refreshSkewSeconds);
    this.connectTimeout = connectTimeout == null ? Duration.ofSeconds(10) : connectTimeout;
    this.clock = clock == null ? Clock.systemUTC() : clock;
    this.tokenEndpointClient =
        tokenEndpointClient == null ? new HttpTokenEndpointClient() : tokenEndpointClient;
  }

  @Override
  public Optional<String> authorizationHeader() {
    if (issuer.isEmpty() || clientId.isEmpty() || clientSecret.isEmpty()) {
      return Optional.empty();
    }
    CachedToken current = cachedToken;
    Instant now = clock.instant();
    if (current != null && now.isBefore(current.refreshAt())) {
      return Optional.of(current.authorizationHeader());
    }
    synchronized (this) {
      current = cachedToken;
      now = clock.instant();
      if (current != null && now.isBefore(current.refreshAt())) {
        return Optional.of(current.authorizationHeader());
      }
      CachedToken refreshed = fetchToken(now);
      cachedToken = refreshed;
      return Optional.of(refreshed.authorizationHeader());
    }
  }

  private CachedToken fetchToken(Instant now) {
    URI endpoint = tokenEndpoint();
    String requestBody =
        "client_id="
            + urlEncode(clientId.orElseThrow())
            + "&client_secret="
            + urlEncode(clientSecret.orElseThrow())
            + "&grant_type=client_credentials";
    TokenResponse response = tokenEndpointClient.fetch(endpoint, requestBody, connectTimeout);
    Instant expiresAt = now.plusSeconds(Math.max(1L, response.expiresInSeconds()));
    Instant refreshAt = expiresAt.minusSeconds(refreshSkewSeconds);
    if (refreshAt.isBefore(now)) {
      refreshAt = now;
    }
    LOG.debugf(
        "Acquired reconcile worker OIDC token; refreshAt=%s expiresAt=%s", refreshAt, expiresAt);
    return new CachedToken(withBearerPrefix(response.accessToken()), refreshAt);
  }

  private URI tokenEndpoint() {
    String base = issuer.orElseThrow();
    String endpoint =
        base.endsWith("/")
            ? base + "protocol/openid-connect/token"
            : base + "/protocol/openid-connect/token";
    return URI.create(endpoint);
  }

  private static Optional<String> normalize(Optional<String> value) {
    return value.map(String::trim).filter(v -> !v.isBlank());
  }

  private static String withBearerPrefix(String token) {
    if (token.regionMatches(true, 0, "bearer ", 0, 7)) {
      return token;
    }
    return "Bearer " + token;
  }

  private static String urlEncode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  static TokenResponse parseTokenResponse(String responseBody) {
    String accessToken = extractStringField(responseBody, "access_token");
    long expiresIn = extractLongField(responseBody, "expires_in");
    return new TokenResponse(accessToken, expiresIn);
  }

  private static String extractStringField(String json, String fieldName) {
    Matcher matcher =
        Pattern.compile("\"" + Pattern.quote(fieldName) + "\"\\s*:\\s*\"([^\"]+)\"")
            .matcher(json == null ? "" : json);
    if (!matcher.find()) {
      throw new IllegalStateException("Missing string field " + fieldName + " in token response");
    }
    return matcher.group(1);
  }

  private static long extractLongField(String json, String fieldName) {
    Matcher matcher =
        Pattern.compile("\"" + Pattern.quote(fieldName) + "\"\\s*:\\s*(\\d+)")
            .matcher(json == null ? "" : json);
    if (!matcher.find()) {
      throw new IllegalStateException("Missing numeric field " + fieldName + " in token response");
    }
    return Long.parseLong(matcher.group(1));
  }

  record CachedToken(String authorizationHeader, Instant refreshAt) {}

  public record TokenResponse(String accessToken, long expiresInSeconds) {}

  @FunctionalInterface
  public interface TokenEndpointClient {
    TokenResponse fetch(URI endpoint, String requestBody, Duration connectTimeout);
  }

  static final class HttpTokenEndpointClient implements TokenEndpointClient {
    @Override
    public TokenResponse fetch(URI endpoint, String requestBody, Duration connectTimeout) {
      try {
        HttpRequest request =
            HttpRequest.newBuilder()
                .uri(endpoint)
                .timeout(connectTimeout)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
        HttpResponse<String> response =
            HttpClient.newBuilder()
                .connectTimeout(connectTimeout)
                .build()
                .send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
          throw new IllegalStateException(
              "OIDC token request failed: " + response.statusCode() + " " + response.body());
        }
        return parseTokenResponse(response.body());
      } catch (Exception e) {
        LOG.warn("Reconcile worker OIDC token request failed", e);
        throw new IllegalStateException("Failed to acquire reconcile worker OIDC token", e);
      }
    }
  }
}
