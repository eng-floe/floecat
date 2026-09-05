/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.catalog.unity;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.client.unity.HttpUnityCatalogClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;

/** Refreshes a Databricks OAuth client-credentials token independently of storage credentials. */
final class UnityOAuthTokenProvider implements AutoCloseable {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
  private static final Duration DEFAULT_TOKEN_LIFETIME = Duration.ofMinutes(5);

  /**
   * How much of a token response is worth holding.
   *
   * <p>A token response is a small JSON object. The endpoint it comes from is named by the
   * integration, so a tenant can point it at a server they control, and {@code
   * BodyHandlers.ofString} would buffer whatever that server sends into the shared service's heap.
   * The request timeout is no defence -- the body arrives inside it. Generous next to any real
   * response and far below anything that threatens the heap. {@code HttpUnityCatalogClient} bounds
   * catalog responses for the same reason.
   */
  private static final int MAX_TOKEN_RESPONSE_BYTES = 1024 * 1024;

  /** Status and body of one token request, bounded by the sender that produced it. */
  record TokenResponse(int statusCode, String body) {}

  /**
   * Sends one token request.
   *
   * <p>Implementations must bound what they read. The abstraction hands back a body rather than an
   * {@code HttpResponse} so that reading it -- and deciding how much of it to keep -- happens
   * inside the sender rather than at an arbitrary later point.
   */
  @FunctionalInterface
  interface Sender {
    TokenResponse send(HttpRequest request) throws Exception;
  }

  private final URI tokenUri;
  private final String basicAuthorization;
  private final String scope;
  private final Sender sender;
  private final AutoCloseable senderOwner;
  private final Clock clock;
  private Token current;

  UnityOAuthTokenProvider(URI tokenUri, String credential, String scope) {
    // Validation ahead of newHttpClient in the argument list, deliberately. Java evaluates these
    // left to right, so a client built first would be orphaned when the checks below reject the
    // input -- nothing holds it, and UnityCatalogClientProvider.open assigns authenticationOwner
    // only once this constructor returns, so its closeQuietly has nothing to close. Every retried
    // open of a misconfigured integration leaked another client and its executor.
    // HttpUnityCatalogClient orders its own delegation the same way for the same reason.
    this(
        requireTokenUri(tokenUri),
        requireCredential(credential),
        scope,
        newHttpClient(),
        Clock.systemUTC());
  }

  private UnityOAuthTokenProvider(
      URI tokenUri, String credential, String scope, HttpClient httpClient, Clock clock) {
    this(
        tokenUri,
        credential,
        scope,
        request -> boundedSend(httpClient, request),
        httpClient,
        clock);
  }

  UnityOAuthTokenProvider(
      URI tokenUri,
      String credential,
      String scope,
      Sender sender,
      AutoCloseable senderOwner,
      Clock clock) {
    this.tokenUri = requireTokenUri(tokenUri);
    this.basicAuthorization = basicAuthorization(credential);
    this.scope = scope == null ? "" : scope.trim();
    this.sender = Objects.requireNonNull(sender, "sender");
    this.senderOwner = senderOwner;
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  synchronized String accessToken() {
    Instant now = clock.instant();
    if (current == null || !now.isBefore(current.refreshAt())) {
      current = requestToken(now);
    }
    return current.value();
  }

  private Token requestToken(Instant now) {
    String body = "grant_type=client_credentials";
    if (!scope.isBlank()) {
      body += "&scope=" + URLEncoder.encode(scope, StandardCharsets.UTF_8);
    }
    HttpRequest request =
        HttpRequest.newBuilder(tokenUri)
            .timeout(REQUEST_TIMEOUT)
            .header("Accept", "application/json")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Authorization", basicAuthorization)
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    try {
      TokenResponse response = sender.send(request);
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new CatalogAccessException(
            tokenFailureCode(response.statusCode()),
            "Unity Catalog OAuth token request failed with HTTP " + response.statusCode());
      }
      JsonNode payload = JSON.readTree(response.body());
      String token = text(payload, "access_token");
      if (token == null) {
        throw new CatalogAccessException(
            CatalogAccessException.Code.INTERNAL,
            "Unity Catalog OAuth token response omitted access_token");
      }
      long lifetimeSeconds = positiveLong(payload.path("expires_in"), DEFAULT_TOKEN_LIFETIME);
      Duration lifetime = Duration.ofSeconds(lifetimeSeconds);
      Duration skew =
          lifetime.compareTo(Duration.ofMinutes(10)) < 0
              ? lifetime.dividedBy(10)
              : Duration.ofMinutes(1);
      return new Token(token, now.plus(lifetime).minus(skew));
    } catch (CatalogAccessException failure) {
      throw failure;
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new CatalogAccessException(
          CatalogAccessException.Code.TIMEOUT, "Unity Catalog OAuth token request interrupted");
    } catch (Exception failure) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNAVAILABLE,
          "Unity Catalog OAuth token request failed",
          failure);
    }
  }

  /**
   * Sends the request and keeps at most {@link #MAX_TOKEN_RESPONSE_BYTES} of the response.
   *
   * <p>A non-2xx body is never read at all: the status has already decided the outcome, and the
   * body is discarded either way, so buffering it would spend heap on a response nothing consults.
   * On a 2xx, one byte past the limit is enough to detect the overrun without holding the rest, and
   * the stream is closed either way.
   */
  private static TokenResponse boundedSend(HttpClient httpClient, HttpRequest request)
      throws Exception {
    HttpResponse<InputStream> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
    InputStream stream = response.body();
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      closeQuietly(stream);
      return new TokenResponse(response.statusCode(), null);
    }
    // The request timeout covered the headers only: ofInputStream returns as soon as they arrive,
    // so the read below runs outside it. A token endpoint that sends a status line and then stops
    // emitting would block this thread forever, and the overlay reconcile path has no budget to
    // release it.
    //
    // On a virtual thread rather than the common pool. This runs while accessToken() holds its
    // monitor, so a starved ForkJoinPool.commonPool -- parallelism one on a small container --
    // serialized token refresh across every caller of this provider and then failed them on a
    // deadline the server had already met.
    byte[] bytes;
    try {
      bytes = readWithin(stream, MAX_TOKEN_RESPONSE_BYTES, REQUEST_TIMEOUT);
    } finally {
      closeQuietly(stream);
    }
    if (bytes.length > MAX_TOKEN_RESPONSE_BYTES) {
      // Classified without retaining the body: an endpoint returning a token response this large is
      // not returning a token response.
      throw new CatalogAccessException(
          CatalogAccessException.Code.INTERNAL,
          "Unity Catalog OAuth token response exceeded " + MAX_TOKEN_RESPONSE_BYTES + " bytes");
    }
    return new TokenResponse(response.statusCode(), new String(bytes, StandardCharsets.UTF_8));
  }

  /**
   * What a token endpoint's status says about whether another attempt could work.
   *
   * <p>Only 408 and 429 are worth retrying among the 4xx: the rest describe the request this
   * provider built from stored configuration -- a scope the server rejects, a token route that is
   * not there -- and will answer the same way every time. Reporting those as an unavailable
   * endpoint sent an operator looking at the network and spent a reconcile job's retry budget on a
   * configuration error.
   */
  private static CatalogAccessException.Code tokenFailureCode(int statusCode) {
    if (statusCode == 401) {
      return CatalogAccessException.Code.UNAUTHENTICATED;
    }
    if (statusCode == 403) {
      return CatalogAccessException.Code.PERMISSION_DENIED;
    }
    if (statusCode == 408 || statusCode == 429) {
      return CatalogAccessException.Code.UNAVAILABLE;
    }
    if (statusCode >= 400 && statusCode < 500) {
      return CatalogAccessException.Code.INVALID_CONFIGURATION;
    }
    return CatalogAccessException.Code.UNAVAILABLE;
  }

  /**
   * Reads {@code stream} up to {@code limit + 1} bytes, giving up after {@code deadline}.
   *
   * <p>Its own virtual thread, for the reason the caller's comment gives: the read blocks for the
   * whole body, and the common pool has parallelism one on a small container. Closing the stream is
   * what releases a genuinely stalled read.
   */
  private static byte[] readWithin(InputStream stream, int limit, java.time.Duration deadline)
      throws IOException {
    var body = new java.util.concurrent.CompletableFuture<byte[]>();
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                body.complete(stream.readNBytes(limit + 1));
              } catch (Throwable failure) {
                body.completeExceptionally(failure);
              }
            });
    try {
      return body.get(deadline.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.TimeoutException stalled) {
      closeQuietly(stream);
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNAVAILABLE,
          "Unity Catalog OAuth token response stalled after " + deadline,
          stalled);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      closeQuietly(stream);
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNAVAILABLE,
          "Interrupted reading the Unity Catalog OAuth token response",
          interrupted);
    } catch (java.util.concurrent.ExecutionException failure) {
      Throwable cause = failure.getCause();
      throw cause instanceof IOException io ? io : new IOException(cause);
    }
  }

  private static void closeQuietly(InputStream stream) {
    try {
      stream.close();
    } catch (IOException ignored) {
      // Closing is how a stalled read is released; a failure to close adds nothing to report.
    }
  }

  private static HttpClient newHttpClient() {
    return HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  }

  private static URI requireTokenUri(URI tokenUri) {
    Objects.requireNonNull(tokenUri, "tokenUri");
    // HTTPS, or cleartext to loopback on the same explicit opt-in the catalog endpoint uses. The
    // two are not independent: with no oauth2-server-uri the token endpoint is derived from the
    // catalog URI, so an HTTP loopback catalog produces an HTTP loopback token endpoint. Requiring
    // HTTPS here with no escape hatch made client-credentials auth unusable in local development
    // while the catalog request beside it went through.
    boolean cleartextLoopback =
        "http".equalsIgnoreCase(tokenUri.getScheme())
            && HttpUnityCatalogClient.isCleartextLoopbackAllowed(tokenUri);
    if (!tokenUri.isAbsolute()
        || tokenUri.getHost() == null
        || !("https".equalsIgnoreCase(tokenUri.getScheme()) || cleartextLoopback)
        || tokenUri.getRawUserInfo() != null
        || tokenUri.getRawFragment() != null) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog OAuth token URI must be HTTPS, or HTTP to a loopback host when cleartext"
              + " loopback is enabled");
    }
    // Scheme and shape are not the whole policy. This URI receives an Authorization: Basic header
    // carrying the integration's OAuth client id and secret, so a tenant that can set it can name
    // any reachable host and be sent those credentials -- https://169.254.169.254/token is a
    // well-formed HTTPS URI addressing a cloud metadata service. The catalog endpoint in this same
    // integration is already held to an address-class policy; the token endpoint was not.
    try {
      HttpUnityCatalogClient.assertEndpointAddressAllowed(tokenUri);
    } catch (IllegalArgumentException refused) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog OAuth token URI names an address class that is not allowed",
          refused);
    }
    return tokenUri;
  }

  /**
   * Rejects a credential that cannot form a Basic header, before anything is allocated.
   *
   * <p>Split out of {@link #basicAuthorization} so the delegating constructor can reject it in its
   * argument list; the value itself is still assembled there.
   */
  private static String requireCredential(String credential) {
    basicAuthorization(credential);
    return credential;
  }

  private static String basicAuthorization(String credential) {
    String value = credential == null ? "" : credential;
    int separator = value.indexOf(':');
    if (separator <= 0 || separator == value.length() - 1) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog OAuth client credentials are incomplete");
    }
    return "Basic " + Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String text(JsonNode node, String name) {
    JsonNode value = node.path(name);
    if (!value.isTextual() || value.textValue().isBlank()) {
      return null;
    }
    return value.textValue();
  }

  /**
   * A textual integer counts too. {@code canConvertToLong} is false for a string, so an issuer that
   * answers {@code "expires_in": "3600"} -- legal JSON, and what several OAuth2 servers emit --
   * fell through to the default lifetime. Nothing broke: the provider just refreshed on the
   * default's schedule instead of the issuer's, putting a synchronized token exchange in the middle
   * of long reconcile walks with nothing above debug to explain it.
   */
  private static long positiveLong(JsonNode value, Duration defaultValue) {
    if (value.canConvertToLong() && value.longValue() > 0) {
      return value.longValue();
    }
    if (value.isTextual()) {
      try {
        long parsed = Long.parseLong(value.textValue().trim());
        if (parsed > 0) {
          return parsed;
        }
      } catch (NumberFormatException notANumber) {
        // Falls through to the default, like any other unusable value.
      }
    }
    return defaultValue.toSeconds();
  }

  @Override
  public void close() {
    if (senderOwner == null) {
      return;
    }
    try {
      senderOwner.close();
    } catch (Exception ignored) {
    }
  }

  private record Token(String value, Instant refreshAt) {}
}
