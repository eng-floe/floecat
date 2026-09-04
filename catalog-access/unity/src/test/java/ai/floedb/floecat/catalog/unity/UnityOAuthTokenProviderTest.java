/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.catalog.unity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class UnityOAuthTokenProviderTest {

  /**
   * The token URI receives an Authorization: Basic header with the integration's client id and
   * secret, so a tenant able to set it must not be able to name a host that only exists inside the
   * deployment. https://169.254.169.254/token is a well-formed HTTPS URI addressing a cloud
   * metadata service, and the catalog endpoint of the same integration is already refused it.
   */
  @Test
  void refusesATokenUriNamingAnAddressClassTheCatalogEndpointWouldAlsoRefuse() {
    for (String refused :
        java.util.List.of(
            "https://169.254.169.254/token",
            "https://0.0.0.0/token",
            "https://224.0.0.1/token",
            "https://7147006462/token")) {
      assertThatThrownBy(
              () -> new UnityOAuthTokenProvider(URI.create(refused), "id:secret", null), refused)
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code())
                      .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
    }
  }

  /** A routable host is still fine: the guard is about address class, not about being strict. */
  @Test
  void acceptsARoutableHttpsTokenUri() {
    new UnityOAuthTokenProvider(
            URI.create("https://example.databricks.com/oidc/v1/token"), "id:secret", null)
        .close();
  }

  /**
   * Rejection happens before the HttpClient exists. Java evaluates the delegating constructor's
   * arguments left to right, so a client built ahead of validation is orphaned when validation
   * throws -- nothing holds it to close, and each retried open of a misconfigured integration
   * leaked another client and its executor.
   */
  @Test
  void rejectsBadInputWithoutAllocatingAClient() {
    for (String[] bad :
        new String[][] {
          {"http://example.com/token", "id:secret"},
          {"https://example.com/token", "no-colon"},
        }) {
      assertThatThrownBy(() -> new UnityOAuthTokenProvider(URI.create(bad[0]), bad[1], null))
          .isInstanceOf(CatalogAccessException.class);
    }
  }

  /**
   * A textual {@code expires_in} is legal JSON and several OAuth2 servers emit it. Reading only the
   * numeric form fell through to the default five-minute lifetime, so the provider refreshed on its
   * own schedule instead of the issuer's -- a synchronized token exchange in the middle of long
   * reconcile walks, with nothing above debug to explain it.
   *
   * <p>Observed through the refresh, since the lifetime has no accessor. An hour-long token keeps
   * its own skew of a minute, so it is still live at t+280s; the default lifetime is five minutes
   * with a thirty-second skew, so it has already been replaced by then.
   */
  @Test
  void aTextualExpiresInIsTheIssuersLifetimeNotTheDefault() {
    record Case(String name, String expiresIn, int exchangesByPlus280s) {}
    for (Case c :
        java.util.List.of(
            new Case("numeric", "3600", 1),
            new Case("textual", "\"3600\"", 1),
            new Case("textual, padded", "\" 3600 \"", 1),
            // Unusable values still fall back, so these refresh inside the window.
            new Case("textual, not a number", "\"soon\"", 2),
            new Case("textual zero", "\"0\"", 2),
            new Case("absent", null, 2))) {
      AtomicInteger requests = new AtomicInteger();
      AtomicReference<Instant> nowRef =
          new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));
      Clock moving =
          new Clock() {
            @Override
            public ZoneId getZone() {
              return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(ZoneId zone) {
              return this;
            }

            @Override
            public Instant instant() {
              return nowRef.get();
            }
          };
      String body =
          c.expiresIn() == null
              ? "{\"access_token\":\"t\"}"
              : "{\"access_token\":\"t\",\"expires_in\":" + c.expiresIn() + "}";
      UnityOAuthTokenProvider provider =
          new UnityOAuthTokenProvider(
              URI.create("https://identity.example/token"),
              "client-id:client-secret",
              "all-apis",
              request -> {
                requests.incrementAndGet();
                return response(200, body);
              },
              null,
              moving);

      assertThat(provider.accessToken()).as(c.name()).isEqualTo("t");
      nowRef.set(nowRef.get().plusSeconds(280));
      assertThat(provider.accessToken()).as(c.name()).isEqualTo("t");
      assertThat(requests).as(c.name()).hasValue(c.exchangesByPlus280s());
    }
  }

  @Test
  void requestsAndCachesAClientCredentialsToken() {
    AtomicInteger requests = new AtomicInteger();
    AtomicReference<HttpRequest> captured = new AtomicReference<>();
    UnityOAuthTokenProvider.TokenResponse response =
        response(200, "{\"access_token\":\"token-1\",\"expires_in\":3600}");
    UnityOAuthTokenProvider provider =
        new UnityOAuthTokenProvider(
            URI.create("https://identity.example/token"),
            "client-id:client-secret",
            "all-apis",
            request -> {
              requests.incrementAndGet();
              captured.set(request);
              return response;
            },
            null,
            Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC));

    assertThat(provider.accessToken()).isEqualTo("token-1");
    assertThat(provider.accessToken()).isEqualTo("token-1");
    assertThat(requests).hasValue(1);
    assertThat(captured.get().method()).isEqualTo("POST");
    String authorization = captured.get().headers().firstValue("Authorization").orElseThrow();
    assertThat(authorization).startsWith("Basic ");
    assertThat(
            new String(
                Base64.getDecoder().decode(authorization.substring("Basic ".length())),
                StandardCharsets.UTF_8))
        .isEqualTo("client-id:client-secret");
    assertThat(captured.get().headers().firstValue("Content-Type"))
        .contains("application/x-www-form-urlencoded");
  }

  @Test
  void classifiesRejectedClientCredentialsWithoutLeakingThem() {
    UnityOAuthTokenProvider provider =
        new UnityOAuthTokenProvider(
            URI.create("https://identity.example/token"),
            "client-id:client-secret",
            null,
            request -> response(401, "client-secret"),
            null,
            Clock.systemUTC());

    assertThatThrownBy(provider::accessToken)
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure -> {
              assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNAUTHENTICATED);
              assertThat(failure.getMessage()).doesNotContain("client-secret");
            });
  }

  @Test
  void rejectsNonHttpsTokenUriAndMalformedCredential() {
    assertThatThrownBy(
            () ->
                new UnityOAuthTokenProvider(
                    URI.create("http://identity.example/token"),
                    "client-id:client-secret",
                    null,
                    request -> response(200, "{}"),
                    null,
                    Clock.systemUTC()))
        .isInstanceOf(CatalogAccessException.class);
    assertThatThrownBy(
            () ->
                new UnityOAuthTokenProvider(
                    URI.create("https://identity.example/token"),
                    "client-id",
                    null,
                    request -> response(200, "{}"),
                    null,
                    Clock.systemUTC()))
        .isInstanceOf(CatalogAccessException.class);
  }

  @SuppressWarnings("unchecked")
  private static UnityOAuthTokenProvider.TokenResponse response(int status, String body) {
    return new UnityOAuthTokenProvider.TokenResponse(status, body);
  }
}
