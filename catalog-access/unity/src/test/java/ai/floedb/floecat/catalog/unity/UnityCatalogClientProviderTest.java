/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.catalog.unity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogAuthentication;
import ai.floedb.floecat.catalog.access.CatalogAuthenticationScheme;
import ai.floedb.floecat.catalog.access.CatalogClientProvider;
import ai.floedb.floecat.catalog.access.CatalogConnectionConfig;
import ai.floedb.floecat.catalog.access.CatalogProtocol;
import ai.floedb.floecat.catalog.access.ResolvedCatalogCredentials;
import ai.floedb.floecat.client.unity.UnityCatalogAuthentication;
import ai.floedb.floecat.client.unity.UnityCatalogClient;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class UnityCatalogClientProviderTest {
  @Test
  void isRegisteredAsTheUnityCatalogProvider() {
    assertThat(ServiceLoader.load(CatalogClientProvider.class))
        .anyMatch(provider -> provider.getClass() == UnityCatalogClientProvider.class);
  }

  @Test
  void opensClientWithBearerAuthenticationAndTransportOptions() {
    UnityCatalogClient unity = mock(UnityCatalogClient.class);
    AtomicReference<URI> endpoint = new AtomicReference<>();
    AtomicReference<Duration> connectTimeout = new AtomicReference<>();
    AtomicReference<Duration> readTimeout = new AtomicReference<>();
    AtomicReference<UnityCatalogAuthentication> authentication = new AtomicReference<>();
    AtomicReference<String> vendPath = new AtomicReference<>();
    UnityCatalogClientProvider provider =
        new UnityCatalogClientProvider(
            (uri, connect, read, auth, path) -> {
              endpoint.set(uri);
              connectTimeout.set(connect);
              readTimeout.set(read);
              authentication.set(auth);
              vendPath.set(path);
              return unity;
            });

    var opened =
        provider.open(
            config(
                Map.of(
                    "http.connect.ms", "1200",
                    "http.read.ms", "3400",
                    "unity.temporary-table-vend-path", "/api/custom/vend")),
            new ResolvedCatalogCredentials(
                Map.of("token", "catalog-token"), Map.of("X-Tenant", "tenant-a"), null));

    assertThat(opened).isInstanceOf(UnityCatalogAccessClient.class);
    assertThat(endpoint.get()).isEqualTo(URI.create("https://catalog.example"));
    assertThat(connectTimeout.get()).isEqualTo(Duration.ofMillis(1200));
    assertThat(readTimeout.get()).isEqualTo(Duration.ofMillis(3400));
    assertThat(vendPath.get()).isEqualTo("/api/custom/vend");
    assertThat(authentication.get().headers())
        .containsEntry("Authorization", "Bearer catalog-token")
        .containsEntry("X-Tenant", "tenant-a");
  }

  @Test
  void rejectsMissingOrAmbiguousCredentials() {
    UnityCatalogClientProvider provider =
        new UnityCatalogClientProvider(
            (uri, connect, read, auth, path) -> mock(UnityCatalogClient.class));

    assertThatThrownBy(() -> provider.open(config(Map.of()), ResolvedCatalogCredentials.none()))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure ->
                assertThat(failure.code())
                    .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
    assertThatThrownBy(
            () ->
                provider.open(
                    config(Map.of()),
                    new ResolvedCatalogCredentials(
                        Map.of("token", "token", "credential", "id:secret"), Map.of(), null)))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure ->
                assertThat(failure.code())
                    .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
  }

  @Test
  void rejectsCallerControlledAuthorizationHeader() {
    AtomicReference<UnityCatalogAuthentication> authentication = new AtomicReference<>();
    UnityCatalogClientProvider provider =
        new UnityCatalogClientProvider(
            (uri, connect, read, auth, path) -> {
              authentication.set(auth);
              return mock(UnityCatalogClient.class);
            });
    provider.open(
        config(Map.of()),
        new ResolvedCatalogCredentials(
            Map.of("token", "catalog-token"), Map.of("authorization", "Basic bad"), null));

    assertThatThrownBy(() -> authentication.get().headers())
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure ->
                assertThat(failure.code())
                    .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
  }

  /**
   * The sole guard on a tenant-supplied value that reaches {@code endpointOverride} and is
   * republished to query workers, and until now the only guard in this module with no test.
   */
  @Test
  void refusesAnS3EndpointNamingAnAddressClassTheCatalogEndpointWouldAlsoRefuse() {
    UnityCatalogClientProvider provider =
        new UnityCatalogClientProvider(
            (uri, connect, read, auth, path) -> mock(UnityCatalogClient.class));
    for (String refused :
        java.util.List.of(
            "https://169.254.169.254",
            "https://0.0.0.0:9000",
            "https://224.0.0.1",
            "https://user@storage.example",
            "https://storage.example?x=1",
            "not-a-uri",
            "/no/scheme")) {
      assertThatThrownBy(
              () -> provider.open(config(Map.of("s3.endpoint", refused)), credentials()), refused)
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code())
                      .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
    }
  }

  /**
   * Cleartext is a deployment's statement, not a default. A vend is refused without {@code
   * s3.session-token}, so every signed request carries that token in a header -- replayable by
   * anyone who sees it -- and {@code s3.endpoint} travels on to query workers.
   */
  @Test
  void refusesACleartextS3EndpointUnlessTheDeploymentAllowsOne() {
    UnityCatalogClientProvider provider =
        new UnityCatalogClientProvider(
            (uri, connect, read, auth, path) -> mock(UnityCatalogClient.class));

    assertThatThrownBy(
            () ->
                provider.open(
                    config(Map.of("s3.endpoint", "http://minio.internal:9000")), credentials()))
        .isInstanceOfSatisfying(
            CatalogAccessException.class,
            failure -> {
              assertThat(failure.code())
                  .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION);
              assertThat(failure.getMessage()).contains("HTTPS", ALLOW_CLEARTEXT_S3_ENV);
            });

    // HTTPS needs no opt-in.
    provider.open(config(Map.of("s3.endpoint", "https://storage.example")), credentials()).close();

    System.setProperty(UnityCatalogClientProvider.ALLOW_CLEARTEXT_S3_PROPERTY, "true");
    try {
      provider
          .open(config(Map.of("s3.endpoint", "http://minio.internal:9000")), credentials())
          .close();
    } finally {
      System.clearProperty(UnityCatalogClientProvider.ALLOW_CLEARTEXT_S3_PROPERTY);
    }
  }

  private static final String ALLOW_CLEARTEXT_S3_ENV =
      UnityCatalogClientProvider.ALLOW_CLEARTEXT_S3_ENV;

  /** Real credentials, because the authentication check runs ahead of the endpoint check. */
  private static ResolvedCatalogCredentials credentials() {
    return new ResolvedCatalogCredentials(Map.of("token", "catalog-token"), Map.of(), null);
  }

  private static CatalogConnectionConfig config(Map<String, String> properties) {
    return new CatalogConnectionConfig(
        CatalogProtocol.UNITY_CATALOG,
        URI.create("https://catalog.example"),
        properties,
        new CatalogAuthentication(CatalogAuthenticationScheme.OAUTH2, Map.of()));
  }
}
