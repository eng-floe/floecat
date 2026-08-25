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

package ai.floedb.floecat.catalog.iceberg.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.catalog.access.CatalogAuthentication;
import ai.floedb.floecat.catalog.access.CatalogAuthenticationScheme;
import ai.floedb.floecat.catalog.access.CatalogClientFactory;
import ai.floedb.floecat.catalog.access.CatalogConnectionConfig;
import ai.floedb.floecat.catalog.access.CatalogProtocol;
import ai.floedb.floecat.catalog.access.ResolvedCatalogCredentials;
import ai.floedb.floecat.catalog.iceberg.rest.auth.AwsCredentialScope;
import ai.floedb.floecat.catalog.iceberg.rest.auth.AwsCredentialValue;
import ai.floedb.floecat.catalog.iceberg.rest.auth.CatalogSigV4AuthManager;
import ai.floedb.floecat.catalog.iceberg.rest.auth.RefreshingAwsCredentialsRegistry;
import ai.floedb.floecat.catalog.iceberg.rest.auth.RegistryBackedAwsCredentialsProvider;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.rest.RESTUtil;
import org.junit.jupiter.api.Test;

class IcebergRestCatalogClientProviderTest {
  @Test
  void buildsAnonymousPropertiesWithoutCredentials() {
    Map<String, String> properties =
        IcebergRestCatalogClientProvider.catalogProperties(
            config(CatalogAuthentication.none(), Map.of("warehouse", "sales")),
            ResolvedCatalogCredentials.none());

    assertEquals("https://catalog.example/v1", properties.get("uri"));
    assertEquals("sales", properties.get("warehouse"));
    assertEquals("vended-credentials", properties.get("header.X-Iceberg-Access-Delegation"));
    assertEquals(
        "vended-credentials",
        RESTUtil.configHeaders(properties).get("X-Iceberg-Access-Delegation"));
    assertFalse(properties.containsKey("token"));
  }

  @Test
  void injectsOauthSecretsOnlyAtOpenBoundary() {
    Map<String, String> properties =
        IcebergRestCatalogClientProvider.catalogProperties(
            config(
                new CatalogAuthentication(
                    CatalogAuthenticationScheme.OAUTH2,
                    Map.of("oauth2-server-uri", "https://identity.example/token")),
                Map.of()),
            new ResolvedCatalogCredentials(
                Map.of("token", "secret-token"), Map.of("X-Tenant", "tenant-a"), null));

    assertEquals("oauth2", properties.get("rest.auth.type"));
    assertEquals("secret-token", properties.get("token"));
    assertEquals("tenant-a", properties.get("header.X-Tenant"));
  }

  @Test
  void rejectsCallerControlledAccessDelegationHeader() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergRestCatalogClientProvider.catalogProperties(
                    config(
                        new CatalogAuthentication(CatalogAuthenticationScheme.OAUTH2, Map.of()),
                        Map.of()),
                    new ResolvedCatalogCredentials(
                        Map.of("token", "secret-token"),
                        Map.of("X-Iceberg-Access-Delegation", "remote-signing"),
                        null)));

    assertEquals(
        "X-Iceberg-Access-Delegation is controlled by the Iceberg REST provider",
        error.getMessage());
  }

  @Test
  void rejectsDifferentlyCasedCallerControlledAccessDelegationHeader() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergRestCatalogClientProvider.catalogProperties(
                    config(
                        new CatalogAuthentication(CatalogAuthenticationScheme.OAUTH2, Map.of()),
                        Map.of()),
                    new ResolvedCatalogCredentials(
                        Map.of("token", "secret-token"),
                        Map.of("x-iceberg-access-delegation", "remote-signing"),
                        null)));

    assertEquals(
        "X-Iceberg-Access-Delegation is controlled by the Iceberg REST provider",
        error.getMessage());
  }

  @Test
  void rejectsSecretsInPersistableConnectionConfig() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergRestCatalogClientProvider.catalogProperties(
                    config(CatalogAuthentication.none(), Map.of("token", "must-not-persist")),
                    ResolvedCatalogCredentials.none()));

    assertEquals("connection properties must not contain secret key: token", error.getMessage());
  }

  @Test
  void rejectsHeadersInPersistableConnectionConfig() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergRestCatalogClientProvider.catalogProperties(
                    config(
                        CatalogAuthentication.none(),
                        Map.of("header.Authorization", "Bearer must-not-persist")),
                    ResolvedCatalogCredentials.none()));

    assertEquals(
        "connection properties must not contain secret key: header.Authorization",
        error.getMessage());
  }

  @Test
  void rejectsResolvedHeadersForAnonymousAuthentication() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergRestCatalogClientProvider.catalogProperties(
                    config(CatalogAuthentication.none(), Map.of()),
                    new ResolvedCatalogCredentials(
                        Map.of(), Map.of("Authorization", "secret"), null)));

    assertEquals(
        "Authentication scheme none does not accept resolved credentials", error.getMessage());
  }

  @Test
  void rejectsNonHttpRestEndpoints() {
    CatalogConnectionConfig config =
        new CatalogConnectionConfig(
            CatalogProtocol.ICEBERG_REST,
            URI.create("s3://bucket/catalog"),
            Map.of(),
            CatalogAuthentication.none());

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergRestCatalogClientProvider.catalogProperties(
                    config, ResolvedCatalogCredentials.none()));

    assertEquals("Iceberg REST endpoint must use http or https: scheme=s3", error.getMessage());
  }

  @Test
  void providerIsDiscoverableWithoutConnectorServiceLoading() {
    CatalogConnectionConfig config =
        new CatalogConnectionConfig(
            CatalogProtocol.ICEBERG_REST,
            URI.create("s3://bucket/catalog"),
            Map.of(),
            CatalogAuthentication.none());

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> CatalogClientFactory.load().open(config, ResolvedCatalogCredentials.none()));

    assertEquals("Iceberg REST endpoint must use http or https: scheme=s3", error.getMessage());
  }

  @Test
  void buildsStaticSigV4CatalogAndStorageCredentialsInSeparateLanes() {
    Map<String, String> properties =
        IcebergRestCatalogClientProvider.catalogProperties(
            config(
                new CatalogAuthentication(
                    CatalogAuthenticationScheme.AWS_SIGV4,
                    Map.of("signing-name", "glue", "signing-region", "us-west-2")),
                Map.of("warehouse", "warehouse-a", "s3.region", "us-west-2")),
            new ResolvedCatalogCredentials(
                Map.of(
                    "rest.access-key-id", "catalog-access",
                    "rest.secret-access-key", "catalog-secret",
                    "rest.session-token", "catalog-token",
                    "s3.access-key-id", "storage-access",
                    "s3.secret-access-key", "storage-secret",
                    "s3.session-token", "storage-token"),
                Map.of(),
                null));

    assertEquals(CatalogSigV4AuthManager.class.getName(), properties.get("rest.auth.type"));
    assertEquals("glue", properties.get("rest.signing-name"));
    assertEquals("us-west-2", properties.get("rest.signing-region"));
    assertEquals("catalog-access", properties.get("rest.access-key-id"));
    assertEquals("catalog-secret", properties.get("rest.secret-access-key"));
    assertEquals("storage-access", properties.get("s3.access-key-id"));
    assertEquals("storage-secret", properties.get("s3.secret-access-key"));
    assertEquals("org.apache.iceberg.aws.s3.S3FileIO", properties.get("io-impl"));
    assertEquals("us-west-2", properties.get("client.region"));
  }

  @Test
  void leavesSigV4ServiceAndRegionUnsetForIcebergDefaults() {
    Map<String, String> properties =
        IcebergRestCatalogClientProvider.catalogProperties(
            config(
                new CatalogAuthentication(CatalogAuthenticationScheme.AWS_SIGV4, Map.of()),
                Map.of()),
            new ResolvedCatalogCredentials(
                Map.of(
                    "rest.access-key-id", "catalog-access",
                    "rest.secret-access-key", "catalog-secret"),
                Map.of(),
                null));

    assertFalse(properties.containsKey("rest.signing-name"));
    assertFalse(properties.containsKey("rest.signing-region"));
  }

  @Test
  void configuresDifferentRenewableProvidersForCatalogAndStorage() {
    try (var catalogRegistration =
            RefreshingAwsCredentialsRegistry.register(
                credentials("catalog"), () -> credentials("catalog-refreshed"));
        var storageRegistration =
            RefreshingAwsCredentialsRegistry.register(
                credentials("storage"), () -> credentials("storage-refreshed"))) {
      Map<String, String> resolvedProperties = new java.util.HashMap<>();
      resolvedProperties.putAll(
          RefreshingAwsCredentialsRegistry.propertiesFor(
              catalogRegistration, AwsCredentialScope.CATALOG));
      resolvedProperties.putAll(
          RefreshingAwsCredentialsRegistry.propertiesFor(
              storageRegistration, AwsCredentialScope.STORAGE));

      Map<String, String> properties =
          IcebergRestCatalogClientProvider.catalogProperties(
              config(
                  new CatalogAuthentication(CatalogAuthenticationScheme.AWS_SIGV4, Map.of()),
                  Map.of()),
              new ResolvedCatalogCredentials(resolvedProperties, Map.of(), null));

      assertEquals(
          resolvedProperties.get(RefreshingAwsCredentialsRegistry.CATALOG_PROVIDER_ID),
          properties.get(RefreshingAwsCredentialsRegistry.CATALOG_PROVIDER_ID));
      assertEquals(
          RegistryBackedAwsCredentialsProvider.class.getName(),
          properties.get("client.credentials-provider"));
      assertEquals(
          resolvedProperties.get(RefreshingAwsCredentialsRegistry.STORAGE_PROVIDER_ID),
          properties.get("client.credentials-provider.floecat-provider-id"));
      assertEquals(
          AwsCredentialScope.STORAGE.name(),
          properties.get("client.credentials-provider.floecat-credential-scope"));
    }
  }

  @Test
  void rejectsSigV4WithoutCatalogCredentials() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergRestCatalogClientProvider.catalogProperties(
                    config(
                        new CatalogAuthentication(CatalogAuthenticationScheme.AWS_SIGV4, Map.of()),
                        Map.of()),
                    ResolvedCatalogCredentials.none()));

    assertEquals(
        "AWS SigV4 requires catalog access/secret keys or a renewable catalog provider",
        error.getMessage());
  }

  @Test
  void rejectsIncompleteStaticStorageCredentials() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergRestCatalogClientProvider.catalogProperties(
                    config(
                        new CatalogAuthentication(CatalogAuthenticationScheme.AWS_SIGV4, Map.of()),
                        Map.of()),
                    new ResolvedCatalogCredentials(
                        Map.of(
                            "rest.access-key-id", "catalog-access",
                            "rest.secret-access-key", "catalog-secret",
                            "s3.access-key-id", "storage-access"),
                        Map.of(),
                        null)));

    assertEquals(
        "AWS storage credentials require both s3.access-key-id and s3.secret-access-key",
        error.getMessage());
  }

  @Test
  void rejectsStorageSessionTokenWithoutStaticKeys() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IcebergRestCatalogClientProvider.catalogProperties(
                    config(
                        new CatalogAuthentication(CatalogAuthenticationScheme.AWS_SIGV4, Map.of()),
                        Map.of()),
                    new ResolvedCatalogCredentials(
                        Map.of(
                            "rest.access-key-id", "catalog-access",
                            "rest.secret-access-key", "catalog-secret",
                            "s3.session-token", "orphan-storage-token"),
                        Map.of(),
                        null)));

    assertEquals(
        "AWS storage credentials require both s3.access-key-id and s3.secret-access-key",
        error.getMessage());
  }

  private static AwsCredentialValue credentials(String suffix) {
    return new AwsCredentialValue("access-" + suffix, "secret-" + suffix, "token-" + suffix, null);
  }

  private static CatalogConnectionConfig config(
      CatalogAuthentication authentication, Map<String, String> properties) {
    return new CatalogConnectionConfig(
        CatalogProtocol.ICEBERG_REST,
        URI.create("https://catalog.example/v1"),
        properties,
        authentication);
  }
}
