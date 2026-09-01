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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.connector.common.auth.RefreshingAwsCredentialsProviderRegistry;
import ai.floedb.floecat.connector.common.auth.RegistryBackedAwsCredentialsProvider;
import ai.floedb.floecat.connector.spi.AuthProvider;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

class DeltaConnectorFactoryTest {

  @Test
  void selectSourceSupportsGlue() {
    var source = DeltaConnectorFactory.selectSource(Map.of("delta.source", "glue"));
    assertEquals(DeltaConnectorFactory.DeltaSource.GLUE, source);
  }

  @Test
  void filesystemSourceRequiresTableRoot() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DeltaConnectorFactory.validateOptions(
                    DeltaConnectorFactory.DeltaSource.FILESYSTEM, ""));
    assertTrue(ex.getMessage().contains("delta.table-root"));
  }

  @Test
  void tableRootRequiresFilesystemSource() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DeltaConnectorFactory.validateOptions(
                    DeltaConnectorFactory.DeltaSource.GLUE, "s3://bucket/table"));
    assertTrue(ex.getMessage().contains("delta.source=filesystem"));
  }

  @Test
  void resolveCredentialsPrefersRegisteredRefreshingProvider() throws Exception {
    var method = DeltaConnectorFactory.class.getDeclaredMethod("resolveCredentials", Map.class);
    method.setAccessible(true);

    AwsCredentialsProvider provider =
        (AwsCredentialsProvider)
            method.invoke(
                null,
                Map.of(
                    RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID,
                    "provider-1",
                    "s3.access-key-id",
                    "akid",
                    "s3.secret-access-key",
                    "secret"));

    assertInstanceOf(RegistryBackedAwsCredentialsProvider.class, provider);
  }

  @Test
  void credentialsProviderFactoryBuildsFreshProviderForEachClientRefresh() {
    var factory = DeltaConnectorFactory.credentialsProviderFactory(Map.of());

    AwsCredentialsProvider first = factory.get();
    AwsCredentialsProvider second = factory.get();

    assertNotSame(first, second);
  }

  @Test
  void credentialsProviderFactoryBuildsFreshRegistryProviderForEachClientRefresh() {
    var factory =
        DeltaConnectorFactory.credentialsProviderFactory(
            Map.of(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID, "provider-1"));

    AwsCredentialsProvider first = factory.get();
    AwsCredentialsProvider second = factory.get();

    assertNotSame(first, second);
    assertInstanceOf(RegistryBackedAwsCredentialsProvider.class, first);
    assertInstanceOf(RegistryBackedAwsCredentialsProvider.class, second);
  }

  @Test
  void glueCatalogOptionsKeepCatalogProviderSeparateFromStorageProvider() {
    Map<String, String> catalogOptions =
        DeltaConnectorFactory.buildGlueCatalogOptions(
            Map.of(
                "delta.source",
                "glue",
                RefreshingAwsCredentialsProviderRegistry.CATALOG_OPTION_PROVIDER_ID,
                "catalog-provider",
                RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID,
                "storage-provider",
                "rest.access-key-id",
                "catalog-access",
                "rest.secret-access-key",
                "catalog-secret",
                "s3.access-key-id",
                "storage-access",
                "s3.secret-access-key",
                "storage-secret"));

    assertEquals(
        "catalog-provider",
        catalogOptions.get(RefreshingAwsCredentialsProviderRegistry.CATALOG_OPTION_PROVIDER_ID));
    assertEquals("catalog-access", catalogOptions.get("rest.access-key-id"));
    assertEquals("catalog-secret", catalogOptions.get("rest.secret-access-key"));
    assertFalse(
        catalogOptions.containsKey(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID));
    assertFalse(catalogOptions.containsKey("s3.access-key-id"));
    assertFalse(catalogOptions.containsKey("s3.secret-access-key"));
  }

  @Test
  void glueCatalogOptionsUseConnectorProviderWhenStorageAuthorityWasNotApplied() {
    Map<String, String> catalogOptions =
        DeltaConnectorFactory.buildGlueCatalogOptions(
            Map.of(
                "delta.source",
                "glue",
                RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID,
                "connector-provider"));

    assertEquals(
        "connector-provider",
        catalogOptions.get(RefreshingAwsCredentialsProviderRegistry.CATALOG_OPTION_PROVIDER_ID));
    assertFalse(
        catalogOptions.containsKey(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID));
  }

  @Test
  void failedUnityClientConstructionClosesAuthProvider() {
    var auth = new ClosableAuthProvider();

    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaConnectorFactory.create("http://example.com", Map.of(), auth, Map.of()));

    assertTrue(auth.closed);
  }

  @Test
  void aMalformedUriIsRejectedWithoutQuotingIt() {
    // URI.create puts the whole input in its message, and validateConnector appends every cause
    // message to the summary it logs and returns. A malformed URI never reaches the client's
    // userinfo check, so the credential has to be withheld here.
    var auth = new ClosableAuthProvider();

    var failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DeltaConnectorFactory.create(
                    "https://user:supersecret@example.com/%", Map.of(), auth, Map.of()));

    assertFalse(failure.getMessage().contains("supersecret"), failure.getMessage());
    assertFalse(failure.getMessage().contains("user:"), failure.getMessage());
    assertNull(failure.getCause());
    assertTrue(auth.closed);
  }

  @Test
  void aBlankVendPathOptionFallsBackToTheDefault() {
    // Blank is how config UIs and persisted-property round-trips encode "not set"; it must not be
    // passed through to the client as a path.
    for (String blank : new String[] {"", "   "}) {
      var auth = new ClosableAuthProvider();
      try (var connector =
          DeltaConnectorFactory.create(
              "https://example.com",
              Map.of("unity.temporary-table-vend-path", blank),
              auth,
              Map.of())) {
        assertNotNull(connector);
      }
    }
  }

  @Test
  void anInvalidCredentialsPathOptionFailsAtConnectorCreation() {
    var auth = new ClosableAuthProvider();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            DeltaConnectorFactory.create(
                "https://example.com",
                Map.of("unity.temporary-table-vend-path", "not-absolute"),
                auth,
                Map.of()));

    assertTrue(auth.closed);
  }

  @Test
  void nonPositiveTimeoutOptionsFailAtConnectorCreation() {
    // http.read.ms reaches the client as a per-request timeout, which the JDK only rejects when a
    // request is built, where the client classifies it as a retryable TRANSPORT failure on every
    // call. It has to fail while the connector is being created.
    for (String option : new String[] {"http.read.ms", "http.connect.ms"}) {
      for (String value : new String[] {"0", "-1"}) {
        var auth = new ClosableAuthProvider();

        var failure =
            assertThrows(
                IllegalArgumentException.class,
                () ->
                    DeltaConnectorFactory.create(
                        "https://example.com", Map.of(option, value), auth, Map.of()),
                option + "=" + value);

        // Named against the connector option, not the client's internal parameter.
        assertTrue(failure.getMessage().contains(option), failure.getMessage());
        assertTrue(auth.closed, option + "=" + value);
      }
    }
  }

  @Test
  void aCloseFailureDuringCleanupIsSuppressedOntoTheOriginalFailure() {
    var auth =
        new ClosableAuthProvider() {
          @Override
          public void close() {
            closed = true;
            throw new IllegalStateException("close blew up");
          }
        };

    var failure =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaConnectorFactory.create("http://example.com", Map.of(), auth, Map.of()));

    // The construction failure is what the caller sees; the cleanup failure rides along rather
    // than replacing it.
    assertTrue(auth.closed);
    assertEquals(1, failure.getSuppressed().length);
    assertEquals("close blew up", failure.getSuppressed()[0].getMessage());
  }

  @Test
  void failedUnitySetupBeforeClientConstructionClosesAuthProvider() {
    var auth = new ClosableAuthProvider();

    var failure =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DeltaConnectorFactory.create(
                    "https://example.com", Map.of("http.connect.ms", "invalid"), auth, Map.of()));
    assertTrue(failure.getMessage().contains("http.connect.ms"), failure.getMessage());

    assertTrue(auth.closed);
  }

  private static class ClosableAuthProvider implements AuthProvider, AutoCloseable {
    boolean closed;

    @Override
    public String scheme() {
      return "oauth2";
    }

    @Override
    public Map<String, String> apply(Map<String, String> baseProps) {
      return baseProps;
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
