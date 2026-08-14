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

package ai.floedb.floecat.catalog.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CatalogClientFactoryTest {
  @Test
  void routesByCatalogProtocol() {
    StubClient client = new StubClient();
    CatalogClientProvider provider = provider(CatalogProtocol.ICEBERG_REST, client);
    CatalogClientFactory factory = new CatalogClientFactory(List.of(provider));

    CatalogClient opened =
        factory.open(config(CatalogProtocol.ICEBERG_REST), ResolvedCatalogCredentials.none());

    assertSame(client, opened);
  }

  @Test
  void rejectsDuplicateProviders() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new CatalogClientFactory(
                List.of(
                    provider(CatalogProtocol.ICEBERG_REST, new StubClient()),
                    provider(CatalogProtocol.ICEBERG_REST, new StubClient()))));
  }

  @Test
  void reportsMissingProvider() {
    CatalogClientFactory factory = new CatalogClientFactory(List.of());

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () ->
                factory.open(
                    config(CatalogProtocol.UNITY_CATALOG), ResolvedCatalogCredentials.none()));

    assertEquals("No CatalogClientProvider for protocol=UNITY_CATALOG", error.getMessage());
  }

  @Test
  void credentialsNeverRenderSecretValues() {
    ResolvedCatalogCredentials credentials =
        new ResolvedCatalogCredentials(
            Map.of("token", "secret-token"),
            Map.of("Authorization", "Bearer secret-token"),
            Instant.parse("2026-08-05T12:00:00Z"));

    assertFalse(credentials.toString().contains("secret-token"));
    assertFalse(credentials.toString().contains("Bearer"));
  }

  @Test
  void rejectsEndpointUserInfoFromPersistableConfiguration() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new CatalogConnectionConfig(
                    CatalogProtocol.ICEBERG_REST,
                    URI.create("https://user:secret@catalog.example/v1"),
                    Map.of(),
                    CatalogAuthentication.none()));

    assertEquals("endpoint must not contain user-info", error.getMessage());
  }

  private static CatalogConnectionConfig config(CatalogProtocol protocol) {
    return new CatalogConnectionConfig(
        protocol, URI.create("https://catalog.example/v1"), Map.of(), CatalogAuthentication.none());
  }

  private static CatalogClientProvider provider(CatalogProtocol protocol, CatalogClient client) {
    return new CatalogClientProvider() {
      @Override
      public CatalogProtocol protocol() {
        return protocol;
      }

      @Override
      public CatalogClient open(
          CatalogConnectionConfig config, ResolvedCatalogCredentials resolvedCredentials) {
        return client;
      }
    };
  }

  private static final class StubClient implements CatalogClient {
    @Override
    public CatalogCapabilities capabilities() {
      return CatalogCapabilities.of();
    }

    @Override
    public void validate() {}

    @Override
    public List<NamespacePath> listNamespaces(NamespacePath parent) {
      return List.of();
    }

    @Override
    public List<CatalogObjectName> listTables(NamespacePath namespace) {
      return List.of();
    }

    @Override
    public CatalogTable loadTable(CatalogObjectName table) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogObjectName> listViews(NamespacePath namespace) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CatalogView loadView(CatalogObjectName view) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.util.Optional<VendedStorageCredentials> vendStorageCredentials(
        CatalogObjectName table) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void validateStorageAccess(
        CatalogObjectName table, VendedStorageCredentials vendedStorageCredentials) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}
  }
}
