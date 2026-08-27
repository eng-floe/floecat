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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.client.unity.UnityCatalogClient;
import ai.floedb.floecat.client.unity.UnityCatalogTable;
import ai.floedb.floecat.connector.spi.AuthProvider;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnityDeltaConnectorTest {
  private UnityCatalogClient catalog;
  private UnityDeltaConnector connector;

  @BeforeEach
  void setUp() {
    catalog = mock(UnityCatalogClient.class);
    connector = new UnityDeltaConnector("test-id", catalog, null, null, null, false, 0.0, 0);
  }

  @Test
  void listNamespacesCombinesCatalogsAndSchemasAndSorts() {
    when(catalog.listCatalogs()).thenReturn(List.of("z", "a"));
    when(catalog.listSchemas("z")).thenReturn(List.of("two", "one"));
    when(catalog.listSchemas("a")).thenReturn(List.of("default"));

    assertThat(connector.listNamespaces()).containsExactly("a.default", "z.one", "z.two");
  }

  @Test
  void listTablesFiltersNonDeltaEntries() {
    when(catalog.listTables("cat", "schema"))
        .thenReturn(
            List.of(
                table("view", "VIEW", null),
                table("z_orders", "MANAGED", "DELTA"),
                table("a_orders", "EXTERNAL", "delta")));

    assertThat(connector.listTables("cat.schema")).containsExactly("a_orders", "z_orders");
  }

  @Test
  void malformedNamespaceDoesNotCallClient() {
    assertThat(connector.listTables("missing_separator")).isEmpty();
    assertThat(connector.listViews("missing_separator")).isEmpty();

    verify(catalog, never())
        .listTables(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
  }

  @Test
  void describeMapsNormalizedTableMetadata() {
    var column =
        new UnityCatalogTable.Column(
            "tags",
            "ARRAY",
            "array<string>",
            "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"string\"}}",
            true);
    var table =
        new UnityCatalogTable(
            "orders", "id", "EXTERNAL", "DELTA", null, null, List.of(column), Map.of());
    when(catalog.getTable("cat.schema.orders")).thenReturn(Optional.of(table));

    var descriptor = connector.describe("cat.schema", "orders");

    assertThat(descriptor.schemaJson()).contains("\"elementType\":\"string\"");
    assertThat(descriptor.schemaJson()).doesNotContain("array<string>");
    assertThat(descriptor.properties())
        .containsEntry("table_type", "EXTERNAL")
        .containsEntry("data_source_format", "DELTA");
  }

  @Test
  void storageLocationLoadsThroughClientEachTime() {
    var table = table("orders", "EXTERNAL", "DELTA", "s3://bucket/orders");
    when(catalog.getTable("cat.schema.orders")).thenReturn(Optional.of(table));

    assertThat(connector.storageLocation("cat.schema", "orders")).isEqualTo("s3://bucket/orders");
    assertThat(connector.storageLocation("cat.schema", "orders")).isEqualTo("s3://bucket/orders");

    verify(catalog, org.mockito.Mockito.times(2)).getTable("cat.schema.orders");
  }

  @Test
  void constraintFallbackUsesNormalizedProperties() {
    var table =
        new UnityCatalogTable(
            "orders",
            "id",
            "EXTERNAL",
            "DELTA",
            null,
            null,
            List.of(),
            Map.of("delta.constraints.positive", "amount > 0"));
    when(catalog.getTable("cat.schema.orders")).thenReturn(Optional.of(table));

    assertThat(connector.fallbackTablePropertiesForConstraints("cat.schema", "orders"))
        .containsEntry("delta.constraints.positive", "amount > 0");
  }

  @Test
  void listViewDescriptorsUsesSingleTypedListAndSchemaOnlySearchPath() {
    var column = new UnityCatalogTable.Column("amount", "DOUBLE", null, null, false);
    var view =
        new UnityCatalogTable(
            "revenue",
            "view-id",
            "VIEW",
            null,
            null,
            "SELECT amount FROM sales",
            List.of(column),
            Map.of());
    when(catalog.listTables("cat", "nested.schema"))
        .thenReturn(List.of(table("orders", "EXTERNAL", "DELTA"), view));

    var descriptors = connector.listViewDescriptors("cat.nested.schema");

    assertThat(descriptors).hasSize(1);
    assertThat(descriptors.getFirst().name()).isEqualTo("revenue");
    assertThat(descriptors.getFirst().sql()).isEqualTo("SELECT amount FROM sales");
    assertThat(descriptors.getFirst().searchPath()).containsExactly("nested", "schema");
    assertThat(descriptors.getFirst().schemaJson()).contains("amount", "DOUBLE");
    verify(catalog).listTables("cat", "nested.schema");
  }

  @Test
  void describeViewReturnsEmptyWhenClientDoesNotFindIt() {
    when(catalog.getTable("cat.schema.missing")).thenReturn(Optional.empty());

    assertThat(connector.describeView("cat.schema", "missing")).isEmpty();
  }

  @Test
  void closeReleasesTheCatalogTransport() {
    connector.close();

    verify(catalog).close();
  }

  @Test
  void closeReleasesTheAuthProvider() {
    var auth = new ClosableAuthProvider();
    try (var scoped =
        new UnityDeltaConnector("test-id", catalog, auth, null, null, false, 0.0, 0)) {
      assertThat(auth.closed).isFalse();
    }

    verify(catalog).close();
    assertThat(auth.closed).isTrue();
  }

  /** A failure closing the catalog transport must not skip the auth provider. */
  @Test
  void closeReleasesTheAuthProviderEvenWhenTheCatalogCloseFails() {
    var auth = new ClosableAuthProvider();
    doThrow(new IllegalStateException("boom")).when(catalog).close();

    new UnityDeltaConnector("test-id", catalog, auth, null, null, false, 0.0, 0).close();

    assertThat(auth.closed).isTrue();
  }

  private static final class ClosableAuthProvider implements AuthProvider, AutoCloseable {
    private boolean closed;

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

  private static UnityCatalogTable table(String name, String type, String format) {
    return table(name, null, type, format, null);
  }

  private static UnityCatalogTable table(String name, String type, String format, String location) {
    return table(name, null, type, format, location);
  }

  private static UnityCatalogTable table(
      String name, String id, String type, String format, String location) {
    return new UnityCatalogTable(name, id, type, format, location, null, List.of(), Map.of());
  }
}
