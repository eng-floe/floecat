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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.client.unity.UnityCatalogClient;
import ai.floedb.floecat.client.unity.UnityCatalogException;
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
  void listingPreservesUnityCatalogFailureClassification() {
    var failure =
        new UnityCatalogException(
            UnityCatalogException.Failure.PERMISSION_DENIED, 403, "catalog denied request");
    when(catalog.listCatalogs()).thenThrow(failure);
    when(catalog.listTables("cat", "schema")).thenThrow(failure);

    assertThatThrownBy(connector::listNamespaces).isSameAs(failure);
    assertThatThrownBy(() -> connector.listTables("cat.schema")).isSameAs(failure);
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
    when(catalog.getTableWithLenientColumns("cat.schema.orders")).thenReturn(Optional.of(table));
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
    // The lenient decode, not getTable: planning reads only the location, so a columns field this
    // path never looks at must not fail it.
    when(catalog.getTableWithLenientColumns("cat.schema.orders")).thenReturn(Optional.of(table));

    assertThat(connector.storageLocation("cat.schema", "orders")).isEqualTo("s3://bucket/orders");
    assertThat(connector.storageLocation("cat.schema", "orders")).isEqualTo("s3://bucket/orders");

    verify(catalog, org.mockito.Mockito.times(2)).getTableWithLenientColumns("cat.schema.orders");
    verify(catalog, org.mockito.Mockito.never()).getTable("cat.schema.orders");
  }

  @Test
  void aFailedLookupReportsItsKindAndStatusWithoutTheResponseBody() {
    // GrpcReconcilerBackend.isMissingObjectFailure matches "not found" against the top-level
    // message and does not walk causes, so a gateway error page reaching that message turns a
    // retryable 502 into a permanently missing table.
    String gatewayPage =
        "<html><body>The requested URL was not found on this server.</body></html>";
    var failure =
        new UnityCatalogException(
            UnityCatalogException.Failure.SERVER_ERROR,
            502,
            "Unity Catalog returned HTTP 502 for cat.schema.orders: " + gatewayPage);
    // One stub covers both calls below: describe now reaches the catalog leniently first, so it
    // fails here exactly as storageLocation does.
    when(catalog.getTableWithLenientColumns("cat.schema.orders")).thenThrow(failure);

    for (Runnable call :
        List.<Runnable>of(
            () -> connector.storageLocation("cat.schema", "orders"),
            () -> connector.describe("cat.schema", "orders"))) {
      assertThatThrownBy(call::run)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              rethrown -> {
                assertThat(rethrown.getMessage()).doesNotContain("not found");
                assertThat(rethrown.getMessage()).contains("SERVER_ERROR").contains("502");
                // Kind and status survive for the caller; the body survives for the log.
                assertThat(rethrown.failure())
                    .isEqualTo(UnityCatalogException.Failure.SERVER_ERROR);
                assertThat(rethrown.statusCode()).isEqualTo(502);
                assertThat(rethrown.getCause()).isSameAs(failure);
              });
    }
  }

  @Test
  void describeReadsTheCatalogLenientlyBeforeItNeedsTheColumnList() {
    // The descriptor's own fields come from the lenient decode, so a malformed columns field cannot
    // fail the call before the Delta log is consulted. Strict is escalated to only when the column
    // list turns out to be the answer -- which is what happens here, because a unit test has no
    // readable Delta log, and is exactly why this cannot assert "strict is never consulted".
    var table = table("orders", "EXTERNAL", "DELTA", "s3://bucket/orders");
    when(catalog.getTableWithLenientColumns("cat.schema.orders")).thenReturn(Optional.of(table));
    when(catalog.getTable("cat.schema.orders")).thenReturn(Optional.of(table));

    var descriptor = connector.describe("cat.schema", "orders");

    assertThat(descriptor.storageLocation()).isEqualTo("s3://bucket/orders");
    // The lenient fetch happens first and unconditionally; the strict one only on the fallback.
    var order = org.mockito.Mockito.inOrder(catalog);
    order.verify(catalog).getTableWithLenientColumns("cat.schema.orders");
    order.verify(catalog).getTable("cat.schema.orders");
  }

  @Test
  void describeFallsBackToTheStrictDecodeWhenTheColumnListIsTheAnswer() {
    // No storage location, so nothing overwrites the catalog's columns and they are reported as
    // authoritative. That is the one case where a malformed field must fail rather than yield an
    // empty schema, so the strict decode is fetched for it.
    var lenient = table("orders", "EXTERNAL", "DELTA", null);
    when(catalog.getTableWithLenientColumns("cat.schema.orders")).thenReturn(Optional.of(lenient));
    when(catalog.getTable("cat.schema.orders"))
        .thenThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.INVALID_RESPONSE,
                -1,
                "Expected 'columns' to be an array from Unity Catalog"));

    assertThatThrownBy(() -> connector.describe("cat.schema", "orders"))
        .isInstanceOf(UnityCatalogException.class);
    verify(catalog).getTable("cat.schema.orders");
  }

  @Test
  void aDescribeViewFailureCarriesNeitherTheBodyNorAMissingObjectPhrase() {
    // Same hazard as the table path: describeView's failure reaches the reconciler's top-level
    // message match, so a gateway page saying "not found" would be reported as VIEW_MISSING.
    var failure =
        new UnityCatalogException(
            UnityCatalogException.Failure.SERVER_ERROR,
            502,
            "Unity Catalog returned HTTP 502 for cat.schema.v: "
                + "<html>The requested URL was not found on this server.</html>");
    when(catalog.getTable("cat.schema.v")).thenThrow(failure);

    assertThatThrownBy(() -> connector.describeView("cat.schema", "v"))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            rethrown -> {
              assertThat(rethrown.getMessage().toLowerCase(java.util.Locale.ROOT))
                  .doesNotContain("not found")
                  .doesNotContain("does not exist")
                  .doesNotContain("http 404")
                  .doesNotContain("status 404");
              assertThat(rethrown.getCause()).isSameAs(failure);
            });
  }

  @Test
  void aDeniedFourOhFourIsNotRenderedAsAMissingObject() {
    // A workspace hiding a table it will not admit exists answers 404 with an error_code the client
    // classifies as PERMISSION_DENIED. Writing "HTTP 404" into the message would hand that back to
    // the reconciler as a missing table -- terminal for the wrong reason, and the wrong reason is
    // the one that stops anyone looking at permissions.
    var denied =
        new UnityCatalogException(
            UnityCatalogException.Failure.PERMISSION_DENIED,
            404,
            "PERMISSION_DENIED",
            true,
            "Unity Catalog returned HTTP 404 for cat.schema.orders",
            null);
    when(catalog.getTableWithLenientColumns("cat.schema.orders")).thenThrow(denied);

    assertThatThrownBy(() -> connector.storageLocation("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            rethrown -> {
              assertThat(rethrown.getMessage().toLowerCase(java.util.Locale.ROOT))
                  .doesNotContain("http 404")
                  .doesNotContain("status 404")
                  .doesNotContain("not found");
              assertThat(rethrown.getMessage()).contains("PERMISSION_DENIED").contains("404");
              assertThat(rethrown.failure())
                  .isEqualTo(UnityCatalogException.Failure.PERMISSION_DENIED);
            });
  }

  @Test
  void anObjectNameThatWouldTriggerTheHeuristicIsDroppedFromTheMessage() {
    // The name is catalog-controlled, so it can carry a trigger phrase of its own.
    var failure =
        new UnityCatalogException(UnityCatalogException.Failure.SERVER_ERROR, 502, "upstream");
    when(catalog.getTableWithLenientColumns("cat.schema.not found")).thenThrow(failure);

    assertThatThrownBy(() -> connector.storageLocation("cat.schema", "not found"))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            rethrown -> {
              assertThat(rethrown.getMessage().toLowerCase(java.util.Locale.ROOT))
                  .doesNotContain("not found");
              assertThat(rethrown.getMessage()).contains("SERVER_ERROR");
            });
  }

  @Test
  void aGenuinelyMissingTableIsStillReportedAsNotFound() {
    // The other side: getTable folds NOT_FOUND into an empty Optional, so a missing table arrives
    // as the IllegalStateException whose message the reconciler matches on purpose. Rewriting the
    // failure message above must not disturb that.
    when(catalog.getTableWithLenientColumns("cat.schema.orders")).thenReturn(Optional.empty());

    assertThatThrownBy(() -> connector.storageLocation("cat.schema", "orders"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not found");
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
    // Lenient: this path reads properties() only, so malformed columns must not cost the table its
    // constraints -- the surrounding catch would swallow that into an empty map.
    when(catalog.getTableWithLenientColumns("cat.schema.orders")).thenReturn(Optional.of(table));

    assertThat(connector.fallbackTablePropertiesForConstraints("cat.schema", "orders"))
        .containsEntry("delta.constraints.positive", "amount > 0");
    verify(catalog, org.mockito.Mockito.never()).getTable("cat.schema.orders");
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
  void describeUsesTypeTextWhenTypeJsonIsAbsent() {
    // The path OSS Unity Catalog and older API versions take: no type_json, so the declared type
    // text is what reaches DeltaSchemaMapper.
    var column = new UnityCatalogTable.Column("total", "DECIMAL", "decimal(10,2)", null, true);
    var table =
        new UnityCatalogTable(
            "orders", "id", "EXTERNAL", "DELTA", null, null, List.of(column), Map.of());
    when(catalog.getTableWithLenientColumns("cat.schema.orders")).thenReturn(Optional.of(table));
    when(catalog.getTable("cat.schema.orders")).thenReturn(Optional.of(table));

    var descriptor = connector.describe("cat.schema", "orders");

    // type_text wins over type_name when both are present.
    assertThat(descriptor.schemaJson()).contains("\"type\":\"decimal(10,2)\"");
    assertThat(descriptor.schemaJson()).doesNotContain("DECIMAL");
  }

  @Test
  void describeEmitsAnEmptyTypeWhenEveryTypeSpellingIsAbsent() {
    // An empty string rather than a literal null: DeltaSchemaMapper reads the field with asText(),
    // which reads a JSON null back as the string "null".
    var column = new UnityCatalogTable.Column("mystery", null, null, null, true);
    var table =
        new UnityCatalogTable(
            "orders", "id", "EXTERNAL", "DELTA", null, null, List.of(column), Map.of());
    when(catalog.getTableWithLenientColumns("cat.schema.orders")).thenReturn(Optional.of(table));
    when(catalog.getTable("cat.schema.orders")).thenReturn(Optional.of(table));

    var descriptor = connector.describe("cat.schema", "orders");

    assertThat(descriptor.schemaJson()).contains("\"type\":\"\"");
    // Not a literal JSON null, which asText() reads back as "null". Checks the type field
    // specifically, since "nullable" contains the substring.
    assertThat(descriptor.schemaJson()).doesNotContain("\"type\":null");
  }

  @Test
  void listViewsReturnsOnlyViewTableTypesAndIgnoresDeltaTables() {
    when(catalog.listTables("cat", "schema"))
        .thenReturn(
            List.of(
                table("orders", "EXTERNAL", "DELTA"),
                table("revenue", "VIEW", null),
                table("managed", "MANAGED", "DELTA"),
                table("summary", "view", null)));

    // Case-insensitive on table_type, and sorted.
    assertThat(connector.listViews("cat.schema")).containsExactly("revenue", "summary");
  }

  @Test
  void describeViewReturnsEmptyWhenClientDoesNotFindIt() {
    when(catalog.getTable("cat.schema.missing")).thenReturn(Optional.empty());

    assertThat(connector.describeView("cat.schema", "missing")).isEmpty();
  }

  @Test
  void closeReleasesTheEngineResources() {
    // Nothing else retains the refreshing S3 client the engine is built on, so the connector's
    // close is its only release point.
    var released = new java.util.concurrent.atomic.AtomicBoolean();
    var connector =
        new UnityDeltaConnector(
            "delta-unity", catalog, null, null, null, false, 0.0, 0, () -> released.set(true));

    connector.close();

    assertThat(released).isTrue();
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
