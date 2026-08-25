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

import ai.floedb.floecat.client.unity.TemporaryTableCredentials;
import ai.floedb.floecat.client.unity.UnityCatalogClient;
import ai.floedb.floecat.client.unity.UnityCatalogException;
import ai.floedb.floecat.client.unity.UnityCatalogTable;
import ai.floedb.floecat.connector.spi.SourceCatalogAccessException;
import java.time.Instant;
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
    connector = new UnityDeltaConnector("test-id", catalog, null, null, false, 0.0, 0);
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
  void vendStorageCredentialsMapsAwsCredentials() {
    when(catalog.getTable("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "table-id", "EXTERNAL", "DELTA", null)));
    Instant expiry = Instant.parse("2030-01-01T00:00:00Z");
    when(catalog.generateTemporaryTableCredentials(
            "table-id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(
            new TemporaryTableCredentials(
                new TemporaryTableCredentials.AwsCredentials(
                    "key", "secret", "token", "arn:aws:s3:us-east-1:123:accesspoint/orders"),
                false,
                Long.toString(expiry.toEpochMilli()),
                "s3://bucket/orders"));

    var result = connector.vendStorageCredentials("cat.schema", "orders");

    assertThat(result).isPresent();
    assertThat(result.orElseThrow().properties())
        .containsEntry("s3.access-key-id", "key")
        .containsEntry("s3.secret-access-key", "secret")
        .containsEntry("s3.session-token", "token")
        .containsEntry("s3.access-point", "arn:aws:s3:us-east-1:123:accesspoint/orders");
    assertThat(result.orElseThrow().expiresAt()).isEqualTo(expiry);
    assertThat(result.orElseThrow().scopePrefix()).isEqualTo("s3://bucket/orders");
  }

  @Test
  void vendStorageCredentialsFallsBackForMissingTableOrUnsupportedCloud() {
    when(catalog.getTable("cat.schema.missing")).thenReturn(Optional.empty());
    when(catalog.getTable("cat.schema.azure"))
        .thenReturn(Optional.of(table("azure", "id", "EXTERNAL", "DELTA", null)));
    when(catalog.generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(new TemporaryTableCredentials(null, true, null, null));

    assertThat(connector.vendStorageCredentials("cat.schema", "missing")).isEmpty();
    assertThat(connector.vendStorageCredentials("cat.schema", "azure")).isEmpty();
  }

  @Test
  void vendStorageCredentialsFallsBackForAnUnrecognizedCredentialShape() {
    // Neither an AWS tuple nor a cloud this client knows about -- a UC response shape newer than
    // this code. "Cannot vend" must stay a fallback to a storage authority: handing back an empty
    // credential object instead would reach the service's usability check and fail the reconcile
    // job terminally on a condition the caller can recover from.
    when(catalog.getTable("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    when(catalog.generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(new TemporaryTableCredentials(null, false, "1893456000000", "s3://b/orders"));

    assertThat(connector.vendStorageCredentials("cat.schema", "orders")).isEmpty();
  }

  @Test
  void vendStorageCredentialsClassifiesPermanentNonAuthRefusalsAsTerminal() {
    // Databricks answers with 400 + error_code when the workspace lacks EXTERNAL USE SCHEMA, and
    // with 404 for a table id it no longer knows. Both are permanent; left unclassified they
    // escape as a retryable INTERNAL and the reconciler loops on a job that can never succeed.
    when(catalog.getTable("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    for (UnityCatalogException.Failure failure :
        new UnityCatalogException.Failure[] {
          UnityCatalogException.Failure.INVALID_REQUEST, UnityCatalogException.Failure.NOT_FOUND
        }) {
      // doThrow, not when(...).thenThrow: when() would evaluate the already-stubbed call and
      // rethrow the previous iteration's exception before it could re-stub.
      doThrow(new UnityCatalogException(failure, 400, "refused"))
          .when(catalog)
          .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

      assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
          .as(failure.name())
          .isInstanceOfSatisfying(
              SourceCatalogAccessException.class,
              error ->
                  assertThat(error.denial())
                      .isEqualTo(SourceCatalogAccessException.Denial.UNSUPPORTED));
    }
  }

  @Test
  void closeReleasesTheCatalogTransport() {
    connector.close();

    verify(catalog).close();
  }

  @Test
  void vendStorageCredentialsPreservesIncompleteAwsTupleForServiceValidation() {
    when(catalog.getTable("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    when(catalog.generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(
            new TemporaryTableCredentials(
                new TemporaryTableCredentials.AwsCredentials("key", "secret", null, null),
                false,
                "not-a-number",
                null));

    var result = connector.vendStorageCredentials("cat.schema", "orders");

    assertThat(result).isPresent();
    assertThat(result.orElseThrow().properties())
        .containsEntry("s3.access-key-id", "key")
        .containsEntry("s3.secret-access-key", "secret")
        .doesNotContainKey("s3.session-token");
    // A malformed expiry folds to null rather than failing the vend, per the shared parser.
    assertThat(result.orElseThrow().expiresAt()).isNull();
  }

  @Test
  void vendStorageCredentialsClassifiesAuthenticationFailures() {
    when(catalog.getTable("cat.schema.orders"))
        .thenThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.UNAUTHENTICATED, 401, "HTTP 401"));

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error ->
                assertThat(error.denial())
                    .isEqualTo(SourceCatalogAccessException.Denial.UNAUTHENTICATED));
  }

  @Test
  void vendStorageCredentialsLeavesServerFailuresRetryable() {
    when(catalog.getTable("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    when(catalog.generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ))
        .thenThrow(
            new UnityCatalogException(UnityCatalogException.Failure.SERVER_ERROR, 503, "HTTP 503"));

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOf(UnityCatalogException.class)
        .isNotInstanceOf(SourceCatalogAccessException.class);
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
