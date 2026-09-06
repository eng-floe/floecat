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
import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.connector.spi.SourceCatalogAccessException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
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
  void vendStorageCredentialsMapsAwsCredentials() {
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
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
  void vendStorageCredentialsFallsBackForAnUnsupportedCloud() {
    // A cloud this connector does not map is "cannot vend": a configured storage authority is the
    // right answer, so this falls back rather than failing.
    when(catalog.getTableWithLenientColumns("cat.schema.azure"))
        .thenReturn(Optional.of(table("azure", "id", "EXTERNAL", "DELTA", null)));
    when(catalog.generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(new TemporaryTableCredentials(null, true, null, null));

    assertThat(connector.vendStorageCredentials("cat.schema", "azure")).isEmpty();
  }

  @Test
  void vendStorageCredentialsFallsBackForAnUnrecognizedCredentialShape() {
    // Neither an AWS tuple nor a cloud this client knows about -- a UC response shape newer than
    // this code. "Cannot vend" must stay a fallback to a storage authority: handing back an empty
    // credential object instead would reach the service's usability check and fail the reconcile
    // job terminally on a condition the caller can recover from.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
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
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    for (UnityCatalogException.Failure failure :
        new UnityCatalogException.Failure[] {
          UnityCatalogException.Failure.INVALID_REQUEST, UnityCatalogException.Failure.NOT_FOUND
        }) {
      // doThrow, not when(...).thenThrow: when() would evaluate the already-stubbed call and
      // rethrow the previous iteration's exception before it could re-stub.
      // errorCode set: only a failure Databricks itself envelopes is permanent. See
      // notFoundWithoutAnErrorEnvelopeStaysRetryable.
      doThrow(new UnityCatalogException(failure, 400, "RESOURCE_DOES_NOT_EXIST", "refused", null))
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
  void aTableLookupNotFoundStaysRetryable() {
    // The same carve-out on the lookup rather than the credentials POST: getTable erases the
    // envelope by folding NOT_FOUND into an empty Optional, so this leg cannot terminalize either.
    when(catalog.getTableWithLenientColumns("cat.schema.orders")).thenReturn(Optional.empty());

    assertThat(connector.vendStorageCredentials("cat.schema", "orders")).isEmpty();
    verify(catalog, never())
        .generateTemporaryTableCredentials(
            org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
  }

  @Test
  void aLocalAuthFailureIsTerminalDespiteHavingNoEnvelope() {
    // statusCode -1: the request never reached the workspace. A misconfigured auth provider, or one
    // returning a header the request cannot carry, never clears on retry, so the missing envelope
    // must not buy it the ambiguity carve-out -- and it is an authentication failure, not the
    // "catalog will not vend for this table" that UNSUPPORTED reports.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    doThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.INVALID_REQUEST,
                -1,
                "Unity Catalog authentication is misconfigured"))
        .when(catalog)
        .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error ->
                assertThat(error.denial())
                    .isEqualTo(SourceCatalogAccessException.Denial.UNAUTHENTICATED));
  }

  @Test
  void aMalformedCredentialPayloadIsTerminalButAGarbledBodyIsNot() {
    // Both are INVALID_RESPONSE; the status separates them. No status means the client rejected the
    // shape of a body that had already parsed -- aws_temp_credentials without an access key and
    // secret -- which the catalog will send again, so untyped it becomes INTERNAL at the service
    // and retries forever. A real status means the body never became JSON at all, which is what a
    // proxy answering mid-deploy looks like, and that may parse on the next attempt.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));

    doThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.INVALID_RESPONSE,
                -1,
                "Unity Catalog returned aws_temp_credentials without an access key and secret"))
        .when(catalog)
        .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error ->
                assertThat(error.denial())
                    .isEqualTo(SourceCatalogAccessException.Denial.UNSUPPORTED));

    UnityCatalogException garbled =
        new UnityCatalogException(
            UnityCatalogException.Failure.INVALID_RESPONSE, 200, "response was not JSON");
    doThrow(garbled)
        .when(catalog)
        .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

    // Escapes untyped, which is what keeps it retryable at the service.
    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isSameAs(garbled);
  }

  @Test
  void aVendLookupFailureDoesNotCarryTheResponseBodyIntoTheTerminalReason() {
    // The table lookup on the vend path is wrapped like every other one. Without it a proxy's error
    // page reaches describeRefusal, which builds the terminal reason -- so the body would land in
    // the gRPC status description and the persisted reconcile failure text.
    String proxyPage = "<html>The requested URL was not found on this server.</html>";
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.PERMISSION_DENIED,
                403,
                "PERMISSION_DENIED",
                true,
                "Unity Catalog returned HTTP 403 for cat.schema.orders: " + proxyPage,
                null));

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error -> {
              assertThat(error.getMessage()).doesNotContain("requested URL");
              assertThat(error.getMessage().toLowerCase(java.util.Locale.ROOT))
                  .doesNotContain("not found");
              assertThat(error.denial())
                  .isEqualTo(SourceCatalogAccessException.Denial.PERMISSION_DENIED);
            });
  }

  @Test
  void aVendSucceedsWhenOnlyTheColumnMetadataIsMalformed() {
    // Vending reads the table id and nothing else. The strict decode would refuse the response for
    // a columns field this path never looks at, and classifyAccessFailure would read that
    // INVALID_RESPONSE as a permanent refusal -- failing the vend on unrelated metadata.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    when(catalog.generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ))
        .thenReturn(
            new TemporaryTableCredentials(
                new TemporaryTableCredentials.AwsCredentials("ak", "sk", "tok", null),
                false,
                null,
                "s3://bucket/orders"));

    assertThat(connector.vendStorageCredentials("cat.schema", "orders")).isPresent();
    verify(catalog, org.mockito.Mockito.never()).getTable("cat.schema.orders");
  }

  @Test
  void noVendFailureLeavesAMessageTheReconcilerReadsAsAMissingTable() {
    // The real client message shape, not a stand-in. JavaConnectorCaptureEngine catches the storage
    // RPC failure inside its try-with-resources and matches "http 404" on the top-level message;
    // ReconcileExecutor maps TABLE_MISSING to OBSOLETE, retiring the source table. A wrong
    // unity.temporary-table-vend-path is enough to produce that 404.
    record Case(String name, UnityCatalogException failure) {}
    String credentialsPath = "/api/2.0/unity-catalog/temporary-table-credentials";
    var cases =
        List.of(
            // Enveloped 404 from the credentials leg: terminal, and must not read as missing.
            new Case(
                "enveloped credentials 404",
                new UnityCatalogException(
                    UnityCatalogException.Failure.NOT_FOUND,
                    404,
                    "RESOURCE_DOES_NOT_EXIST",
                    true,
                    "Unity Catalog returned HTTP 404 for " + credentialsPath,
                    null)),
            // Unenveloped 404: the carve-out keeps it retryable, which the heuristic would undo.
            new Case(
                "unenveloped credentials 404",
                new UnityCatalogException(
                    UnityCatalogException.Failure.NOT_FOUND,
                    404,
                    "Unity Catalog returned HTTP 404 for " + credentialsPath)),
            // The second vector: errorCode is unfiltered on the lookup route, so the catalog can
            // put the trigger phrase in the reason of a call that was wrapped.
            // Reaches the switch default. Not producible from the client today -- the credentials
            // leg suppresses bodies and the lookup leg is scrubbed on the way in -- so this pins
            // the arm as a guard rather than a fix for a live path.
            new Case(
                "retryable failure carrying the phrase",
                new UnityCatalogException(
                    UnityCatalogException.Failure.SERVER_ERROR,
                    502,
                    "Unity Catalog returned HTTP 502 for " + credentialsPath + ": not found")),
            new Case(
                "error_code carrying the phrase",
                new UnityCatalogException(
                    UnityCatalogException.Failure.INVALID_REQUEST,
                    400,
                    "table not found",
                    true,
                    "Unity Catalog returned HTTP 400 for " + credentialsPath,
                    null)));

    for (Case c : cases) {
      when(catalog.getTableWithLenientColumns("cat.schema.orders"))
          .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
      doThrow(c.failure())
          .when(catalog)
          .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

      assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
          .as(c.name())
          .isInstanceOfSatisfying(
              RuntimeException.class,
              thrown -> {
                String message = String.valueOf(thrown.getMessage()).toLowerCase(Locale.ROOT);
                assertThat(message).as(c.name()).doesNotContain("http 404");
                assertThat(message).as(c.name()).doesNotContain("status 404");
                assertThat(message).as(c.name()).doesNotContain("not found");
                assertThat(message).as(c.name()).doesNotContain("does not exist");
              });
    }
  }

  @Test
  void aLocalFailureKeepsTheTextThatNamesWhatToFix() {
    // A negative status carries no body, so there is nothing to suppress -- and the wrapper used to
    // replace this with "INVALID_REQUEST [-1]", which is the whole explanation an operator gets,
    // since SourceCatalogAccessException carries no cause.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.INVALID_REQUEST,
                -1,
                "Unity Catalog authentication is misconfigured"));

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error -> {
              assertThat(error.getMessage()).contains("authentication is misconfigured");
              assertThat(error.denial())
                  .isEqualTo(SourceCatalogAccessException.Denial.UNAUTHENTICATED);
            });
  }

  @Test
  void aTerminalRefusalCarriesTheErrorCode() {
    // The vending route suppresses response bodies, so the message alone is a bare "returned HTTP
    // 400". Once the refusal is terminal, the code is the only thing naming which permanent cause
    // fired -- a workspace without EXTERNAL USE SCHEMA, or a table with external access off.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    doThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.INVALID_REQUEST,
                400,
                "PERMISSION_DENIED",
                "Unity Catalog returned HTTP 400 for /api/2.0/unity-catalog/…",
                null))
        .when(catalog)
        .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOf(SourceCatalogAccessException.class)
        .hasMessageContaining("PERMISSION_DENIED")
        .hasMessageContaining("returned HTTP 400");
  }

  @Test
  void anOverlongErrorCodeAlreadyInTheMessageIsNotAppendedAgain() {
    // The client interpolated the full code into its own message on this route. Comparing the
    // bounded form against it would never match, so the code would be appended a second time in
    // its truncated shape.
    String code = "PERMISSION_DENIED_" + "X".repeat(300);
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    doThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.PERMISSION_DENIED,
                403,
                code,
                "Unity Catalog returned HTTP 403 (" + code + ") for /api/…",
                null))
        .when(catalog)
        .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error -> assertThat(error.getMessage()).doesNotContain("chars)"));
  }

  @Test
  void anErrorCodeIsFlattenedAndBoundedLikeTheMessage() {
    // The client shape-checks error_code only on the route that suppresses response bodies, so on
    // the table lookup it is whatever the catalog put in the envelope.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.PERMISSION_DENIED,
                403,
                "PERM\nDENIED" + "x".repeat(300),
                "Unity Catalog returned HTTP 403 for /api/2.1/unity-catalog/tables/…",
                null));

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error -> {
              assertThat(error.getMessage()).doesNotContain("\n");
              assertThat(error.getMessage()).hasSizeLessThan(400);
            });
  }

  @Test
  void aRefusalWithNeitherMessageNorCodeNamesTheFailureKind() {
    // A terminal exception with a null message is a permanently failed job carrying no reason at
    // all. The kind is thin, but it is not nothing.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    doThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.PERMISSION_DENIED, 403, null, null, null))
        .when(catalog)
        .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error -> assertThat(error.getMessage()).isEqualTo("PERMISSION_DENIED"));
  }

  @Test
  void aRefusalMessageIsFlattenedBeforeItBecomesAStatusDescription() {
    // The message carries up to two thousand characters of catalog or proxy body, and it ends up in
    // a log line and a gRPC status description.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.PERMISSION_DENIED,
                403,
                "PERMISSION_DENIED",
                "Unity Catalog returned HTTP 403 for /api/…: <html>\n2030-01-01 ERROR forged</html>",
                null));

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error -> {
              assertThat(error.getMessage()).doesNotContain("\n");
              assertThat(error.getMessage()).contains("PERMISSION_DENIED");
            });
  }

  @Test
  void aMessagelessRefusalStillReportsItsErrorCode() {
    // The code is the whole diagnostic here, and it feeds a terminal exception: folding this in
    // with "the code is already in the message" would leave a human reading a bare null.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    doThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.PERMISSION_DENIED,
                403,
                "PERMISSION_DENIED",
                null,
                null))
        .when(catalog)
        .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error -> assertThat(error.getMessage()).isEqualTo("PERMISSION_DENIED"));
  }

  @Test
  void anErrorCodeTheClientAlreadyInterpolatedIsNotRepeated() {
    // Every route except the credentials POST keeps the response body, and the client puts the code
    // in that message itself. The table lookup is one of them, so appending again would print the
    // code twice in the whole diagnostic left by a job that will not be retried.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    doThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.PERMISSION_DENIED,
                403,
                "PERMISSION_DENIED",
                "Unity Catalog returned HTTP 403 (PERMISSION_DENIED) for /api/2.1/…: "
                    + "{\"error_code\":\"PERMISSION_DENIED\"}",
                null))
        .when(catalog)
        .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error -> {
              assertThat(error.denial())
                  .isEqualTo(SourceCatalogAccessException.Denial.PERMISSION_DENIED);
              assertThat(error.getMessage()).endsWith("}");
              assertThat(error.getMessage().split("PERMISSION_DENIED", -1)).hasSize(3);
            });
  }

  @Test
  void anEnvelopedRefusalWhoseCodeWasWithheldSaysSo() {
    // Terminality already turns on the envelope. Without saying so here the operator reads a
    // permanently failed job as though the catalog gave no reason, when it is in the audit log.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    doThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.INVALID_REQUEST,
                400,
                null,
                true,
                "Unity Catalog returned HTTP 400 for /api/2.0/unity-catalog/…",
                null))
        .when(catalog)
        .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error -> assertThat(error.getMessage()).endsWith("(error code withheld)"));
  }

  @Test
  void anEnvelopedRefusalWhoseCodeWasWithheldIsStillTerminal() {
    // The vending route withholds a code it judges unsafe to show. Reading that as "no envelope"
    // would hand an enveloped permanent refusal back to be retried forever.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    doThrow(
            new UnityCatalogException(
                UnityCatalogException.Failure.INVALID_REQUEST,
                400,
                null,
                true,
                "Unity Catalog returned HTTP 400 for /api/2.0/unity-catalog/…",
                null))
        .when(catalog)
        .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOf(SourceCatalogAccessException.class);
  }

  @Test
  void anUnenvelopedInvalidRequestStaysRetryable() {
    // 405 and 422 come from status alone, and so does a 3xx the client declines to follow -- an SSO
    // proxy's redirect to a login page. None of those is the workspace refusing the request.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));

    for (int status : new int[] {405, 422, 302}) {
      doThrow(new UnityCatalogException(UnityCatalogException.Failure.INVALID_REQUEST, status, "?"))
          .when(catalog)
          .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

      assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
          .as("HTTP %d", status)
          .isInstanceOf(UnityCatalogException.class)
          .isNotInstanceOf(SourceCatalogAccessException.class);
    }
  }

  @Test
  void notFoundWithoutAnErrorEnvelopeStaysRetryable() {
    // The client types 404 as NOT_FOUND on status alone, so an HTML 404 from a load balancer in
    // front of the workspace -- what one serves mid-deploy -- is indistinguishable here from an
    // unknown table id except by the error_code envelope. Without one it must stay unclassified:
    // classifying it terminally fails the reconcile job on a condition that recovers by itself.
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    doThrow(new UnityCatalogException(UnityCatalogException.Failure.NOT_FOUND, 404, "<html/>"))
        .when(catalog)
        .generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ);

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOf(UnityCatalogException.class)
        .isNotInstanceOf(SourceCatalogAccessException.class);
  }

  /**
   * The credentials endpoint keys on table_id, so a table without one can never be vended for, and
   * a catalog that omits it will keep omitting it. An untyped failure escapes classification and
   * comes back from the service as a retryable INTERNAL, looping the reconcile job forever.
   */
  @Test
  void vendStorageCredentialsTreatsAMissingTableIdAsTerminal() {
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", " ", "EXTERNAL", "DELTA", null)));

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOfSatisfying(
            SourceCatalogAccessException.class,
            error ->
                assertThat(error.denial())
                    .isEqualTo(SourceCatalogAccessException.Denial.UNSUPPORTED));
  }

  @Test
  void closeReleasesTheCatalogTransport() {
    connector.close();

    verify(catalog).close();
  }

  @Test
  void vendStorageCredentialsPreservesIncompleteAwsTupleForServiceValidation() {
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
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
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
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
    when(catalog.getTableWithLenientColumns("cat.schema.orders"))
        .thenReturn(Optional.of(table("orders", "id", "EXTERNAL", "DELTA", null)));
    when(catalog.generateTemporaryTableCredentials("id", UnityCatalogClient.TableOperation.READ))
        .thenThrow(
            new UnityCatalogException(UnityCatalogException.Failure.SERVER_ERROR, 503, "HTTP 503"));

    assertThatThrownBy(() -> connector.vendStorageCredentials("cat.schema", "orders"))
        .isInstanceOf(UnityCatalogException.class)
        .isNotInstanceOf(SourceCatalogAccessException.class);
  }

  /**
   * A connector is built per vend, so an auth provider that owns an HTTP client leaks a selector
   * thread and an executor on every call unless {@code close()} releases it alongside the catalog
   * transport.
   */
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
