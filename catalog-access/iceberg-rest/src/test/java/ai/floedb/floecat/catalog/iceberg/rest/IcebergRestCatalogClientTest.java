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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.ExternalObjectIdentity;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewVersion;
import org.junit.jupiter.api.Test;

class IcebergRestCatalogClientTest {
  private final Catalog catalog = mock(Catalog.class);
  private final SupportsNamespaces namespaces = mock(SupportsNamespaces.class);
  private final ViewCatalog views = mock(ViewCatalog.class);

  @Test
  void listsAndSortsStructuredNamespaces() {
    when(namespaces.listNamespaces(Namespace.of("production")))
        .thenReturn(
            List.of(Namespace.of("production", "sales"), Namespace.of("production", "finance")));
    IcebergRestCatalogClient client = client();

    assertEquals(
        List.of(NamespacePath.of("production", "finance"), NamespacePath.of("production", "sales")),
        client.listNamespaces(NamespacePath.of("production")));
  }

  @Test
  void listsTablesWithTheirFullNamespace() {
    when(catalog.listTables(Namespace.of("production", "sales")))
        .thenReturn(
            List.of(
                TableIdentifier.of(Namespace.of("production", "sales"), "orders"),
                TableIdentifier.of(Namespace.of("production", "sales"), "customers")));
    IcebergRestCatalogClient client = client();

    assertEquals(
        List.of(
            new CatalogObjectName(NamespacePath.of("production", "sales"), "customers"),
            new CatalogObjectName(NamespacePath.of("production", "sales"), "orders")),
        client.listTables(NamespacePath.of("production", "sales")));
  }

  @Test
  void loadsProviderNeutralTableMetadataWithExplicitPathFallbackIdentity() {
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    when(table.schema()).thenReturn(schema);
    when(table.spec()).thenReturn(PartitionSpec.builderFor(schema).identity("id").build());
    when(table.location()).thenReturn("s3://warehouse/sales/orders");
    when(table.properties()).thenReturn(Map.of("owner", "sales-team"));
    when(catalog.loadTable(TableIdentifier.of(Namespace.of("production", "sales"), "orders")))
        .thenReturn(table);
    IcebergRestCatalogClient client = client();

    var loaded = client.loadTable(name);

    assertEquals(name, loaded.name());
    assertEquals(ExternalObjectIdentity.pathFallback(name), loaded.identity());
    assertFalse(loaded.identity().stable());
    assertEquals("ICEBERG", loaded.format());
    assertTrue(loaded.schemaJson().contains("\"name\":\"id\""));
    assertEquals(List.of("id"), loaded.partitionKeys());
    assertEquals("s3://warehouse/sales/orders", loaded.storageLocation().orElseThrow());
    assertEquals(Map.of("owner", "sales-team"), loaded.properties());
  }

  @Test
  void listsAndLoadsViewsWithProviderMetadata() {
    Namespace namespace = Namespace.of("production", "sales");
    TableIdentifier identifier = TableIdentifier.of(namespace, "monthly_sales");
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "monthly_sales");
    View view = mock(View.class);
    ViewVersion version = mock(ViewVersion.class);
    SQLViewRepresentation representation = mock(SQLViewRepresentation.class);
    when(views.listViews(namespace)).thenReturn(List.of(identifier));
    when(views.loadView(identifier)).thenReturn(view);
    when(view.uuid()).thenReturn(UUID.fromString("22398561-563c-4e4a-b90c-2a275264d40c"));
    when(view.schema())
        .thenReturn(new Schema(Types.NestedField.required(1, "total", Types.LongType.get())));
    when(view.currentVersion()).thenReturn(version);
    when(version.defaultNamespace()).thenReturn(Namespace.of("production", "shared"));
    when(version.representations()).thenReturn(List.of(representation));
    when(representation.sql()).thenReturn("select sum(amount) as total from sales");
    when(representation.dialect()).thenReturn("spark");
    when(view.properties()).thenReturn(Map.of("owner", "finance"));
    IcebergRestCatalogClient client = client();

    assertEquals(List.of(name), client.listViews(NamespacePath.of("production", "sales")));
    var loaded = client.loadView(name);

    assertEquals(name, loaded.name());
    assertEquals("22398561-563c-4e4a-b90c-2a275264d40c", loaded.identity().value());
    assertTrue(loaded.identity().stable());
    assertTrue(loaded.outputSchemaJson().contains("total"));
    assertEquals("select sum(amount) as total from sales", loaded.definitions().getFirst().sql());
    assertEquals("spark", loaded.definitions().getFirst().dialect());
    assertEquals(NamespacePath.of("production", "shared"), loaded.defaultNamespace());
    assertEquals(Map.of("owner", "finance"), loaded.properties());
    assertTrue(client.capabilities().supports(CatalogCapability.LIST_VIEWS));
    assertTrue(client.capabilities().supports(CatalogCapability.LOAD_VIEW));
  }

  @Test
  void returnsOnlyDedicatedProtocolVendedCredentialsForTheLongestMatchingPrefix() {
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/",
                    Map.of(
                        "s3.access-key-id", "broad-access",
                        "s3.secret-access-key", "broad-secret")),
                StorageCredential.create(
                    "s3://warehouse/sales/orders/",
                    Map.of(
                        "s3.access-key-id", "table-access",
                        "s3.secret-access-key", "table-secret",
                        "s3.session-token", "table-token",
                        "s3.session-token-expires-at-ms", "1786000000000",
                        "token", "catalog-token-must-not-leak"))));
    when(catalog.loadTable(identifier)).thenReturn(table);
    IcebergRestCatalogClient client = client();

    var vended = client.vendStorageCredentials(name).orElseThrow();

    assertEquals("table-access", vended.properties().get("s3.access-key-id"));
    assertEquals("table-secret", vended.properties().get("s3.secret-access-key"));
    assertFalse(vended.properties().containsKey("token"));
    assertEquals("s3://warehouse/sales/orders/", vended.scopePrefix());
    assertEquals(Instant.ofEpochMilli(1786000000000L), vended.expiresAt().orElseThrow());
    assertFalse(vended.toString().contains("table-secret"));
    assertTrue(client.capabilities().supports(CatalogCapability.VEND_STORAGE_CREDENTIALS));

    client.vendStorageCredentials(name);
    verify(catalog, times(2)).loadTable(identifier);
  }

  @Test
  void perTableRoutingOverridesTheCatalogWideDefaultAndTheCredentialStillWins() {
    // The layering a multi-region catalog depends on. RESTSessionCatalog.tableFileIO builds the
    // table's FileIO from RESTUtil.merge(properties(), response.config()), so a LoadTableResponse
    // config reaches table.io() and never reaches the map captured from /v1/config at construction.
    // Reading only the latter signs requests for the catalog's own region against a table that
    // lives somewhere else.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(io.properties())
        .thenReturn(
            Map.of(
                "s3.endpoint", "https://frankfurt.example.invalid",
                "client.region", "eu-central-1",
                // Not routing, and not key material: it must not ride along into the vend.
                "warehouse", "must-not-leak"));
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/sales/orders/",
                    Map.of(
                        "s3.access-key-id", "table-access",
                        "s3.secret-access-key", "table-secret",
                        "s3.session-token", "table-token",
                        "s3.session-token-expires-at-ms", "1786000000000"))));
    when(catalog.loadTable(identifier)).thenReturn(table);
    IcebergRestCatalogClient client =
        new IcebergRestCatalogClient(
            catalog,
            namespaces,
            views,
            () -> {},
            Map.of("client.region", "us-east-1", "s3.path-style-access", "true"));

    var vended = client.vendStorageCredentials(name).orElseThrow();

    // The table's own region beats the catalog default.
    assertEquals("eu-central-1", vended.properties().get("client.region"));
    assertEquals("https://frankfurt.example.invalid", vended.properties().get("s3.endpoint"));
    // A catalog-wide key the table did not override still applies.
    assertEquals("true", vended.properties().get("s3.path-style-access"));
    // Only allowlisted routing crosses over.
    assertFalse(vended.properties().containsKey("warehouse"));
    // The credential still overlays everything.
    assertEquals("table-access", vended.properties().get("s3.access-key-id"));
  }

  @Test
  void aFileIoThatWillNotExposeItsPropertiesStillVends() {
    // Reading per-table routing is enrichment, and it runs after a usable credential is already
    // selected, so nothing it throws may cost the vend. UnsupportedOperationException is what the
    // FileIO.properties() default method declares; the rest is what a concrete implementation is
    // free to throw for the same "I do not expose this" reason -- and for an Integration a lost
    // vend is a failed read, since there is no storage authority behind it to fall back to.
    for (RuntimeException refusal :
        List.of(
            new UnsupportedOperationException("does not expose config"),
            new IllegalStateException("io is closed"),
            new NullPointerException("properties were never populated"))) {
      CatalogObjectName name =
          new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
      TableIdentifier identifier =
          TableIdentifier.of(Namespace.of("production", "sales"), "orders");
      Catalog scopedCatalog = mock(Catalog.class);
      Table table = mock(Table.class);
      FileIO io =
          mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
      when(table.io()).thenReturn(io);
      when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
      when(io.properties()).thenThrow(refusal);
      when(((SupportsStorageCredentials) io).credentials())
          .thenReturn(
              List.of(
                  StorageCredential.create(
                      "s3://warehouse/sales/orders/",
                      Map.of(
                          "s3.access-key-id", "table-access",
                          "s3.secret-access-key", "table-secret",
                          "s3.session-token", "table-token",
                          "s3.session-token-expires-at-ms", "1786000000000"))));
      when(scopedCatalog.loadTable(identifier)).thenReturn(table);
      IcebergRestCatalogClient client =
          new IcebergRestCatalogClient(
              scopedCatalog, namespaces, views, () -> {}, Map.of("client.region", "us-east-1"));

      var vended = client.vendStorageCredentials(name).orElseThrow();

      assertEquals("table-access", vended.properties().get("s3.access-key-id"), refusal.toString());
      // The catalog-wide default still applies, which is what this path had before.
      assertEquals("us-east-1", vended.properties().get("client.region"), refusal.toString());
    }
  }

  @Test
  void anIncompleteCredentialNamesTheFieldsItActuallyLacks() {
    // A lone session token with neither key. The message is terminal, so it is the last thing an
    // operator reads -- a fixed "one of access-key-id or secret-access-key" would point them at
    // half the truth when in fact neither arrived.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/sales/orders/", Map.of("s3.session-token", "orphan-token"))));
    when(catalog.loadTable(identifier)).thenReturn(table);

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> client().vendStorageCredentials(name));

    assertEquals(CatalogAccessException.Code.INVALID_CONFIGURATION, error.code());
    assertTrue(error.getMessage().contains("s3.access-key-id"), error.getMessage());
    assertTrue(error.getMessage().contains("s3.secret-access-key"), error.getMessage());
  }

  @Test
  void aRegionReportedAsClientRegionSurvivesToTheVendedCredential() {
    // Iceberg's own AWS region property, so a catalog may report its region under this name in the
    // /v1/config response rather than as s3.region. Filtering it out left the consumer to fall back
    // to floecat's configured default and sign for the wrong region.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/sales/orders/",
                    Map.of(
                        "s3.access-key-id", "vended-access",
                        "s3.secret-access-key", "vended-secret",
                        "s3.session-token", "vended-token",
                        "s3.session-token-expires-at-ms", "4102444800000"))));
    when(catalog.loadTable(identifier)).thenReturn(table);
    var client =
        new IcebergRestCatalogClient(
            catalog, namespaces, views, () -> {}, Map.of("client.region", "eu-west-1"));

    var vended = client.vendStorageCredentials(name).orElseThrow();

    assertEquals("eu-west-1", vended.properties().get("client.region"));
  }

  @Test
  void anUnrelatedCredentialDoesNotHijackTheDiagnosisForThisTable() {
    // Both faults in one response: a complete session scoped somewhere else, and a bare pair that
    // does cover this table. Reporting "credentials do not cover the upstream table location" is
    // simply untrue here -- one of them does -- and it sends the operator to catalog scoping when
    // the actual defect is that the Integration is not holding a session.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://other-warehouse/",
                    Map.of(
                        "s3.access-key-id", "elsewhere-access",
                        "s3.secret-access-key", "elsewhere-secret",
                        "s3.session-token", "elsewhere-token",
                        "s3.session-token-expires-at-ms", "4102444800000")),
                StorageCredential.create(
                    "s3://warehouse/sales/orders/",
                    Map.of(
                        "s3.access-key-id", "static-access",
                        "s3.secret-access-key", "static-secret"))));
    when(catalog.loadTable(identifier)).thenReturn(table);

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> client().vendStorageCredentials(name));

    assertEquals(CatalogAccessException.Code.INVALID_CONFIGURATION, error.code());
    assertTrue(error.getMessage().contains("s3.session-token"), error.getMessage());
  }

  @Test
  void anOutOfScopeResponseStillReportsScopeWhenNothingCoversTheTable() {
    // The scope check is not gone, just last: with nothing covering the table, "does not cover the
    // upstream table location" is the accurate answer.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://other-warehouse/",
                    Map.of(
                        "s3.access-key-id", "elsewhere-access",
                        "s3.secret-access-key", "elsewhere-secret",
                        "s3.session-token", "elsewhere-token",
                        "s3.session-token-expires-at-ms", "4102444800000"))));
    when(catalog.loadTable(identifier)).thenReturn(table);

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> client().vendStorageCredentials(name));

    assertEquals(CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID, error.code());
  }

  @Test
  void aBroadLiveSessionBeatsANarrowerExpiredOne() {
    // The same rotation that leaves a narrow unrenewable pair can leave a narrow expired session.
    // Ranking on specificity alone picked it, and the consumer then rejected the vend -- failing a
    // read this response could have served from the broader credential.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/",
                    Map.of(
                        "s3.access-key-id", "live-access",
                        "s3.secret-access-key", "live-secret",
                        "s3.session-token", "live-token",
                        "s3.session-token-expires-at-ms", "4102444800000")),
                StorageCredential.create(
                    "s3://warehouse/sales/orders/",
                    Map.of(
                        "s3.access-key-id", "expired-access",
                        "s3.secret-access-key", "expired-secret",
                        "s3.session-token", "expired-token",
                        "s3.session-token-expires-at-ms", "1580000000000"))));
    when(catalog.loadTable(identifier)).thenReturn(table);
    var client = new IcebergRestCatalogClient(catalog, namespaces, views, () -> {}, Map.of());
    client.clock = java.time.Clock.fixed(Instant.parse("2026-09-04T00:00:00Z"), ZoneOffset.UTC);

    var vended = client.vendStorageCredentials(name).orElseThrow();

    assertEquals("live-access", vended.properties().get("s3.access-key-id"));
    assertEquals("s3://warehouse/", vended.scopePrefix());
  }

  @Test
  void anExpiredSessionIsStillHandedOnWhenNothingLiveCoversTheTable() {
    // Liveness ranks, it does not exclude. The consumer owns the expiry policy -- its skew
    // tolerance, and whether a query may read a just-expired credential -- so refusing here would
    // take that decision away and report "no credentials" for a response that had one.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/sales/orders/",
                    Map.of(
                        "s3.access-key-id", "expired-access",
                        "s3.secret-access-key", "expired-secret",
                        "s3.session-token", "expired-token",
                        "s3.session-token-expires-at-ms", "1580000000000"))));
    when(catalog.loadTable(identifier)).thenReturn(table);
    var client = new IcebergRestCatalogClient(catalog, namespaces, views, () -> {}, Map.of());
    client.clock = java.time.Clock.fixed(Instant.parse("2026-09-04T00:00:00Z"), ZoneOffset.UTC);

    var vended = client.vendStorageCredentials(name).orElseThrow();

    assertEquals("expired-access", vended.properties().get("s3.access-key-id"));
  }

  @Test
  void aBroadRenewableSessionBeatsANarrowerUnrenewablePair() {
    // The reverse of the longest-prefix case. Iceberg returns a list so a catalog can scope per
    // prefix, and a rotation or a reconfiguration can leave a narrow unrenewable pair beside a
    // broad complete session. Ranking on specificity alone picked the narrow one and then refused
    // the whole response, with a usable credential sitting in it.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/",
                    Map.of(
                        "s3.access-key-id", "session-access",
                        "s3.secret-access-key", "session-secret",
                        "s3.session-token", "session-token",
                        "s3.session-token-expires-at-ms", "4102444800000")),
                StorageCredential.create(
                    "s3://warehouse/sales/orders/",
                    Map.of(
                        "s3.access-key-id", "static-access",
                        "s3.secret-access-key", "static-secret"))));
    when(catalog.loadTable(identifier)).thenReturn(table);

    var vended = client().vendStorageCredentials(name).orElseThrow();

    assertEquals("session-access", vended.properties().get("s3.access-key-id"));
    assertEquals("s3://warehouse/", vended.scopePrefix());
  }

  @Test
  void aBareKeyPairIsRefusedRatherThanVendedAsALongLivedCredential() {
    // The shape that made validation and the vend disagree. A bare pair reads storage perfectly
    // well, so every validation check passed -- including the storage-access probe -- and then the
    // first capture or query of every table an overlay had materialized failed terminally. Refusing
    // it here means both paths reach the same answer, because both come through this method.
    //
    // There is no scoping-down step that could rescue it: floecat does not mint a narrower
    // credential with STS, so a pair that arrives is a pair that would travel to a reader.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/sales/orders/",
                    Map.of(
                        "s3.access-key-id", "static-access",
                        "s3.secret-access-key", "static-secret"))));
    when(catalog.loadTable(identifier)).thenReturn(table);

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> client().vendStorageCredentials(name));

    assertEquals(CatalogAccessException.Code.INVALID_CONFIGURATION, error.code());
    assertTrue(error.getMessage().contains("s3.session-token"));
  }

  @Test
  void aSessionWithNoExpiryIsRefusedForTheSameReason() {
    // Half the requirement is not the requirement: an expiry is what makes the session renewable,
    // and without one the reconcile path embeds the credential statically and never re-vends.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/sales/orders/",
                    Map.of(
                        "s3.access-key-id", "session-access",
                        "s3.secret-access-key", "session-secret",
                        "s3.session-token", "session-token"))));
    when(catalog.loadTable(identifier)).thenReturn(table);

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> client().vendStorageCredentials(name));

    assertEquals(CatalogAccessException.Code.INVALID_CONFIGURATION, error.code());
    assertTrue(error.getMessage().contains("s3.session-token-expires-at-ms"));
  }

  @Test
  void aNonPositiveExpiryIsAbsentRatherThanNineteenSeventy() {
    // Both bounds the connector-side parser applies, because a comment here claims parity with it
    // and nothing else pins the two together. Non-positive would otherwise hand back an instant
    // every consumer reads as already expired; out of range is a unit mismatch -- microseconds in
    // a field documented as milliseconds lands in year 62178 -- from the same field of the same
    // response.
    //
    // Observed through the refusal rather than through an empty expiry: a value that does not parse
    // leaves the credential with no renewal point, which is one of the two things this provider now
    // declines to vend. The parse is still what is being pinned -- a value inside the bounds would
    // not reach this branch.
    // "1790000000" is the case that matters most: a plausible expiry reported in seconds, which
    // as milliseconds lands in January 1970. Positive and inside the ceiling, so only the floor
    // catches it -- and uncaught it is deterministically stale, which the refresh path re-vends on
    // every resolveCredentials instead of failing once with the field named.
    for (String raw :
        new String[] {
          "0", "-1", "1790000000", "946684799999", "1900000000000000", "253402300800000"
        }) {
      CatalogObjectName name =
          new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
      TableIdentifier identifier =
          TableIdentifier.of(Namespace.of("production", "sales"), "orders");
      Table table = mock(Table.class);
      FileIO io =
          mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
      when(table.io()).thenReturn(io);
      when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
      when(((SupportsStorageCredentials) io).credentials())
          .thenReturn(
              List.of(
                  StorageCredential.create(
                      "s3://warehouse/sales/orders/",
                      Map.of(
                          "s3.access-key-id", "table-access",
                          "s3.secret-access-key", "table-secret",
                          "s3.session-token", "table-token",
                          "s3.session-token-expires-at-ms", raw))));
      when(catalog.loadTable(identifier)).thenReturn(table);

      CatalogAccessException error =
          assertThrows(
              CatalogAccessException.class, () -> client().vendStorageCredentials(name), raw);

      assertEquals(CatalogAccessException.Code.INVALID_CONFIGURATION, error.code(), raw);
      assertTrue(error.getMessage().contains("s3.session-token-expires-at-ms"), raw);
    }
  }

  @Test
  void anOutOfScopeCredentialIsReportedAsScopeInvalidEvenBesideStrayKeyMaterial() {
    // The two classifications are not interchangeable: scope-invalid is a fall-back the caller
    // handles, while an incomplete tuple is terminal. A half tuple offered for some unrelated
    // prefix must not decide the answer for this table, or a response the code deliberately falls
    // back on would permanently fail the job.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                // A complete pair, but for another bucket.
                StorageCredential.create(
                    "s3://other-bucket/",
                    Map.of(
                        "s3.access-key-id", "vended-access",
                        "s3.secret-access-key", "vended-secret")),
                // Stray half tuple, also for another bucket.
                StorageCredential.create(
                    "s3://third-bucket/", Map.of("s3.access-key-id", "half"))));
    when(catalog.loadTable(identifier)).thenReturn(table);
    var client = new IcebergRestCatalogClient(catalog, namespaces, views, () -> {}, Map.of());

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> client.vendStorageCredentials(name));

    assertEquals(CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID, error.code());
  }

  @Test
  void anUnscopedPartialCredentialIsAFaultToo() {
    // A catalog that vends one credential with no prefix at all and half a tuple in it. An absent
    // prefix covers every location by contract, so this is the clearest case of the fault the
    // partial check names -- and the case a null-prefix guard on that filter silently dropped back
    // onto the missing-authority path.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    StorageCredential unscoped = mock(StorageCredential.class);
    when(unscoped.prefix()).thenReturn(null);
    when(unscoped.config()).thenReturn(Map.of("s3.access-key-id", "vended-access"));
    when(((SupportsStorageCredentials) io).credentials()).thenReturn(List.of(unscoped));
    when(catalog.loadTable(identifier)).thenReturn(table);
    var client = new IcebergRestCatalogClient(catalog, namespaces, views, () -> {}, Map.of());

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> client.vendStorageCredentials(name));

    assertEquals(CatalogAccessException.Code.INVALID_CONFIGURATION, error.code());
  }

  @Test
  void aPartialVendedCredentialIsAFaultRatherThanNoDelegation() {
    // Half a tuple is not "this catalog does not vend for this table". Returning empty would hand
    // the caller its no-delegation path, and since an integration has no storage-authority
    // fall-back to opt into, that surfaces as a missing-authority error -- which reads as a
    // configuration gap and hides the real cause. A retry cannot change it either, so it is
    // reported as an invalid configuration.
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/", Map.of("s3.access-key-id", "vended-access"))));
    when(catalog.loadTable(identifier)).thenReturn(table);
    var client = new IcebergRestCatalogClient(catalog, namespaces, views, () -> {}, Map.of());

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> client.vendStorageCredentials(name));

    assertEquals(CatalogAccessException.Code.INVALID_CONFIGURATION, error.code());
    assertTrue(error.getMessage().contains("incomplete"), error.getMessage());
  }

  @Test
  void carriesConfiguredStorageRoutingWithProtocolVendedCredentials() {
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/",
                    Map.of(
                        "s3.access-key-id", "vended-access",
                        "s3.secret-access-key", "vended-secret",
                        "s3.session-token", "vended-token",
                        "s3.session-token-expires-at-ms", "4102444800000"))));
    when(catalog.loadTable(identifier)).thenReturn(table);
    var client =
        new IcebergRestCatalogClient(
            catalog,
            namespaces,
            views,
            () -> {},
            Map.of(
                "s3.endpoint", "http://localstack:4566",
                "s3.region", "us-east-1",
                "s3.path-style-access", "true"));

    var properties = client.vendStorageCredentials(name).orElseThrow().properties();

    assertEquals("http://localstack:4566", properties.get("s3.endpoint"));
    assertEquals("us-east-1", properties.get("s3.region"));
    assertEquals("true", properties.get("s3.path-style-access"));
    assertEquals("vended-access", properties.get("s3.access-key-id"));
  }

  @Test
  void ignoresLongerRoutingOnlyCredentialCandidates() {
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/",
                    Map.of(
                        "s3.access-key-id", "usable-access",
                        "s3.secret-access-key", "usable-secret",
                        "s3.session-token", "usable-token",
                        "s3.session-token-expires-at-ms", "4102444800000")),
                StorageCredential.create(
                    "s3://warehouse/sales/orders/", Map.of("s3.region", "us-west-2"))));
    when(catalog.loadTable(identifier)).thenReturn(table);

    var vended = client().vendStorageCredentials(name).orElseThrow();

    assertEquals("usable-access", vended.properties().get("s3.access-key-id"));
    assertEquals("s3://warehouse/", vended.scopePrefix());
  }

  @Test
  void matchesS3aLocationsToS3CredentialPrefixes() {
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3a://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://warehouse/sales/orders/",
                    Map.of(
                        "s3.access-key-id", "vended-access",
                        "s3.secret-access-key", "vended-secret",
                        "s3.session-token", "vended-token",
                        "s3.session-token-expires-at-ms", "4102444800000"))));
    when(catalog.loadTable(identifier)).thenReturn(table);

    var vended = client().vendStorageCredentials(name).orElseThrow();

    assertEquals("vended-access", vended.properties().get("s3.access-key-id"));
    assertEquals("s3://warehouse/sales/orders/", vended.scopePrefix());
  }

  @Test
  void routingOnlyCredentialOutsideTableScopeIsNotReportedAsInvalidCredentials() {
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3a://warehouse/sales/orders/data");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(StorageCredential.create("s3://different/", Map.of("s3.region", "us-east-1"))));
    when(catalog.loadTable(TableIdentifier.of(Namespace.of("production", "sales"), "orders")))
        .thenReturn(table);

    assertTrue(client().vendStorageCredentials(name).isEmpty());
  }

  @Test
  void doesNotTreatOrdinaryFileIoPropertiesAsProtocolVendedCredentials() {
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io = mock(FileIO.class);
    when(table.io()).thenReturn(io);
    when(io.properties())
        .thenReturn(
            Map.of(
                "s3.access-key-id", "configured-access",
                "s3.secret-access-key", "configured-secret"));
    when(catalog.loadTable(TableIdentifier.of(Namespace.of("production", "sales"), "orders")))
        .thenReturn(table);

    assertTrue(client().vendStorageCredentials(name).isEmpty());
  }

  @Test
  void prefersStableIcebergTableUuidWhenMetadataExposesIt() {
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    Table table = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
    TableOperations operations = mock(TableOperations.class);
    TableMetadata metadata = mock(TableMetadata.class);
    when(((HasTableOperations) table).operations()).thenReturn(operations);
    when(operations.current()).thenReturn(metadata);
    when(metadata.uuid()).thenReturn("6d4fe3f7-4615-4bc5-b9ae-62c691d6ba7e");
    when(metadata.metadataFileLocation()).thenReturn("s3://warehouse/metadata/v2.json");
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.LongType.get()));
    when(table.schema()).thenReturn(schema);
    when(table.spec()).thenReturn(PartitionSpec.unpartitioned());
    when(table.location()).thenReturn("s3://warehouse/sales/orders");
    when(table.properties()).thenReturn(Map.of());
    when(catalog.loadTable(TableIdentifier.of(Namespace.of("production", "sales"), "orders")))
        .thenReturn(table);
    IcebergRestCatalogClient client = client();

    var loaded = client.loadTable(name);

    assertEquals("6d4fe3f7-4615-4bc5-b9ae-62c691d6ba7e", loaded.identity().value());
    assertTrue(loaded.identity().stable());
    assertEquals("s3://warehouse/metadata/v2.json", loaded.metadataLocation().orElseThrow());
    assertTrue(client.capabilities().supports(CatalogCapability.STABLE_OBJECT_IDS));
  }

  @Test
  void validatesWithARealCatalogOperation() {
    IcebergRestCatalogClient client = client();

    client.validate();

    verify(namespaces, times(1)).listNamespaces(Namespace.empty());
  }

  @Test
  void validatesStorageWithANonMutatingMetadataFileRead() {
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
    TableOperations operations = mock(TableOperations.class);
    TableMetadata metadata = mock(TableMetadata.class);
    FileIO io = mock(FileIO.class);
    InputFile input = mock(InputFile.class);
    when(((HasTableOperations) table).operations()).thenReturn(operations);
    when(operations.current()).thenReturn(metadata);
    when(metadata.metadataFileLocation()).thenReturn("s3://warehouse/metadata/v2.json");
    when(io.newInputFile("s3://warehouse/metadata/v2.json")).thenReturn(input);
    when(input.getLength()).thenReturn(42L);
    when(catalog.loadTable(identifier)).thenReturn(table);

    var vended =
        new VendedStorageCredentials(
            Map.of(
                "s3.access-key-id", "vended-access",
                "s3.secret-access-key", "vended-secret"),
            "s3://warehouse/",
            Optional.empty());
    var client = new IcebergRestCatalogClient(catalog, namespaces, views, () -> {}, ignored -> io);

    client.validateStorageAccess(name, vended);

    verify(input).getLength();
    verify(io).close();
    assertTrue(client.capabilities().supports(CatalogCapability.VALIDATE_STORAGE_ACCESS));
  }

  @Test
  void validatesS3aMetadataWithS3CredentialScope() {
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("production", "sales"), "orders");
    Table table = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
    TableOperations operations = mock(TableOperations.class);
    TableMetadata metadata = mock(TableMetadata.class);
    FileIO io = mock(FileIO.class);
    InputFile input = mock(InputFile.class);
    when(((HasTableOperations) table).operations()).thenReturn(operations);
    when(operations.current()).thenReturn(metadata);
    when(metadata.metadataFileLocation()).thenReturn("s3a://warehouse/metadata/v2.json");
    when(io.newInputFile("s3a://warehouse/metadata/v2.json")).thenReturn(input);
    when(input.getLength()).thenReturn(42L);
    when(catalog.loadTable(identifier)).thenReturn(table);
    var vended =
        new VendedStorageCredentials(
            Map.of(
                "s3.access-key-id", "vended-access",
                "s3.secret-access-key", "vended-secret"),
            "s3://warehouse/",
            Optional.empty());
    var client = new IcebergRestCatalogClient(catalog, namespaces, views, () -> {}, ignored -> io);

    client.validateStorageAccess(name, vended);

    verify(input).getLength();
    verify(io).close();
  }

  @Test
  void rejectsVendedCredentialsOutsideTheMetadataLocationScope() {
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    Table table = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
    TableOperations operations = mock(TableOperations.class);
    TableMetadata metadata = mock(TableMetadata.class);
    when(((HasTableOperations) table).operations()).thenReturn(operations);
    when(operations.current()).thenReturn(metadata);
    when(metadata.metadataFileLocation()).thenReturn("s3://warehouse/metadata/v2.json");
    when(catalog.loadTable(TableIdentifier.of(Namespace.of("production", "sales"), "orders")))
        .thenReturn(table);
    var vended =
        new VendedStorageCredentials(
            Map.of(
                "s3.access-key-id", "vended-access",
                "s3.secret-access-key", "vended-secret"),
            "s3://other-bucket/",
            Optional.empty());
    var client =
        new IcebergRestCatalogClient(
            catalog, namespaces, views, () -> {}, ignored -> mock(FileIO.class));

    CatalogAccessException error =
        assertThrows(
            CatalogAccessException.class, () -> client.validateStorageAccess(name, vended));

    assertEquals(CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID, error.code());
  }

  @Test
  void reportsProtocolCredentialsThatDoNotCoverTheTableLocation() {
    CatalogObjectName name =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    Table table = mock(Table.class);
    FileIO io =
        mock(FileIO.class, withSettings().extraInterfaces(SupportsStorageCredentials.class));
    when(table.io()).thenReturn(io);
    when(table.location()).thenReturn("s3://warehouse/sales/orders");
    when(((SupportsStorageCredentials) io).credentials())
        .thenReturn(
            List.of(
                StorageCredential.create(
                    "s3://different/",
                    Map.of(
                        "s3.access-key-id", "vended-access",
                        "s3.secret-access-key", "vended-secret"))));
    when(catalog.loadTable(TableIdentifier.of(Namespace.of("production", "sales"), "orders")))
        .thenReturn(table);

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> client().vendStorageCredentials(name));

    assertEquals(CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID, error.code());
  }

  @Test
  void translatesProviderAuthenticationFailuresWithoutLeakingTheirMessage() {
    when(namespaces.listNamespaces(Namespace.empty()))
        .thenThrow(new org.apache.iceberg.exceptions.NotAuthorizedException("secret-token"));

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> client().validate());

    assertEquals(CatalogAccessException.Code.UNAUTHENTICATED, error.code());
    assertFalse(error.getMessage().contains("secret-token"));
  }

  @Test
  void translatesInvalidRootNamespaceForDiscoveryFallback() {
    when(catalog.listTables(Namespace.empty()))
        .thenThrow(new NoSuchNamespaceException("Invalid namespace: secret-detail"));

    CatalogAccessException error =
        assertThrows(CatalogAccessException.class, () -> client().listTables(NamespacePath.root()));

    assertEquals(CatalogAccessException.Code.NOT_FOUND, error.code());
    assertFalse(error.getMessage().contains("secret-detail"));
  }

  @Test
  void translatingFailureTerminatesForIndirectCauseCycles() {
    RuntimeException first = new RuntimeException("first");
    RuntimeException second = new RuntimeException("second");
    first.initCause(second);
    second.initCause(first);

    assertTimeoutPreemptively(
        Duration.ofSeconds(1),
        () -> assertSame(first, IcebergRestCatalogErrors.translate("test", first)));
  }

  @Test
  void closesProviderSessionOnlyOnce() {
    AtomicInteger closes = new AtomicInteger();
    IcebergRestCatalogClient client =
        new IcebergRestCatalogClient(catalog, namespaces, views, closes::incrementAndGet);

    client.close();
    client.close();

    assertEquals(1, closes.get());
  }

  private IcebergRestCatalogClient client() {
    return new IcebergRestCatalogClient(catalog, namespaces, views, () -> {});
  }
}
