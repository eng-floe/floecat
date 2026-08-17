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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.NamespacePath;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.io.FileIO;
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
    when(table.location()).thenReturn("s3://warehouse/sales/orders");
    when(table.properties()).thenReturn(Map.of("owner", "sales-team"));
    when(catalog.loadTable(TableIdentifier.of(Namespace.of("production", "sales"), "orders")))
        .thenReturn(table);
    IcebergRestCatalogClient client = client();

    var loaded = client.loadTable(name);

    assertEquals(name, loaded.name());
    assertEquals("production.sales.orders", loaded.identity().value());
    assertFalse(loaded.identity().stable());
    assertEquals("ICEBERG", loaded.format());
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
