/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.catalog.iceberg.rest;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.NamespacePath;
import java.util.List;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.NoSuchTableException;

/** Test-only construction of the package-private Iceberg REST catalog client. */
public final class IcebergRestCatalogClientTestFactory {
  private IcebergRestCatalogClientTestFactory() {}

  public static CatalogClient catalogWithNonIcebergTable(
      NamespacePath namespace, CatalogObjectName table) {
    Catalog catalog = mock(Catalog.class);
    SupportsNamespaces namespaces = mock(SupportsNamespaces.class);
    ViewCatalog views = mock(ViewCatalog.class);
    Namespace icebergNamespace = Namespace.of(namespace.segments().toArray(String[]::new));
    TableIdentifier identifier = TableIdentifier.of(icebergNamespace, table.name());

    when(namespaces.listNamespaces(Namespace.empty())).thenReturn(List.of(icebergNamespace));
    when(namespaces.listNamespaces(icebergNamespace)).thenReturn(List.of());
    when(catalog.listTables(icebergNamespace)).thenReturn(List.of(identifier));
    when(catalog.loadTable(identifier))
        .thenThrow(new NoSuchTableException("Input table is not an iceberg table"));
    when(views.listViews(icebergNamespace)).thenReturn(List.of());

    return new IcebergRestCatalogClient(catalog, namespaces, views, () -> {});
  }
}
