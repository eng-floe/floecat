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

package ai.floedb.floecat.connector.iceberg.impl;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;

final class IcebergRestConnector extends IcebergConnector {
  private final RESTCatalog catalog;
  private final Catalog tableCatalog;
  private final boolean closeCatalogOnClose;
  private final Runnable closeHook;

  IcebergRestConnector(
      String connectorId,
      RESTCatalog catalog,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles) {
    this(connectorId, catalog, catalog, ndvEnabled, ndvSampleFraction, ndvMaxFiles, true, null);
  }

  IcebergRestConnector(
      String connectorId,
      RESTCatalog catalog,
      Catalog tableCatalog,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles) {
    this(
        connectorId, catalog, tableCatalog, ndvEnabled, ndvSampleFraction, ndvMaxFiles, true, null);
  }

  IcebergRestConnector(
      String connectorId,
      RESTCatalog catalog,
      Catalog tableCatalog,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles,
      boolean closeCatalogOnClose) {
    this(
        connectorId,
        catalog,
        tableCatalog,
        ndvEnabled,
        ndvSampleFraction,
        ndvMaxFiles,
        closeCatalogOnClose,
        null);
  }

  IcebergRestConnector(
      String connectorId,
      RESTCatalog catalog,
      Catalog tableCatalog,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles,
      boolean closeCatalogOnClose,
      Runnable closeHook) {
    super(connectorId, null, null, null, ndvEnabled, ndvSampleFraction, ndvMaxFiles, null);
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.tableCatalog = Objects.requireNonNull(tableCatalog, "tableCatalog");
    this.closeCatalogOnClose = closeCatalogOnClose;
    this.closeHook = closeHook;
  }

  @Override
  public List<String> listNamespaces() {
    if (isSingleTableMode()) {
      return listNamespacesSingle();
    }
    return catalog.listNamespaces().stream().map(Namespace::toString).sorted().toList();
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    if (isSingleTableMode()) {
      return listTablesSingle(namespaceFq);
    }
    Namespace ns =
        (namespaceFq == null || namespaceFq.isBlank())
            ? Namespace.empty()
            : Namespace.of(namespaceFq.split("\\."));
    return catalog.listTables(ns).stream().map(TableIdentifier::name).sorted().toList();
  }

  @Override
  public List<String> listViews(String namespaceFq) {
    return listViewsFromCatalog(catalog, namespaceFq);
  }

  @Override
  public List<ViewDescriptor> listViewDescriptors(String namespaceFq) {
    return listViewDescriptorsFromCatalog(catalog, namespaceFq);
  }

  @Override
  public Optional<ViewDescriptor> describeView(String namespaceFq, String viewName) {
    return describeViewFromCatalog(catalog, namespaceFq, viewName);
  }

  @Override
  protected Table loadTableFromSource(String namespaceFq, String tableName) {
    Namespace namespace =
        (namespaceFq == null || namespaceFq.isBlank())
            ? Namespace.empty()
            : Namespace.of(namespaceFq.split("\\."));
    TableIdentifier tableId =
        namespace.isEmpty()
            ? TableIdentifier.of(tableName)
            : TableIdentifier.of(namespace, tableName);
    return tableCatalog.loadTable(tableId);
  }

  @Override
  protected void closeCatalog() {
    try {
      if (closeCatalogOnClose) {
        catalog.close();
      }
    } catch (Exception ignore) {
    } finally {
      if (closeHook != null) {
        try {
          closeHook.run();
        } catch (RuntimeException ignore) {
        }
      }
    }
  }
}
