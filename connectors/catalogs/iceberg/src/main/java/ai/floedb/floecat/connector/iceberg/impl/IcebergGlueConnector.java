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
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;

final class IcebergGlueConnector extends IcebergConnector {
  private final GlueIcebergFilter glueFilter;
  private final Catalog catalog;
  private final SupportsNamespaces namespaceCatalog;
  private final ViewCatalog viewCatalog;
  private final Catalog tableCatalog;
  private final Runnable closeHook;

  IcebergGlueConnector(
      String connectorId,
      Catalog catalog,
      SupportsNamespaces namespaceCatalog,
      ViewCatalog viewCatalog,
      Catalog tableCatalog,
      GlueIcebergFilter glueFilter,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles,
      Runnable closeHook) {
    super(connectorId, null, null, null, ndvEnabled, ndvSampleFraction, ndvMaxFiles, null);
    this.glueFilter = Objects.requireNonNull(glueFilter, "glueFilter");
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.namespaceCatalog = Objects.requireNonNull(namespaceCatalog, "namespaceCatalog");
    this.viewCatalog = Objects.requireNonNull(viewCatalog, "viewCatalog");
    this.tableCatalog = Objects.requireNonNull(tableCatalog, "tableCatalog");
    this.closeHook = closeHook;
  }

  @Override
  public List<String> listNamespaces() {
    if (isSingleTableMode()) {
      return listNamespacesSingle();
    }
    return namespaceCatalog.listNamespaces().stream()
        .map(Namespace::toString)
        .filter(glueFilter::databaseHasIceberg)
        .sorted()
        .toList();
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    if (isSingleTableMode()) {
      return listTablesSingle(namespaceFq);
    }
    return glueFilter.icebergTables(namespaceFq);
  }

  @Override
  public List<String> listViews(String namespaceFq) {
    return listViewsFromCatalog(viewCatalog, namespaceFq);
  }

  @Override
  public List<ViewDescriptor> listViewDescriptors(String namespaceFq) {
    return listViewDescriptorsFromCatalog(viewCatalog, namespaceFq);
  }

  @Override
  public Optional<ViewDescriptor> describeView(String namespaceFq, String viewName) {
    return describeViewFromCatalog(viewCatalog, namespaceFq, viewName);
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
    if (closeHook != null) {
      try {
        closeHook.run();
      } catch (RuntimeException ignore) {
      }
    }
  }
}
