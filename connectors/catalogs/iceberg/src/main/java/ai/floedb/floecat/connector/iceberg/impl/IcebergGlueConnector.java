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
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;

final class IcebergGlueConnector extends IcebergConnector {
  private final GlueIcebergFilter glueFilter;
  private final RESTCatalog catalog;

  IcebergGlueConnector(
      String connectorId,
      RESTCatalog catalog,
      GlueIcebergFilter glueFilter,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles) {
    super(connectorId, null, null, null, ndvEnabled, ndvSampleFraction, ndvMaxFiles, null);
    this.glueFilter = Objects.requireNonNull(glueFilter, "glueFilter");
    this.catalog = catalog;
  }

  @Override
  public List<String> listNamespaces() {
    if (isSingleTableMode()) {
      return listNamespacesSingle();
    }
    return catalog.listNamespaces().stream()
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
  protected Table loadTableFromSource(String namespaceFq, String tableName) {
    Namespace namespace =
        (namespaceFq == null || namespaceFq.isBlank())
            ? Namespace.empty()
            : Namespace.of(namespaceFq.split("\\."));
    TableIdentifier tableId =
        namespace.isEmpty()
            ? TableIdentifier.of(tableName)
            : TableIdentifier.of(namespace, tableName);
    return catalog.loadTable(tableId);
  }

  @Override
  protected void closeCatalog() {
    try {
      catalog.close();
    } catch (Exception ignore) {
    }
  }
}
