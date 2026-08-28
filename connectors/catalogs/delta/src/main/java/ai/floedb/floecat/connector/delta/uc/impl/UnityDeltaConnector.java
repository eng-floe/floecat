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

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.client.unity.UnityCatalogClient;
import ai.floedb.floecat.client.unity.UnityCatalogTable;
import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.engine.Engine;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.parquet.io.InputFile;
import org.jboss.logging.Logger;

public final class UnityDeltaConnector extends DeltaConnector {
  private static final Logger LOG = Logger.getLogger(UnityDeltaConnector.class);

  private final UnityCatalogClient catalog;

  /**
   * The auth provider backing {@link #catalog}'s request headers, closed with the connector when it
   * owns resources. Null when the caller keeps ownership.
   */
  private final AuthProvider auth;

  UnityDeltaConnector(
      String connectorId,
      UnityCatalogClient catalog,
      AuthProvider auth,
      Engine engine,
      Function<String, InputFile> parquetInput,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles) {
    super(connectorId, engine, parquetInput, ndvEnabled, ndvSampleFraction, ndvMaxFiles);
    this.catalog = catalog;
    this.auth = auth;
  }

  @Override
  public List<String> listNamespaces() {
    List<String> namespaces = new ArrayList<>();
    for (String catalogName : catalog.listCatalogs()) {
      for (String schemaName : catalog.listSchemas(catalogName)) {
        namespaces.add(catalogName + "." + schemaName);
      }
    }
    namespaces.sort(String::compareTo);
    return namespaces;
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    Namespace namespace = parseNamespace(namespaceFq);
    if (namespace == null) {
      return List.of();
    }
    return catalog.listTables(namespace.catalog(), namespace.schema()).stream()
        .filter(table -> "DELTA".equalsIgnoreCase(table.dataSourceFormat()))
        .map(UnityCatalogTable::name)
        .sorted()
        .toList();
  }

  @Override
  public TableDescriptor describe(String namespaceFq, String tableName) {
    String fullName = namespaceFq + "." + tableName;
    UnityCatalogTable table = requiredTable(fullName);
    Map<String, String> descriptorProperties = new LinkedHashMap<>();
    putIfPresent(descriptorProperties, "table_type", table.tableType());
    putIfPresent(descriptorProperties, "data_source_format", table.dataSourceFormat());
    putIfPresent(descriptorProperties, "storage_location", table.storageLocation());

    String schemaJson = buildSchemaJson(table);
    if (table.storageLocation() != null) {
      try {
        schemaJson = describeTableSchemaJson(table.storageLocation());
      } catch (Exception ignored) {
        // Fall back to UC column metadata when Delta snapshot metadata is unavailable.
      }
    }
    return new TableDescriptor(
        namespaceFq,
        tableName,
        table.storageLocation(),
        schemaJson,
        List.of(),
        ColumnIdAlgorithm.CID_PATH_ORDINAL,
        descriptorProperties);
  }

  @Override
  protected String storageLocation(String namespaceFq, String tableName) {
    String fullName = namespaceFq + "." + tableName;
    String location = requiredTable(fullName).storageLocation();
    if (location == null || location.isBlank()) {
      throw new IllegalStateException("Table has no storage_location: " + fullName);
    }
    return location;
  }

  @Override
  protected Map<String, String> fallbackTablePropertiesForConstraints(
      String namespaceFq, String tableName) {
    try {
      return catalog
          .getTable(namespaceFq + "." + tableName)
          .map(UnityCatalogTable::properties)
          .orElseGet(Map::of);
    } catch (RuntimeException ignored) {
      return Map.of();
    }
  }

  @Override
  public List<String> listViews(String namespaceFq) {
    return listNamespaceTables(namespaceFq).stream()
        .filter(table -> "VIEW".equalsIgnoreCase(table.tableType()))
        .map(UnityCatalogTable::name)
        .sorted()
        .toList();
  }

  @Override
  public List<FloecatConnector.ViewDescriptor> listViewDescriptors(String namespaceFq) {
    List<String> searchPath = searchPath(namespaceFq);
    return listNamespaceTables(namespaceFq).stream()
        .filter(table -> "VIEW".equalsIgnoreCase(table.tableType()))
        .map(
            table ->
                new FloecatConnector.ViewDescriptor(
                    namespaceFq,
                    table.name(),
                    nullToEmpty(table.viewDefinition()),
                    "spark",
                    searchPath,
                    buildSchemaJson(table)))
        .sorted((left, right) -> left.name().compareTo(right.name()))
        .toList();
  }

  @Override
  public Optional<FloecatConnector.ViewDescriptor> describeView(
      String namespaceFq, String viewName) {
    return catalog
        .getTable(namespaceFq + "." + viewName)
        .map(
            table ->
                new FloecatConnector.ViewDescriptor(
                    namespaceFq,
                    viewName,
                    nullToEmpty(table.viewDefinition()),
                    "spark",
                    searchPath(namespaceFq),
                    buildSchemaJson(table)));
  }

  /**
   * Releases the catalog client's transport and the auth provider's.
   *
   * <p>A connector is built per vend -- once per scan session and once per file group -- so the
   * HTTP client behind {@code catalog} would otherwise leak a selector thread and an executor pool
   * on every call. The auth provider leaks the same way: with {@code oauth.mode=cli} it is an
   * {@code OAuth2BearerAuthProvider} wrapping a token provider that owns a second {@link
   * java.net.http.HttpClient}.
   *
   * <p>{@code super.close()} is called for form, not for effect: {@code DeltaConnector.close()} is
   * a no-op, so nothing here releases the Delta engine or the {@code RefreshingAwsClient} behind
   * it. That is currently harmless -- the S3 client is built lazily, and a connector used only for
   * credential vending never triggers it -- but it is not a release, and this javadoc does not
   * claim one.
   */
  @Override
  public void close() {
    try {
      catalog.close();
    } catch (RuntimeException e) {
      LOG.debugf(e, "Failed to close the Unity Catalog client for connector %s", id());
    } finally {
      try {
        if (auth instanceof AutoCloseable closeable) {
          closeable.close();
        }
      } catch (Exception e) {
        LOG.debugf(e, "Failed to close the auth provider for connector %s", id());
      } finally {
        super.close();
      }
    }
  }

  private List<UnityCatalogTable> listNamespaceTables(String namespaceFq) {
    Namespace namespace = parseNamespace(namespaceFq);
    return namespace == null
        ? List.of()
        : catalog.listTables(namespace.catalog(), namespace.schema());
  }

  private UnityCatalogTable requiredTable(String fullName) {
    return catalog
        .getTable(fullName)
        .orElseThrow(() -> new IllegalStateException("Unity Catalog table not found: " + fullName));
  }

  private static Namespace parseNamespace(String namespaceFq) {
    int separator = namespaceFq.indexOf('.');
    if (separator < 0) {
      return null;
    }
    return new Namespace(namespaceFq.substring(0, separator), namespaceFq.substring(separator + 1));
  }

  private static List<String> searchPath(String namespaceFq) {
    Namespace namespace = parseNamespace(namespaceFq);
    return namespace == null ? List.of() : List.of(namespace.schema().split("\\."));
  }

  private static String buildSchemaJson(UnityCatalogTable table) {
    var fields = M.createArrayNode();
    for (UnityCatalogTable.Column column : table.columns()) {
      var field = M.createObjectNode();
      field.put("name", column.name());
      JsonNode type = typeFromTypeJson(column.typeJson());
      if (type == null) {
        // Both spellings can be absent; an empty string keeps the schema JSON well-formed for
        // DeltaSchemaMapper, which a literal null field value would not be.
        String declared = column.typeText() != null ? column.typeText() : column.typeName();
        field.put("type", declared == null ? "" : declared);
      } else {
        field.set("type", type);
      }
      field.put("nullable", column.nullable());
      fields.add(field);
    }
    var schema = M.createObjectNode();
    schema.put("type", "struct");
    schema.set("fields", fields);
    return schema.toString();
  }

  private static JsonNode typeFromTypeJson(String typeJson) {
    if (typeJson == null || typeJson.isBlank()) {
      return null;
    }
    try {
      JsonNode type = M.readTree(typeJson).get("type");
      return type == null || type.isNull() ? null : type;
    } catch (Exception ignored) {
      return null;
    }
  }

  private static void putIfPresent(Map<String, String> values, String key, String value) {
    if (value != null) {
      values.put(key, value);
    }
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }

  private record Namespace(String catalog, String schema) {}
}
