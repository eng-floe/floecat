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
import ai.floedb.floecat.client.unity.TemporaryTableCredentials;
import ai.floedb.floecat.client.unity.UnityCatalogClient;
import ai.floedb.floecat.client.unity.UnityCatalogException;
import ai.floedb.floecat.client.unity.UnityCatalogTable;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.connector.spi.SourceCatalogAccessException;
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

  UnityDeltaConnector(
      String connectorId,
      UnityCatalogClient catalog,
      Engine engine,
      Function<String, InputFile> parquetInput,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles) {
    super(connectorId, engine, parquetInput, ndvEnabled, ndvSampleFraction, ndvMaxFiles);
    this.catalog = catalog;
  }

  @Override
  public List<String> listNamespaces() {
    try {
      List<String> namespaces = new ArrayList<>();
      for (String catalogName : catalog.listCatalogs()) {
        for (String schemaName : catalog.listSchemas(catalogName)) {
          namespaces.add(catalogName + "." + schemaName);
        }
      }
      namespaces.sort(String::compareTo);
      return namespaces;
    } catch (RuntimeException e) {
      throw new RuntimeException("listNamespaces failed", e);
    }
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    Namespace namespace = parseNamespace(namespaceFq);
    if (namespace == null) {
      return List.of();
    }
    try {
      return catalog.listTables(namespace.catalog(), namespace.schema()).stream()
          .filter(table -> "DELTA".equalsIgnoreCase(table.dataSourceFormat()))
          .map(UnityCatalogTable::name)
          .sorted()
          .toList();
    } catch (RuntimeException e) {
      throw new RuntimeException("listTables failed", e);
    }
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

  @Override
  public Optional<FloecatConnector.VendedStorageCredentials> vendStorageCredentials(
      String namespaceFq, String tableName) {
    String fullName = namespaceFq + "." + tableName;
    try {
      Optional<UnityCatalogTable> table = catalog.getTable(fullName);
      if (table.isEmpty()) {
        LOG.warnf("Unity Catalog table %s not found; cannot vend credentials", fullName);
        return Optional.empty();
      }
      String tableId = table.get().tableId();
      if (tableId == null || tableId.isBlank()) {
        throw new IllegalStateException("Unity Catalog table has no table_id: " + fullName);
      }

      TemporaryTableCredentials credentials =
          catalog.generateTemporaryTableCredentials(
              tableId, UnityCatalogClient.TableOperation.READ);
      TemporaryTableCredentials.AwsCredentials aws = credentials.awsCredentials();
      if (aws == null) {
        if (credentials.hasUnsupportedCredentials()) {
          LOG.warnf(
              "Unity Catalog vended non-AWS credentials for %s; only AWS is supported, "
                  + "falling back to a storage authority",
              fullName);
          return Optional.empty();
        }
        return Optional.of(
            new FloecatConnector.VendedStorageCredentials(
                Map.of(), credentials.expiresAt(), credentials.storageUrl()));
      }

      Map<String, String> properties = new LinkedHashMap<>();
      putIfNonBlank(properties, "s3.access-key-id", aws.accessKeyId());
      putIfNonBlank(properties, "s3.secret-access-key", aws.secretAccessKey());
      putIfNonBlank(properties, "s3.session-token", aws.sessionToken());
      putIfNonBlank(properties, "s3.access-point", aws.accessPoint());
      return Optional.of(
          new FloecatConnector.VendedStorageCredentials(
              properties, credentials.expiresAt(), credentials.storageUrl()));
    } catch (UnityCatalogException e) {
      throw classifyAccessFailure(e);
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

  private static RuntimeException classifyAccessFailure(UnityCatalogException failure) {
    return switch (failure.failure()) {
      case UNAUTHENTICATED ->
          new SourceCatalogAccessException(
              SourceCatalogAccessException.Denial.UNAUTHENTICATED, failure.getMessage());
      case PERMISSION_DENIED ->
          new SourceCatalogAccessException(
              SourceCatalogAccessException.Denial.PERMISSION_DENIED, failure.getMessage());
      default -> failure;
    };
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
        field.put("type", column.typeText() != null ? column.typeText() : column.typeName());
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

  private static void putIfNonBlank(Map<String, String> values, String key, String value) {
    if (!isBlank(value)) {
      values.put(key, value);
    }
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }

  private record Namespace(String catalog, String schema) {}
}
