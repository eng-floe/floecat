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
import ai.floedb.floecat.connector.spi.AuthProvider;
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
        // No credential shape this connector recognises -- either a cloud it does not map, or a
        // field Unity Catalog added after this code was written. Both are "cannot vend", not
        // "vended nothing": returning a credential object with an empty property map would reach
        // the service's usability check and fail the reconcile job terminally, when the correct
        // answer is the same fallback to a configured storage authority the non-AWS branch takes.
        LOG.warnf(
            "Unity Catalog vended no AWS credentials for %s (unsupportedCloud=%s); "
                + "falling back to a storage authority",
            fullName, credentials.hasUnsupportedCredentials());
        return Optional.empty();
      }

      Map<String, String> properties = new LinkedHashMap<>();
      putIfNonBlank(properties, "s3.access-key-id", aws.accessKeyId());
      putIfNonBlank(properties, "s3.secret-access-key", aws.secretAccessKey());
      putIfNonBlank(properties, "s3.session-token", aws.sessionToken());
      putIfNonBlank(properties, "s3.access-point", aws.accessPoint());
      // An incomplete tuple is deliberately passed through rather than dropped: the service decides
      // how strict to be, because the reconcile path needs a renewable session tuple and the query
      // path does not. Only "no credentials at all" is handled here, above.
      return Optional.of(
          new FloecatConnector.VendedStorageCredentials(
              properties,
              FloecatConnector.VendedStorageCredentials.expiryFromEpochMillis(
                  credentials.expirationEpochMillis()),
              credentials.storageUrl()));
    } catch (UnityCatalogException e) {
      throw classifyAccessFailure(e);
    }
  }

  /**
   * Releases the catalog client's transport and the auth provider's, along with the Delta engine's.
   *
   * <p>A connector is built per vend -- once per scan session and once per file group -- so the
   * HTTP client behind {@code catalog} would otherwise leak a selector thread and an executor pool
   * on every call. The auth provider leaks the same way: with {@code oauth.mode=cli} it is an
   * {@code OAuth2BearerAuthProvider} wrapping a token provider that owns a second {@link
   * java.net.http.HttpClient}.
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

  /**
   * Turns a Unity Catalog failure into the typed signal the storage service classifies on.
   *
   * <p>Every permanent refusal must be named here, not just 401 and 403. Databricks answers the
   * credentials endpoint with {@code 400} plus an {@code error_code} when the workspace lacks
   * {@code EXTERNAL USE SCHEMA} or the table has external access turned off, and with {@code 404}
   * for a table id it no longer knows -- none of which change on retry. Anything left unclassified
   * escapes as a plain {@link UnityCatalogException}, which the service maps to a retryable {@code
   * INTERNAL}, so the reconciler would loop on a job that can never succeed.
   *
   * <p>Transient failures -- 5xx, rate limits, transport errors, and a malformed body, which is
   * usually a proxy error page rather than the catalog itself -- stay unclassified on purpose.
   */
  private static RuntimeException classifyAccessFailure(UnityCatalogException failure) {
    return switch (failure.failure()) {
      case UNAUTHENTICATED ->
          new SourceCatalogAccessException(
              SourceCatalogAccessException.Denial.UNAUTHENTICATED, failure.getMessage());
      case PERMISSION_DENIED ->
          new SourceCatalogAccessException(
              SourceCatalogAccessException.Denial.PERMISSION_DENIED, failure.getMessage());
      case NOT_FOUND, INVALID_REQUEST ->
          new SourceCatalogAccessException(
              SourceCatalogAccessException.Denial.UNSUPPORTED, failure.getMessage());
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
