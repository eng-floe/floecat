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
import ai.floedb.floecat.client.unity.UnityCatalogException;
import ai.floedb.floecat.client.unity.UnityCatalogTable;
import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.engine.Engine;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
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
    this(
        connectorId,
        catalog,
        auth,
        engine,
        parquetInput,
        ndvEnabled,
        ndvSampleFraction,
        ndvMaxFiles,
        null);
  }

  UnityDeltaConnector(
      String connectorId,
      UnityCatalogClient catalog,
      AuthProvider auth,
      Engine engine,
      Function<String, InputFile> parquetInput,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles,
      AutoCloseable engineResources) {
    super(
        connectorId,
        engine,
        parquetInput,
        ndvEnabled,
        ndvSampleFraction,
        ndvMaxFiles,
        engineResources);
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
    // Lenient for the fields below, which no column decode can affect. The strict decode is not
    // free -- it fails the whole call on a malformed columns field -- and for an external table
    // with a storage location the catalog's column list is overwritten by the Delta log's a few
    // lines down without ever being read.
    UnityCatalogTable table =
        withoutResponseBodyInMessage(fullName, () -> catalog.getTableWithLenientColumns(fullName))
            .orElseThrow(
                () -> new IllegalStateException("Unity Catalog table not found: " + fullName));
    Map<String, String> descriptorProperties = new LinkedHashMap<>();
    putIfPresent(descriptorProperties, "table_type", table.tableType());
    putIfPresent(descriptorProperties, "data_source_format", table.dataSourceFormat());
    putIfPresent(descriptorProperties, "storage_location", table.storageLocation());

    String schemaJson = null;
    if (table.storageLocation() != null) {
      try {
        schemaJson = describeTableSchemaJson(table.storageLocation());
      } catch (Exception ignored) {
        // Fall back to UC column metadata when Delta snapshot metadata is unavailable.
      }
    }
    if (schemaJson == null) {
      // Only here is the catalog's column list the answer, so only here does it have to be decoded
      // strictly: a silently empty schema reported as authoritative is worse than a failure. Costs
      // a second lookup, on the two paths that reach it -- no storage location, or a Delta log that
      // would not read -- rather than on every describe.
      schemaJson = buildSchemaJson(requiredTable(fullName));
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
    // The lenient decode: this path reads only the location, and a malformed columns field must
    // not fail planning and capture for a table whose schema nothing here looks at.
    String location =
        withoutResponseBodyInMessage(fullName, () -> catalog.getTableWithLenientColumns(fullName))
            .orElseThrow(
                () -> new IllegalStateException("Unity Catalog table not found: " + fullName))
            .storageLocation();
    if (location == null || location.isBlank()) {
      throw new IllegalStateException("Table has no storage_location: " + fullName);
    }
    return location;
  }

  @Override
  protected Map<String, String> fallbackTablePropertiesForConstraints(
      String namespaceFq, String tableName) {
    try {
      // Lenient, for the same reason storageLocation is: this reads properties() and nothing else,
      // so a columns field rendered in a shape the strict decode rejects would drop the table's
      // constraints on the floor. Not wrapped, unlike the other call sites -- the catch below
      // swallows the failure, so no message escapes to be misread.
      return catalog
          .getTableWithLenientColumns(namespaceFq + "." + tableName)
          .map(UnityCatalogTable::properties)
          .orElseGet(Map::of);
    } catch (RuntimeException failure) {
      LOG.debugf(failure, "Constraint properties unavailable for %s.%s", namespaceFq, tableName);
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

  /**
   * Describes one view, or empty when the catalog does not have it.
   *
   * <p>Stricter than {@link #listViewDescriptors} on the same input, deliberately. Both build the
   * same descriptor, but a listing degrades a view whose {@code columns} the catalog renders in an
   * unreadable shape to an empty schema so one entry cannot hide the rest of the namespace, while
   * this asks for one named view and reports that shape as a failure rather than answering with a
   * schema it could not read.
   */
  @Override
  public Optional<FloecatConnector.ViewDescriptor> describeView(
      String namespaceFq, String viewName) {
    String fullName = namespaceFq + "." + viewName;
    return withoutResponseBodyInMessage(fullName, () -> catalog.getTable(fullName))
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
   * <p>A connector is built per capture, and capture is scoped to a single file group, so each
   * unreleased {@code catalog} costs a selector thread and an executor. The auth provider is the
   * same: with {@code oauth.mode=cli} it wraps a token provider owning a second {@link
   * java.net.http.HttpClient}. Failures are logged at warn, not debug, since a close that starts
   * failing repeats on every vend and is otherwise only visible as thread exhaustion.
   *
   * <p>{@code super.close()} releases the {@code RefreshingAwsClient} the engine was built on,
   * which holds an S3 connection pool and a credentials provider. Nothing else retains it.
   */
  @Override
  public void close() {
    try {
      catalog.close();
    } catch (RuntimeException e) {
      LOG.warnf(e, "Failed to close the Unity Catalog client for connector %s", id());
    } finally {
      try {
        if (auth instanceof AutoCloseable closeable) {
          closeable.close();
        }
      } catch (Exception e) {
        LOG.warnf(e, "Failed to close the auth provider for connector %s", id());
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
    return withoutResponseBodyInMessage(fullName, () -> catalog.getTable(fullName))
        .orElseThrow(() -> new IllegalStateException("Unity Catalog table not found: " + fullName));
  }

  /**
   * Runs a table lookup, restating any failure as its kind and status without the response body.
   *
   * <p>{@code GrpcReconcilerBackend.isMissingObjectFailure} decides {@code TABLE_MISSING} by
   * lowercasing the top-level message and looking for "not found", "does not exist" or a 404, and
   * it does not walk causes. {@code httpFailure} puts up to two kilobytes of response body in that
   * message on every route but vending, so a 502 whose gateway page happens to say "the requested
   * URL was not found on this server" would be reported as a permanently missing table -- the
   * retryable-read-as-permanent inversion the typed Failure enum exists to remove.
   *
   * <p>A genuinely missing object never arrives this way -- {@code getTable} turns {@code
   * NOT_FOUND} into an empty Optional, and callers report that through their own "not found"
   * message -- so suppressing the phrase here cannot hide one. What does arrive is everything else,
   * including a 404 an {@code error_code} classified as {@code PERMISSION_DENIED}: a workspace
   * hiding a table it will not admit exists. Writing "HTTP 404" into the message would hand that
   * back to the heuristic as a missing table, so the status is rendered in a form that cannot match
   * it, and the object name is dropped rather than trusted when it would reintroduce a trigger. The
   * original failure, body and all, stays as the cause for the logs.
   */
  private static <T> T withoutResponseBodyInMessage(String fullName, Supplier<T> lookup) {
    try {
      return lookup.get();
    } catch (UnityCatalogException e) {
      String summary = "Unity Catalog " + e.failure() + " [" + e.statusCode() + "]";
      String withName = summary + " for " + fullName;
      throw new UnityCatalogException(
          e.failure(),
          e.statusCode(),
          e.errorCode(),
          e.hasErrorEnvelope(),
          namesAMissingObject(withName) ? summary : withName,
          e);
    }
  }

  /**
   * Whether a message would be read as "this object does not exist" by the reconciler.
   *
   * <p>{@code GrpcReconcilerBackend} and {@code JavaConnectorCaptureEngine} both decide {@code
   * TABLE_MISSING} and {@code VIEW_MISSING} this way, on the top-level message and without walking
   * causes. Kept in step with them by these four literals; a phrase added there and not here costs
   * a retryable failure reported as permanent, which is the whole reason this method exists.
   */
  private static boolean namesAMissingObject(String message) {
    String normalized = message.toLowerCase(Locale.ROOT);
    return normalized.contains("http 404")
        || normalized.contains("status 404")
        || normalized.contains("not found")
        || normalized.contains("does not exist");
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
