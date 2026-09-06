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
import ai.floedb.floecat.connector.spi.LogSafeText;
import ai.floedb.floecat.connector.spi.SourceCatalogAccessException;
import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.engine.Engine;
import java.time.Instant;
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

  /** Enough of a catalog-supplied table name to identify it in a log line. */
  private static final int MAX_LOGGED_NAME_CHARS = 256;

  /** Enough of a refusal to explain it. The client already caps its body snippet at 2,000. */
  private static final int MAX_REASON_CHARS = 1_024;

  /** An error_code is a short token; anything longer is not one. */
  private static final int MAX_CODE_CHARS = 64;

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

  @Override
  public Optional<FloecatConnector.VendedStorageCredentials> vendStorageCredentials(
      String namespaceFq, String tableName) {
    String fullName = namespaceFq + "." + tableName;
    try {
      // Lenient and wrapped, like every other path here that does not read the schema: vending
      // needs the table id and nothing else, and a strict decode would refuse to vend for a table
      // whose columns are malformed -- terminally, since classifyAccessFailure reads an
      // INVALID_RESPONSE with no status as a permanent refusal. The wrapper keeps a proxy's error
      // page out of the message describeRefusal builds the terminal reason from.
      Optional<UnityCatalogTable> table =
          withoutResponseBodyInMessage(
              fullName, () -> catalog.getTableWithLenientColumns(fullName));
      if (table.isEmpty()) {
        // Not terminal, for the same reason classifyAccessFailure below leaves an unenveloped 404
        // unclassified: getTable folds every NOT_FOUND into an empty Optional, so a genuinely
        // absent table and an HTML 404 from a load balancer mid-deploy arrive here identically.
        // Terminalizing would permanently fail a job on a condition that recovers by itself.
        // Distinguishing them needs the error_code envelope, which this call does not carry.
        LOG.warnf(
            "Unity Catalog table %s not found; cannot vend credentials",
            LogSafeText.bounded(fullName, MAX_LOGGED_NAME_CHARS));
        return Optional.empty();
      }
      String tableId = table.get().tableId();
      if (tableId == null || tableId.isBlank()) {
        // Terminal, and typed so it reads as one. The credentials endpoint keys on table_id, and a
        // catalog that omits it for a table will keep omitting it. A bare IllegalStateException
        // escapes classifyAccessFailure, reaches the service unrecognised, and comes back as a
        // retryable INTERNAL -- the reconcile loop this classification exists to close.
        throw new SourceCatalogAccessException(
            SourceCatalogAccessException.Denial.UNSUPPORTED,
            "Unity Catalog table has no table_id: "
                + LogSafeText.bounded(fullName, MAX_LOGGED_NAME_CHARS));
      }

      // Deliberately not wrapped, unlike the lookup above. httpFailure suppresses the response body
      // for this route already -- includeResponseBody is false for credentialsPath -- so there is
      // no page to keep out of the message, and the catalog's own text is what describeRefusal
      // turns into the terminal reason an operator reads.
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
            LogSafeText.bounded(fullName, MAX_LOGGED_NAME_CHARS),
            credentials.hasUnsupportedCredentials());
        return Optional.empty();
      }

      Map<String, String> properties = new LinkedHashMap<>();
      putIfNonBlank(properties, "s3.access-key-id", aws.accessKeyId());
      putIfNonBlank(properties, "s3.secret-access-key", aws.secretAccessKey());
      putIfNonBlank(properties, "s3.session-token", aws.sessionToken());
      putIfNonBlank(properties, "s3.access-point", aws.accessPoint());
      // A tuple missing its session token or access point is deliberately passed through rather
      // than dropped: the service decides how strict to be, because the reconcile path needs a
      // renewable session tuple and the query path does not. Not the access key or secret -- the
      // client rejects a payload missing either as INVALID_RESPONSE before it reaches here, so
      // those two are never in question by this point. Only "no credentials at all" is handled
      // here, above.
      return Optional.of(
          new FloecatConnector.VendedStorageCredentials(
              properties, credentials.storageUrl(), vendedExpiry(credentials)));
    } catch (UnityCatalogException e) {
      throw classifyAccessFailure(e, fullName);
    }
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
      // A negative status means no HTTP response was classified, so no body was ever interpolated:
      // these messages are fixed strings the client built itself ("authentication is
      // misconfigured", "request interrupted"). Rewriting them suppresses nothing and discards the
      // one line naming what to fix -- and SourceCatalogAccessException carries no cause, so on the
      // vend path the original becomes unreachable rather than merely demoted.
      if (e.statusCode() < 0) {
        throw e;
      }
      throw new UnityCatalogException(
          e.failure(),
          e.statusCode(),
          e.errorCode(),
          e.hasErrorEnvelope(),
          safeSummary(e, fullName),
          e);
    }
  }

  /**
   * The failure named by kind and status, with the object name only when it cannot itself trigger.
   *
   * <p>The one rendering guaranteed not to read as a missing object, used wherever a message is
   * replaced for that reason.
   */
  private static String safeSummary(UnityCatalogException failure, String fullName) {
    String summary = "Unity Catalog " + failure.failure() + " [" + failure.statusCode() + "]";
    String withName = summary + " for " + LogSafeText.bounded(fullName, MAX_LOGGED_NAME_CHARS);
    return namesAMissingObject(withName) ? summary : withName;
  }

  /**
   * The same failure with a message the missing-object heuristic cannot match, or it unchanged.
   *
   * <p>For the failures this class hands back untyped. Wrapping the calls is not enough on its own:
   * the credentials leg suppresses its response body and so is deliberately unwrapped, yet its
   * message still reads "returned HTTP 404 for /api/2.0/..." -- and on the lookup leg {@code
   * errorCode()} is unfiltered, so a catalog answering {@code "error_code": "table not found"} puts
   * the phrase in the reason of a call that was wrapped. Both end up in a {@code
   * StatusRuntimeException} raised inside JavaConnectorCaptureEngine's try-with-resources, where
   * the heuristic reads it as a missing table and ReconcileExecutor maps that to {@code OBSOLETE}.
   * A wrong vend path would retire the table upstream.
   */
  private static UnityCatalogException withoutMissingObjectPhrasing(
      UnityCatalogException failure, String fullName) {
    return namesAMissingObject(String.valueOf(failure.getMessage()))
        ? new UnityCatalogException(
            failure.failure(),
            failure.statusCode(),
            failure.errorCode(),
            failure.hasErrorEnvelope(),
            safeSummary(failure, fullName),
            failure)
        : failure;
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

  /**
   * Unity's {@code expiration_time}, or null when it was absent or unusable.
   *
   * <p>The shared parser folds absent, blank, non-positive, out-of-range and unparseable into the
   * same null, and a null expiry surfaces much later as "missing s3.session-token-expires-at-ms" --
   * an Iceberg key Unity never sends. Saying so here is the only record that a value arrived and
   * was rejected.
   */
  private static Instant vendedExpiry(TemporaryTableCredentials credentials) {
    String raw = credentials.expirationEpochMillis();
    Instant expiry = FloecatConnector.VendedStorageCredentials.expiryFromEpochMillis(raw);
    if (expiry == null && raw != null && !raw.isBlank()) {
      // Neither the value nor a classification of it. The field comes off the credentials response
      // -- the one body this route suppresses because it may carry a secret -- and naming which
      // way it was unusable took more machinery than the answer was worth: the operator's next
      // step is the same for a zero, a wrong unit and a word.
      LOG.warnf("Unity Catalog sent an unusable expiration_time, vending without an expiry");
    }
    return expiry;
  }

  /**
   * The failure's message, with its {@code error_code} appended when it is not already there.
   *
   * <p>The message carries up to two thousand characters of catalog or proxy response body, and it
   * reaches a log line and a gRPC status description. It is flattened and bounded on the way, like
   * every other value here that came off the wire. The code is appended afterwards so bounding
   * cannot cut it off, and never dropped: it is often the only thing naming which permanent cause
   * fired.
   */
  private static String describeRefusal(UnityCatalogException failure) {
    // Both halves come off the wire. The client shape-checks the code only on the route that
    // suppresses response bodies, so on every other one it is whatever the catalog sent.
    String rawCode = failure.errorCode();
    String rawMessage = failure.getMessage();
    String errorCode = LogSafeText.bounded(rawCode, MAX_CODE_CHARS);
    String message = LogSafeText.bounded(rawMessage, MAX_REASON_CHARS);
    if (errorCode == null || errorCode.isBlank()) {
      // The workspace may have answered with a reason whose text the client judged unsafe to print.
      // Terminality already turns on that fact; without it here the operator reads a permanently
      // failed job as though the catalog said nothing, when the reason is in its audit log.
      String withheld = failure.hasErrorEnvelope() ? " (error code withheld)" : "";
      // Neither a message nor a code: name the failure kind rather than hand a terminal exception a
      // null, which a human would read as a permanently failed job with no reason at all.
      return (message == null ? failure.failure().name() : message) + withheld;
    }
    // A message-less failure is the one case where the code is the whole diagnostic, and this feeds
    // a terminal exception -- folding it in with "already present" would leave a human reading
    // null.
    if (message == null) {
      return errorCode;
    }
    // The client interpolates the code into its own message on every route that keeps the response
    // body. The vending route suppresses that body, so its message carries no code even when
    // errorCode() has one -- the case this appends for. Appending unconditionally prints the code
    // twice in the only diagnostic a job that will not be retried leaves behind.
    // Compared against what the client actually interpolated, not the bounded form: a code longer
    // than the bound would never match its own truncation, and would be appended again mangled.
    return rawMessage.contains(rawCode) ? message : message + " (" + errorCode + ")";
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
   *
   * <p>A 404 is permanent only when Databricks answered it. The client types 404 as {@code
   * NOT_FOUND} on status alone, so an HTML 404 from a load balancer or WAF in front of the
   * workspace -- what one briefly serves mid-deploy -- arrives here looking identical to an unknown
   * table id. Its {@code error_code} envelope is what separates them, and without one the failure
   * stays unclassified so the reconciler retries instead of permanently failing a job that would
   * have succeeded a minute later.
   */
  private static RuntimeException classifyAccessFailure(
      UnityCatalogException failure, String fullName) {
    // Only a failure that reached the workspace can be ambiguous, and only then does the missing
    // envelope make it retryable. The client types 404, 405, 422 and an unfollowed 3xx from status
    // alone, and each is something infrastructure in front of the workspace produces -- a load
    // balancer mid-deploy, a gateway, an SSO proxy redirecting to a login page. Terminalizing those
    // fails a job permanently on a condition that clears by itself.
    //
    // A negative status means the request never went out: the auth provider was misconfigured, or
    // returned a header this request cannot carry. Those never clear, so they must not take the
    // carve-out -- they fall through and terminalize.
    // Envelope presence, not the code: on the vending route a code the client judged unsafe to
    // show is withheld from errorCode(), and reading that as "no envelope" would send an enveloped
    // permanent refusal back to be retried forever.
    if (failure.statusCode() >= 0
        && (failure.failure() == UnityCatalogException.Failure.NOT_FOUND
            || failure.failure() == UnityCatalogException.Failure.INVALID_REQUEST)
        && !failure.hasErrorEnvelope()) {
      // Scrubbed, not returned raw: this arm exists to keep the failure retryable, and its message
      // says "returned HTTP 404 for ...". Left alone it reaches the missing-object heuristic and
      // becomes a permanent OBSOLETE -- silently undoing the carve-out it is part of.
      return withoutMissingObjectPhrasing(failure, fullName);
    }
    // With the error_code, which the message omits on the vending route: bodies are suppressed
    // there, so the surviving text is a bare "returned HTTP 400". That is the flagship case for
    // this feature -- a workspace without EXTERNAL USE SCHEMA, or a table with external access off
    // -- and once the refusal is terminal the code is the only thing telling the operator which of
    // several permanent causes fired. The accessor already withholds anything not code-shaped.
    // Scrubbed for the same heuristic. describeRefusal appends errorCode(), which is filtered
    // through the recognized set only on the credentials route -- so on the lookup route a catalog
    // answering "error_code": "table not found" puts the phrase into the reason of a call that was
    // wrapped. The reason travels out as the gRPC status description, which is what gets matched.
    String composed = describeRefusal(failure);
    String reason = namesAMissingObject(composed) ? safeSummary(failure, fullName) : composed;
    // A negative status on an INVALID_REQUEST comes from one place: applying authentication. A
    // token
    // the cache does not hold, one the endpoint refused, a header the request cannot carry. The
    // switch below would call all of those UNSUPPORTED, which the structured reason documents as
    // "neither authentication nor authorization" -- reporting a workspace-wide credential failure
    // as
    // a per-table "the catalog will not vend for this table". The disposition is terminal either
    // way; this only decides what the operator reads.
    if (failure.statusCode() < 0
        && failure.failure() == UnityCatalogException.Failure.INVALID_REQUEST) {
      return new SourceCatalogAccessException(
          SourceCatalogAccessException.Denial.UNAUTHENTICATED, reason);
    }
    // A shape the client rejected in a body that had already parsed -- aws_temp_credentials that
    // is not an object, or one without an access key and secret. The catalog answered, and it will
    // answer the same way next time, so this is as permanent as any refusal here. Untyped it falls
    // through to the service as an unrecognized runtime exception, comes back INTERNAL, and the
    // reconciler retries a malformed credential payload forever.
    //
    // Status, not the kind alone: an INVALID_RESPONSE carrying a real status is the other case --
    // a body that never became JSON at all, which is what a proxy answering mid-deploy produces.
    // That one may well parse on the next attempt, so it stays retryable. statusCode() documents
    // exactly this split.
    if (failure.statusCode() < 0
        && failure.failure() == UnityCatalogException.Failure.INVALID_RESPONSE) {
      return new SourceCatalogAccessException(
          SourceCatalogAccessException.Denial.UNSUPPORTED, reason);
    }
    return switch (failure.failure()) {
      case UNAUTHENTICATED ->
          new SourceCatalogAccessException(
              SourceCatalogAccessException.Denial.UNAUTHENTICATED, reason);
      case PERMISSION_DENIED ->
          new SourceCatalogAccessException(
              SourceCatalogAccessException.Denial.PERMISSION_DENIED, reason);
      case NOT_FOUND, INVALID_REQUEST ->
          new SourceCatalogAccessException(SourceCatalogAccessException.Denial.UNSUPPORTED, reason);
      default -> withoutMissingObjectPhrasing(failure, fullName);
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
