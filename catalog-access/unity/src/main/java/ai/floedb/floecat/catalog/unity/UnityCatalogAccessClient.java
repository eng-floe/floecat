/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.catalog.unity;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogCapabilities;
import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.CatalogTable;
import ai.floedb.floecat.catalog.access.CatalogView;
import ai.floedb.floecat.catalog.access.CatalogViewDefinition;
import ai.floedb.floecat.catalog.access.ExternalObjectIdentity;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import ai.floedb.floecat.client.unity.TemporaryTableCredentials;
import ai.floedb.floecat.client.unity.UnityCatalogClient;
import ai.floedb.floecat.client.unity.UnityCatalogException;
import ai.floedb.floecat.client.unity.UnityCatalogTable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

final class UnityCatalogAccessClient implements CatalogClient {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final CatalogCapabilities CAPABILITIES =
      CatalogCapabilities.of(
          CatalogCapability.VALIDATE,
          CatalogCapability.LIST_NAMESPACES,
          CatalogCapability.LIST_TABLES,
          CatalogCapability.LOAD_TABLE,
          CatalogCapability.LIST_VIEWS,
          CatalogCapability.LOAD_VIEW,
          CatalogCapability.VEND_STORAGE_CREDENTIALS,
          CatalogCapability.VALIDATE_STORAGE_ACCESS,
          CatalogCapability.STABLE_OBJECT_IDS);

  private final UnityCatalogClient unity;
  private final AutoCloseable authenticationOwner;
  private final UnityStorageAccessValidator storageValidator;
  private final Map<String, String> storageRouting;
  private final AtomicBoolean closed = new AtomicBoolean();

  /**
   * The last schema walked and its relations; see {@link #relationsIn}.
   *
   * <p>One entry, not a map. The sharing this exists for is between {@code listTables} and {@code
   * listViews} on the same namespace, and the reconciler calls those adjacently -- so a map only
   * retained every schema's full {@code /tables} payload, columns included, for the client's whole
   * life, alongside the reconciler's own discovered maps.
   */
  private final java.util.concurrent.atomic.AtomicReference<
          java.util.Map.Entry<Namespace, List<UnityCatalogTable>>>
      lastRelations = new java.util.concurrent.atomic.AtomicReference<>();

  /**
   * The table the last vend loaded, for the storage probe that follows it.
   *
   * <p>{@code CatalogIntegrationDiscovery.findValidationTarget} calls {@code
   * vendStorageCredentials} and then {@code validateStorageAccess} on the same object with nothing
   * between them that could change the answer, and both need the table's storage location -- so
   * each validation attempt paid three catalog round trips where the attempt cap is sized for two.
   *
   * <p>Consumed rather than cached: taken once by the probe that follows, so a later standalone
   * {@code validateStorageAccess} on the same name loads the table again rather than probing a
   * location that may have moved since. Single-entry for the same reason {@code lastRelations} is.
   */
  private final java.util.concurrent.atomic.AtomicReference<
          java.util.Map.Entry<CatalogObjectName, UnityCatalogTable>>
      lastVendedTable = new java.util.concurrent.atomic.AtomicReference<>();

  UnityCatalogAccessClient(
      UnityCatalogClient unity,
      AutoCloseable authenticationOwner,
      UnityStorageAccessValidator storageValidator,
      Map<String, String> storageRouting) {
    this.unity = Objects.requireNonNull(unity, "unity");
    this.authenticationOwner = authenticationOwner;
    this.storageValidator = Objects.requireNonNull(storageValidator, "storageValidator");
    this.storageRouting = Map.copyOf(Objects.requireNonNull(storageRouting, "storageRouting"));
  }

  @Override
  public CatalogCapabilities capabilities() {
    return CAPABILITIES;
  }

  @Override
  public void validate() {
    UnityCatalogErrors.run("connection validation", unity::listCatalogs);
  }

  @Override
  public List<NamespacePath> listNamespaces(NamespacePath parent) {
    Objects.requireNonNull(parent, "parent");
    return UnityCatalogErrors.call(
        "namespace listing",
        () -> {
          if (parent.segments().isEmpty()) {
            return unity.listCatalogs().stream()
                .filter(name -> !name.isBlank())
                .map(name -> new NamespacePath(List.of(name)))
                .sorted()
                .toList();
          }
          if (parent.segments().size() == 1) {
            String catalog = parent.segments().getFirst();
            return unity.listSchemas(catalog).stream()
                .filter(name -> !name.isBlank())
                .map(name -> new NamespacePath(List.of(catalog, name)))
                .sorted()
                .toList();
          }
          return List.of();
        });
  }

  @Override
  public List<CatalogObjectName> listTables(NamespacePath namespace) {
    Namespace names = schemaOrNull(namespace);
    if (names == null) {
      return List.of();
    }
    return UnityCatalogErrors.call(
        "table listing",
        () ->
            relationsIn(names).stream()
                .filter(UnityCatalogAccessClient::isSupportedTable)
                .filter(table -> !table.name().isBlank())
                .map(table -> new CatalogObjectName(namespace, table.name()))
                .sorted()
                .toList());
  }

  @Override
  public CatalogTable loadTable(CatalogObjectName name) {
    Objects.requireNonNull(name, "name");
    requireSchema(name.namespace());
    return UnityCatalogErrors.call(
        "table loading",
        () -> {
          UnityCatalogTable table = required(name);
          if (!isSupportedTable(table)) {
            throw new CatalogAccessException(
                CatalogAccessException.Code.UNSUPPORTED,
                "Unity Catalog table format is not supported");
          }
          return new CatalogTable(
              name,
              identity(name, table),
              upperTrimmed(table.dataSourceFormat()),
              schemaJson(name, table),
              partitionKeys(table),
              Optional.empty(),
              optional(table.storageLocation()),
              table.properties());
        });
  }

  @Override
  public List<CatalogObjectName> listViews(NamespacePath namespace) {
    Namespace names = schemaOrNull(namespace);
    if (names == null) {
      return List.of();
    }
    return UnityCatalogErrors.call(
        "view listing",
        () ->
            relationsIn(names).stream()
                .filter(UnityCatalogAccessClient::isView)
                .filter(table -> !table.name().isBlank())
                .map(table -> new CatalogObjectName(namespace, table.name()))
                .sorted()
                .toList());
  }

  @Override
  public CatalogView loadView(CatalogObjectName name) {
    Objects.requireNonNull(name, "name");
    requireSchema(name.namespace());
    return UnityCatalogErrors.call(
        "view loading",
        () -> {
          UnityCatalogTable table = required(name);
          if (!isView(table)) {
            throw new CatalogAccessException(
                CatalogAccessException.Code.NOT_FOUND, "Unity Catalog view was not found");
          }
          // Refused, not returned empty. The overlay reconciler writes the CatalogView straight
          // to the repository, so it never passes the invariant ViewServiceImpl enforces on the
          // gRPC path, where an empty SQL-definition list is rejected outright. A view persisted
          // with no definitions then answers "" from ViewNode.sql() and carries no blob URI
          // either, so it resolves like any other relation and cannot be planned. UNSUPPORTED is
          // scoped to this object, so the reconciler's per-view skip preserves whatever was
          // materialized before and counts the skip instead of publishing a view no query can use.
          List<CatalogViewDefinition> definitions =
              optional(table.viewDefinition())
                  .map(sql -> List.of(new CatalogViewDefinition(sql, "spark")))
                  .orElseThrow(
                      () ->
                          new CatalogAccessException(
                              CatalogAccessException.Code.UNSUPPORTED,
                              "Unity Catalog view exposes no view_definition to plan against"));
          return new CatalogView(
              name,
              identity(name, table),
              viewSchemaJson(name, table),
              definitions,
              name.namespace(),
              table.properties());
        });
  }

  @Override
  public Optional<VendedStorageCredentials> vendStorageCredentials(CatalogObjectName name) {
    Objects.requireNonNull(name, "name");
    requireSchema(name.namespace());
    return UnityCatalogErrors.call(
        "storage credential vending",
        () -> {
          UnityCatalogTable table = requiredWithoutSchema(name);
          lastVendedTable.set(java.util.Map.entry(name, table));
          String tableId = nonBlank(table.tableId());
          if (tableId == null) {
            throw new CatalogAccessException(
                CatalogAccessException.Code.UNSUPPORTED,
                "Unity Catalog table does not expose a stable table ID");
          }
          TemporaryTableCredentials response;
          try {
            response =
                unity.generateTemporaryTableCredentials(
                    tableId, UnityCatalogClient.TableOperation.READ);
          } catch (UnityCatalogException failure) {
            throw credentialRouteFailure(name, failure);
          }
          TemporaryTableCredentials.AwsCredentials aws = response.awsCredentials();
          if (aws == null) {
            // Named like every other unusable shape in this method rather than returned empty. The
            // transport documents this as a "fall back to a storage authority" signal, and that is
            // true of the connector that reads it -- but this provider is reached through a Catalog
            // Integration, where SourceCatalogCredentialVendor already refuses an empty vend
            // outright. So empty bought no fallback here and cost the diagnosis: an Azure- or
            // GCP-backed workspace reported "vended no storage credentials" with nothing naming the
            // cloud. UNSUPPORTED is per-table for validation, so the walk still steps to the next
            // table and reports this reason if none can vend.
            throw new CatalogAccessException(
                CatalogAccessException.Code.UNSUPPORTED,
                response.hasUnsupportedCredentials()
                    ? "Unity Catalog vended storage credentials for a cloud this provider does not"
                        + " support"
                    : "Unity Catalog vended no recognized storage credentials");
          }
          LinkedHashMap<String, String> properties = new LinkedHashMap<>(storageRouting);
          put(properties, "s3.access-key-id", aws.accessKeyId());
          put(properties, "s3.secret-access-key", aws.secretAccessKey());
          put(properties, "s3.session-token", aws.sessionToken());
          put(properties, "s3.access-point", aws.accessPoint());
          if (!properties.containsKey("s3.access-key-id")
              || !properties.containsKey("s3.secret-access-key")) {
            // A boundary guard, not the path a half tuple from the HTTP transport takes.
            // HttpUnityCatalogClient.awsCredentials already refuses a present aws_temp_credentials
            // missing either key with INVALID_RESPONSE, which UnityCatalogErrors maps to INTERNAL
            // and validation treats as integration-wide -- deliberately, because a 200 carrying
            // half a credential is a catalog breaking its own contract rather than a table this
            // provider cannot vend for, and answering UNSUPPORTED would let the walk pass on a
            // sibling table while the catalog is malfunctioning.
            //
            // This stays because UnityCatalogClient is an interface and AwsCredentials is a record
            // whose fields may be null: an implementation other than the HTTP one can still reach
            // here with half a tuple, and that must not be published. UNSUPPORTED like the
            // publishability check below it -- a deterministic description of what this
            // Integration returned, which is what the vendor reserves that code for.
            throw new CatalogAccessException(
                CatalogAccessException.Code.UNSUPPORTED,
                "Unity Catalog vended incomplete AWS credentials for " + fullName(name));
          }
          // Both absent means there is no scope to state, and a blank one is not a narrow
          // credential -- StorageLocations.covers("") is true for every location, the covers check
          // below is skipped because tableLocation is null, and SourceCatalogCredentialVendor
          // stamps the blank string as the response prefix, which its own comment says every
          // consumer reads as unrestricted.
          //
          // Refused by name rather than returned empty. An empty vend is now a refusal too, but a
          // generic one; naming the reason is the difference between an operator seeing "vended no
          // storage credentials" and seeing that the table has no location to scope against.
          String scope = nonBlank(response.storageUrl());
          if (scope == null) {
            scope = optional(table.storageLocation()).orElse(null);
          }
          if (scope == null) {
            throw new CatalogAccessException(
                CatalogAccessException.Code.UNSUPPORTED,
                "Unity Catalog vended credentials cannot be scoped for "
                    + fullName(name)
                    + ": neither the credentials response nor the table states a location");
          }
          // The publishability contract the runtime vend path enforces, applied here so validation
          // cannot pass a tuple a later read terminally refuses. An integration vends only when
          // what it holds is itself a temporary session, and an AWS temporary credential always
          // carries a session token and an expiry; a pair without them is not a long-lived
          // credential to pass along but an external location that is not configured for
          // delegation. UNSUPPORTED because it describes the Integration deterministically: a
          // retry returns the same shape, so it is a refusal rather than an upstream fault, and a
          // validation walk steps to the next table instead of stopping on it.
          Optional<Instant> expiresAt = parseExpiry(response.expirationEpochMillis());
          if (nonBlank(properties.get("s3.session-token")) == null || expiresAt.isEmpty()) {
            throw new CatalogAccessException(
                CatalogAccessException.Code.UNSUPPORTED,
                "Unity Catalog vended credentials cannot be published for "
                    + fullName(name)
                    + ": a delegated vend requires s3.session-token and expiration_time, missing "
                    + (nonBlank(properties.get("s3.session-token")) == null
                        ? (expiresAt.isEmpty() ? "both" : "s3.session-token")
                        : "expiration_time"));
          }
          VendedStorageCredentials credentials =
              new VendedStorageCredentials(properties, scope, expiresAt);
          String tableLocation = nonBlank(table.storageLocation());
          if (tableLocation != null && !credentials.covers(tableLocation)) {
            throw new CatalogAccessException(
                CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID,
                "Vended storage credentials do not cover the Unity Catalog table location");
          }
          return Optional.of(credentials);
        });
  }

  @Override
  public void validateStorageAccess(
      CatalogObjectName name, VendedStorageCredentials vendedStorageCredentials) {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(vendedStorageCredentials, "vendedStorageCredentials");
    requireSchema(name.namespace());
    UnityCatalogErrors.run(
        "storage access validation",
        () -> {
          UnityCatalogTable table = vendedTableOrLoad(name);
          String location =
              optional(table.storageLocation())
                  .orElseThrow(
                      () ->
                          new CatalogAccessException(
                              CatalogAccessException.Code.UNSUPPORTED,
                              "Unity Catalog table does not expose a storage location"));
          if (!vendedStorageCredentials.covers(location)) {
            throw new CatalogAccessException(
                CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID,
                "Vended storage credentials do not cover the Unity Catalog table location");
          }
          storageValidator.validate(location, vendedStorageCredentials);
        });
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    try {
      unity.close();
    } finally {
      closeQuietly(authenticationOwner);
    }
  }

  private UnityCatalogTable required(CatalogObjectName name) {
    return found(name, unity.getTable(fullName(name)));
  }

  /**
   * The same lookup for callers that read no column.
   *
   * <p>{@link UnityCatalogClient#getTable} decodes {@code columns} strictly and its javadoc directs
   * every other caller here, because that strictness otherwise reaches past schema reporting into
   * vending and validation -- neither of which touches a column, and both of which would fail the
   * whole table on a {@code columns} shape some Unity deployment renders differently. {@code
   * UnityDeltaConnector.vendStorageCredentials} makes the same choice for the same reason.
   */
  private UnityCatalogTable requiredWithoutSchema(CatalogObjectName name) {
    return found(name, unity.getTableWithLenientColumns(fullName(name)));
  }

  private static UnityCatalogTable found(
      CatalogObjectName name, Optional<UnityCatalogTable> table) {
    return table.orElseThrow(
        () ->
            new CatalogAccessException(
                CatalogAccessException.Code.NOT_FOUND, "Unity Catalog object was not found"));
  }

  /**
   * Every relation in a schema, fetched once.
   *
   * <p>Unity serves tables and views from the same {@code /tables} endpoint, and this provider
   * separates them by predicate. Calling it twice per namespace -- which the reconciler does, once
   * for each kind -- paid for two full paginated walks to learn the same thing, and opened a window
   * where the two listings could disagree if the upstream changed between them.
   *
   * <p>Safe to hold for the life of this client because that life is one operation: every caller
   * opens a client in a try-with-resources and closes it when the operation ends, so there is no
   * span over which the cache could go stale relative to what the caller already decided.
   */
  private List<UnityCatalogTable> relationsIn(Namespace names) {
    var cached = lastRelations.get();
    if (cached != null && cached.getKey().equals(names)) {
      return cached.getValue();
    }
    List<UnityCatalogTable> relations = unity.listTables(names.catalog(), names.schema());
    lastRelations.set(java.util.Map.entry(names, relations));
    return relations;
  }

  private static String fullName(CatalogObjectName name) {
    List<String> segments = new ArrayList<>(name.namespace().segments());
    segments.add(name.name());
    return String.join(".", segments);
  }

  /**
   * The schema a listing should read, or {@code null} when the namespace cannot hold one.
   *
   * <p>Listing is not addressing. Unity keeps tables in {@code catalog.schema}, so "what tables are
   * in this catalog?" has a true answer -- none -- and a namespace walk that asks is behaving
   * correctly, not misconfigured. {@code listNamespaces} hands out one-segment catalog paths by
   * design, and {@code CatalogOverlayReconciler} lists tables for every namespace an overlay
   * selects, which with no include filters is all of them: the documented default. Throwing here
   * failed every unfiltered overlay against a Unity workspace on the first catalog it reached.
   *
   * <p>{@link #requireSchema} still throws for {@code loadTable}, vending and validation, where a
   * namespace of the wrong depth is a caller naming an object that cannot exist.
   */
  private static Namespace schemaOrNull(NamespacePath namespace) {
    Objects.requireNonNull(namespace, "namespace");
    return namespace.segments().size() == 2
        ? new Namespace(namespace.segments().get(0), namespace.segments().get(1))
        : null;
  }

  private static Namespace requireSchema(NamespacePath namespace) {
    Objects.requireNonNull(namespace, "namespace");
    if (namespace.segments().size() != 2) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog table namespaces must contain catalog and schema");
    }
    return new Namespace(namespace.segments().get(0), namespace.segments().get(1));
  }

  /**
   * Whether this relation is a Delta table the external read path can act on.
   *
   * <p>Named kinds, not "anything that is not a VIEW". Unity also answers MATERIALIZED_VIEW,
   * STREAMING_TABLE and FOREIGN, and only promises {@code storage_location} for MANAGED and
   * EXTERNAL -- so the negative test advertised a materialized view as a Delta table, an Overlay
   * persisted it as TF_DELTA, and vending later selected an object with no addressable storage. The
   * view-like kinds are not offered as views either: {@code loadView} needs a definition and a
   * representable output schema, and promoting them is a feature rather than a classification fix.
   * Anything unrecognised is skipped for the same reason it is here -- this provider cannot say
   * what it is.
   */
  private static boolean isSupportedTable(UnityCatalogTable table) {
    // Normalized through the same helper loadTable uses. Comparing the raw value here let a
    // deployment returning "DELTA " be dropped from every listing while loadTable would have
    // accepted it -- the table simply never appeared in an overlay, with nothing pointing at the
    // whitespace.
    if (!"DELTA".equals(upperTrimmed(table.dataSourceFormat()))) {
      return false;
    }
    String kind = upperTrimmed(table.tableType());
    return "MANAGED".equals(kind) || "EXTERNAL".equals(kind);
  }

  private static String upperTrimmed(String value) {
    return value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
  }

  /**
   * The table's partition columns, in partition order.
   *
   * <p>Unity gives each partition column its ordinal in {@code partition_index} and leaves it
   * absent for the rest, so index 0 is the first partition column rather than a default. This list
   * is persisted into {@code UpstreamRef.partition_keys} and read when marking partition columns
   * and building partition specs; an empty one records a partitioned table as unpartitioned and
   * silently gives up pruning for it.
   */
  private static List<String> partitionKeys(UnityCatalogTable table) {
    return table.columns().stream()
        .filter(column -> column.partitionIndex() != null && column.partitionIndex() >= 0)
        .sorted(java.util.Comparator.comparingInt(UnityCatalogTable.Column::partitionIndex))
        .map(UnityCatalogTable.Column::name)
        .filter(name -> !name.isBlank())
        .toList();
  }

  /**
   * Reclassifies a refusal from the credentials route, and only from there.
   *
   * <p>Unity answers a table it cannot mint credentials for with a 4xx carrying the workspace error
   * envelope, which the transport reads as INVALID_REQUEST and {@code UnityCatalogErrors} then maps
   * to INVALID_CONFIGURATION. That code is deliberately terminal for a validation walk -- it is
   * meant to describe the Integration -- so one such table ended the whole search and reported the
   * Integration invalid. It is not integration-wide: a table with no storage credential, on a
   * non-cloud location, or outside Unity's managed storage answers this way while its neighbours
   * vend, and {@code hive_metastore} -- the case the per-catalog budget exists to survive --
   * answers it for every table it holds.
   *
   * <p>UNSUPPORTED joins the per-table refusals this method already raises, so the walk steps to
   * the next table and reports this reason only if none can vend. Scoped to this one call on
   * purpose: an INVALID_CONFIGURATION from listing or loading really does describe the Integration,
   * and still ends the walk.
   */
  private static RuntimeException credentialRouteFailure(
      CatalogObjectName name, UnityCatalogException failure) {
    if (failure.failure() != UnityCatalogException.Failure.INVALID_REQUEST) {
      return failure;
    }
    return new CatalogAccessException(
        CatalogAccessException.Code.UNSUPPORTED,
        "Unity Catalog cannot mint storage credentials for " + fullName(name),
        failure);
  }

  /** The table the vend just loaded when it is this one, else a fresh load. */
  private UnityCatalogTable vendedTableOrLoad(CatalogObjectName name) {
    var vended = lastVendedTable.getAndSet(null);
    if (vended != null && vended.getKey().equals(name)) {
      return vended.getValue();
    }
    return requiredWithoutSchema(name);
  }

  /**
   * Normalized the same way {@code isSupportedTable} normalizes it. A padded {@code "VIEW "} failed
   * this test, then failed the table test on its data source format, and disappeared from the
   * overlay with no log line and no skip counter.
   */
  private static boolean isView(UnityCatalogTable table) {
    return "VIEW".equals(upperTrimmed(table.tableType()));
  }

  private static ExternalObjectIdentity identity(CatalogObjectName name, UnityCatalogTable table) {
    String tableId = nonBlank(table.tableId());
    return tableId == null
        ? ExternalObjectIdentity.pathFallback(name)
        : ExternalObjectIdentity.stable(tableId);
  }

  /**
   * The view's output schema as Iceberg schema JSON.
   *
   * <p>A different form from {@link #schemaJson}, and the difference belongs to the consumer. A
   * table's schema is stored verbatim beside a {@code column_id_algorithm} derived from its format,
   * so Delta form is carried and interpreted later. A view has no such field: {@code View} holds
   * resolved {@code output_columns}, so {@code CatalogOverlayReconciler} parses this string through
   * {@code IcebergSchemaMapper} -> {@code SchemaParser.fromJson}, which requires an {@code id} and
   * {@code required} on every field. Delta form has neither, so emitting it aborted the entire
   * overlay -- every table in it, not just the view -- on the first non-empty view.
   *
   * <p>Ids are assigned by position from 1. They are not Unity identifiers, because Unity issues
   * none for view columns; for a view's output schema position is the identity, which is what a
   * reader resolving output columns needs.
   */
  private static String viewSchemaJson(CatalogObjectName name, UnityCatalogTable table) {
    requireColumns(name, table, "view");
    var fields = JSON.createArrayNode();
    int[] nextId = {1};
    for (UnityCatalogTable.Column column : table.columns()) {
      // The field's own id before the type's, so a struct's members sit above the field that holds
      // them. Iceberg only requires the ids be unique across the schema, but reading a schema whose
      // ids ascend with nesting is easier than reading one where they do not.
      int fieldId = nextId[0]++;
      JsonNode type = columnType(column, nextId);
      if (type == null) {
        // Named rather than guessed. Widening an unknown type to string would hand the planner a
        // schema that parses and lies; a struct, array or map output column needs the nested
        // Iceberg form this does not build.
        throw new CatalogAccessException(
            CatalogAccessException.Code.UNSUPPORTED,
            "Unity Catalog view column type is not representable: column="
                + column.name()
                + " type="
                + (column.typeText() == null ? column.typeName() : column.typeText()));
      }
      var field = JSON.createObjectNode();
      field.put("id", fieldId);
      field.put("name", column.name());
      field.put("required", !column.nullable());
      field.set("type", type);
      fields.add(field);
    }
    var schema = JSON.createObjectNode();
    schema.put("type", "struct");
    schema.put("schema-id", 0);
    schema.set("fields", fields);
    return schema.toString();
  }

  /**
   * A column's Iceberg type, or {@code null} when it has none.
   *
   * <p>Prefers {@code type_json}, which carries the nested shape for a struct, array or map;
   * without it, only the declared primitive name is available. Nested ids are drawn from the same
   * counter as the top-level fields, because Iceberg requires them unique across the whole schema
   * rather than per level.
   */
  static JsonNode columnType(UnityCatalogTable.Column column, int[] nextId) {
    JsonNode declared = typeFromJson(column.typeJson());
    JsonNode fromJson = declared == null ? null : icebergTypeNode(declared, nextId);
    if (fromJson != null) {
      return fromJson;
    }
    String primitive = icebergType(column);
    return primitive == null ? null : JSON.getNodeFactory().textNode(primitive);
  }

  /**
   * Translates one Spark/Delta type node into Iceberg's form, recursively.
   *
   * <p>The three container shapes each name their members differently, and Iceberg gives every
   * member its own id: a list has one element, a map has a key and a value, a struct has fields. A
   * map key is always required in Iceberg, so Spark has no corresponding flag to carry.
   *
   * <p>{@code null} anywhere below makes the whole type unmappable. Substituting something for a
   * member that could not be translated would produce a schema that parses and misreports the
   * column, which is worse than declining it.
   */
  private static JsonNode icebergTypeNode(JsonNode sparkType, int[] nextId) {
    if (sparkType == null || sparkType.isNull()) {
      return null;
    }
    if (sparkType.isTextual()) {
      String primitive = icebergPrimitive(sparkType.asText());
      return primitive == null ? null : JSON.getNodeFactory().textNode(primitive);
    }
    if (!sparkType.isObject()) {
      return null;
    }
    return switch (sparkType.path("type").asText("")) {
      case "struct" -> icebergStruct(sparkType, nextId);
      case "array" -> icebergList(sparkType, nextId);
      case "map" -> icebergMap(sparkType, nextId);
      default -> null;
    };
  }

  private static JsonNode icebergStruct(JsonNode sparkType, int[] nextId) {
    var fields = JSON.createArrayNode();
    for (JsonNode member : sparkType.path("fields")) {
      if (!member.isObject()) {
        return null;
      }
      int id = nextId[0]++;
      JsonNode type = icebergTypeNode(member.path("type"), nextId);
      if (type == null) {
        return null;
      }
      var field = JSON.createObjectNode();
      field.put("id", id);
      field.put("name", member.path("name").asText(""));
      field.put("required", !member.path("nullable").asBoolean(true));
      field.set("type", type);
      fields.add(field);
    }
    var struct = JSON.createObjectNode();
    struct.put("type", "struct");
    struct.set("fields", fields);
    return struct;
  }

  private static JsonNode icebergList(JsonNode sparkType, int[] nextId) {
    int elementId = nextId[0]++;
    JsonNode element = icebergTypeNode(sparkType.path("elementType"), nextId);
    if (element == null) {
      return null;
    }
    var list = JSON.createObjectNode();
    list.put("type", "list");
    list.put("element-id", elementId);
    list.put("element-required", !sparkType.path("containsNull").asBoolean(true));
    list.set("element", element);
    return list;
  }

  private static JsonNode icebergMap(JsonNode sparkType, int[] nextId) {
    int keyId = nextId[0]++;
    int valueId = nextId[0]++;
    JsonNode key = icebergTypeNode(sparkType.path("keyType"), nextId);
    JsonNode value = icebergTypeNode(sparkType.path("valueType"), nextId);
    if (key == null || value == null) {
      return null;
    }
    var map = JSON.createObjectNode();
    map.put("type", "map");
    map.put("key-id", keyId);
    map.set("key", key);
    map.put("value-id", valueId);
    map.put("value-required", !sparkType.path("valueContainsNull").asBoolean(true));
    map.set("value", value);
    return map;
  }

  /** The Iceberg name for a Spark type-JSON primitive, which spells these differently from UC. */
  private static String icebergPrimitive(String sparkType) {
    String name = sparkType == null ? "" : sparkType.trim().toLowerCase(Locale.ROOT);
    return switch (name) {
      case "boolean" -> "boolean";
      case "byte", "short", "integer", "int" -> "int";
      case "long" -> "long";
      case "float" -> "float";
      case "double" -> "double";
      case "date" -> "date";
      case "timestamp" -> "timestamptz";
      case "timestamp_ntz" -> "timestamp";
      case "string" -> "string";
      case "binary" -> "binary";
      default -> name.startsWith("decimal") ? decimalType(name) : null;
    };
  }

  /**
   * The Iceberg primitive name for a Unity column, or {@code null} when there is not one.
   *
   * <p>Iceberg has no byte or short, so both widen to {@code int}, which cannot lose a value.
   * Delta's {@code TIMESTAMP} is an instant, which is Iceberg's {@code timestamptz}, while {@code
   * TIMESTAMP_NTZ} is the local-time {@code timestamp}; swapping those two shifts every value by
   * the session offset without erroring. Decimal carries precision and scale, which only {@code
   * type_text} states.
   */
  static String icebergType(UnityCatalogTable.Column column) {
    String name =
        column.typeName() == null ? "" : column.typeName().trim().toUpperCase(Locale.ROOT);
    return switch (name) {
      case "BOOLEAN" -> "boolean";
      case "BYTE", "TINYINT", "SHORT", "SMALLINT", "INT", "INTEGER" -> "int";
      case "LONG", "BIGINT" -> "long";
      case "FLOAT", "REAL" -> "float";
      case "DOUBLE" -> "double";
      case "DATE" -> "date";
      case "TIMESTAMP" -> "timestamptz";
      case "TIMESTAMP_NTZ" -> "timestamp";
      case "STRING", "VARCHAR", "CHAR" -> "string";
      case "BINARY" -> "binary";
      case "DECIMAL", "DEC", "NUMERIC" -> decimalType(column.typeText());
      default -> null;
    };
  }

  /** {@code decimal(p,s)} from the declared text; without both, Iceberg cannot type the column. */
  private static String decimalType(String typeText) {
    if (typeText == null) {
      return null;
    }
    var matcher = DECIMAL_TYPE.matcher(typeText.trim());
    return matcher.matches() ? "decimal(" + matcher.group(1) + ", " + matcher.group(2) + ")" : null;
  }

  private static final Pattern DECIMAL_TYPE =
      Pattern.compile("(?i)^decimal\\s*\\(\\s*(\\d{1,2})\\s*,\\s*(\\d{1,2})\\s*\\)$");

  /**
   * Both load paths funnel through a schema builder, so the guard belongs here rather than twice.
   */
  private static void requireColumns(CatalogObjectName name, UnityCatalogTable table, String kind) {
    if (table.columns().isEmpty()) {
      // Refused per object rather than published as an empty schema. parseColumns treats an absent
      // or JSON-null "columns" as an empty list even in strict mode -- only a malformed one raises
      // -- and nothing downstream catches the result: IcebergSchemaMapper.map returns a descriptor
      // with zero columns rather than throwing, so the reconciler persists a relation with no
      // output columns and reports it created. UNSUPPORTED rather than the INVALID_RESPONSE this
      // shape suggests, because INVALID_RESPONSE translates to INTERNAL, which is not a per-object
      // skip: it would abort the whole overlay for one unreadable relation.
      throw new CatalogAccessException(
          CatalogAccessException.Code.UNSUPPORTED,
          "Unity Catalog %s exposes no columns: %s".formatted(kind, fullName(name)));
    }
  }

  private static String schemaJson(CatalogObjectName name, UnityCatalogTable table) {
    requireColumns(name, table, "table");
    var fields = JSON.createArrayNode();
    for (UnityCatalogTable.Column column : table.columns()) {
      // The field Unity sent, kept whole where it parses, rather than three keys copied out of it.
      // Its "metadata" carries delta.columnMapping.id and delta.columnMapping.physicalName on a
      // column-mapped table -- the ordinary shape for a managed Unity table -- and those are read
      // downstream: DeltaSchemaMapper takes the id as the field id, and
      // DeltaSchemaNormalizer.defaultNameMappingJson builds the logical-to-physical Parquet name
      // mapping from the physical name. Rebuilding the field dropped both, so a renamed or
      // column-mapped column resolved against the wrong Parquet column or not at all.
      ObjectNode parsed = fieldFromJson(column.typeJson());
      var field = JSON.createObjectNode();
      // Name and nullability come from the column record, not from type_json: those are what the
      // listing and the detail read agree on, and a type_json that disagrees is not the authority
      // on them. Written in a fixed order so the emitted schema does not depend on how the
      // workspace happened to order its keys.
      field.put("name", column.name());
      if (parsed == null) {
        String declared = column.typeText() != null ? column.typeText() : column.typeName();
        if (declared == null || declared.isBlank()) {
          // Refused rather than published as an empty type, which is the same policy viewSchemaJson
          // applies to a type it cannot represent: a schema that parses and lies is worse than one
          // that is missing, because the failure then surfaces in DeltaSchemaMapper or at scan time
          // with nothing pointing back at the upstream column. Defensive -- Unity rejects a column
          // with no type_json on create -- but this is the one place that would let such a column
          // through.
          throw new CatalogAccessException(
              CatalogAccessException.Code.UNSUPPORTED,
              "Unity Catalog column states no type: column="
                  + column.name()
                  + " table="
                  + fullName(name));
        }
        field.put("type", declared);
      } else {
        field.set("type", parsed.get("type"));
      }
      field.put("nullable", column.nullable());
      if (parsed != null) {
        parsed
            .properties()
            .forEach(
                entry -> {
                  if (!field.has(entry.getKey())) {
                    field.set(entry.getKey(), entry.getValue());
                  }
                });
      }
      fields.add(field);
    }
    var schema = JSON.createObjectNode();
    schema.put("type", "struct");
    schema.set("fields", fields);
    return schema.toString();
  }

  /**
   * Unity's {@code type_json} as an object, when it is one that carries a type.
   *
   * <p>Returned whole so anything beside the type -- {@code metadata} above all -- survives into
   * the schema this provider publishes. A copy, so mutating it cannot reach the parsed tree.
   */
  private static ObjectNode fieldFromJson(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      JsonNode parsed = JSON.readTree(value);
      if (!parsed.isObject()) {
        return null;
      }
      JsonNode type = parsed.get("type");
      if (type == null || type.isNull()) {
        return null;
      }
      return ((ObjectNode) parsed).deepCopy();
    } catch (Exception notParseable) {
      return null;
    }
  }

  private static JsonNode typeFromJson(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      JsonNode type = JSON.readTree(value).get("type");
      return type == null || type.isNull() ? null : type;
    } catch (Exception ignored) {
      return null;
    }
  }

  /**
   * Mirrors {@code FloecatConnector.VendedStorageCredentials.MAX_EXPIRY_EPOCH_MILLIS}: the last
   * instant a proto Timestamp can carry, 9999-12-31T23:59:59.999Z. Duplicated for the same reason
   * {@code IcebergRestCatalogClient} duplicates it -- this module depends on the catalog-access
   * SPI, not the connector one -- and cheaply, because it is fixed by the proto specification
   * rather than a policy either copy owns.
   */
  private static final long MAX_EXPIRY_EPOCH_MILLIS = 253402300799999L;

  private static Optional<Instant> parseExpiry(String value) {
    String normalized = nonBlank(value);
    if (normalized == null) {
      return Optional.empty();
    }
    try {
      long epochMillis = Long.parseLong(normalized);
      // Both bounds, matching the other two parsers of this same field. A non-positive value is an
      // absent expiry, not an instant in 1970 that every consumer reads as already expired; a value
      // past MAX_EXPIRY_EPOCH_MILLIS is a unit mismatch -- Unity deployments have been seen to
      // report expiration_time in microseconds -- not a date. Without the upper bound it survives
      // every downstream expiry check by looking far in the future and then throws inside
      // Timestamps.fromMillis, where the failure is an unrecognized RuntimeException in a gRPC
      // handler and is retried rather than reported.
      return epochMillis <= 0 || epochMillis > MAX_EXPIRY_EPOCH_MILLIS
          ? Optional.empty()
          : Optional.of(Instant.ofEpochMilli(epochMillis));
    } catch (NumberFormatException ignored) {
      return Optional.empty();
    }
  }

  private static Optional<String> optional(String value) {
    return Optional.ofNullable(nonBlank(value));
  }

  private static String nonBlank(String value) {
    return value == null || value.isBlank() ? null : value.trim();
  }

  private static void put(Map<String, String> target, String key, String value) {
    String normalized = nonBlank(value);
    if (normalized != null) {
      target.put(key, normalized);
    }
  }

  private static void closeQuietly(AutoCloseable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Exception ignored) {
    }
  }

  private record Namespace(String catalog, String schema) {}
}
