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

package ai.floedb.floecat.catalog.iceberg.rest;

import ai.floedb.floecat.catalog.access.CatalogCapabilities;
import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.CatalogTable;
import ai.floedb.floecat.catalog.access.CatalogView;
import ai.floedb.floecat.catalog.access.CatalogViewDefinition;
import ai.floedb.floecat.catalog.access.ExternalObjectIdentity;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.StorageLocations;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewVersion;

final class IcebergRestCatalogClient implements CatalogClient {
  private static final Logger LOG = Logger.getLogger(IcebergRestCatalogClient.class.getName());

  private static final List<String> VENDED_STORAGE_KEYS =
      List.of(
          "s3.access-key-id",
          "s3.secret-access-key",
          "s3.session-token",
          "s3.region",
          "client.region",
          "s3.endpoint",
          "s3.path-style-access");

  /**
   * Non-secret routing carried out of the catalog's own answer.
   *
   * <p>{@code client.region} belongs here as well as {@code s3.region}: it is Iceberg's own AWS
   * region property, so a catalog is free to report its region under that name in the {@code
   * /v1/config} response, and the consumer already reads either spelling. Dropping it left the
   * region at floecat's configured default, which signs storage requests for the wrong region -- a
   * signing failure if the caller is lucky, and a read against an unintended region if not.
   */
  private static final List<String> STORAGE_ROUTING_KEYS =
      List.of("s3.region", "client.region", "s3.endpoint", "s3.path-style-access");

  private static final String VENDED_EXPIRY_KEY = "s3.session-token-expires-at-ms";
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

  private final Catalog catalog;
  private final SupportsNamespaces namespaceCatalog;
  private final ViewCatalog viewCatalog;
  private final Runnable closeHook;
  private final Map<String, String> storageRoutingProperties;
  private final Function<VendedStorageCredentials, FileIO> validationFileIoFactory;

  /**
   * Used only to prefer a live session over an expired one when ranking candidates.
   *
   * <p>Deliberately not used to refuse anything. The consumer owns the expiry policy -- its skew
   * tolerance, and which paths may read a just-expired credential -- so a provider that filtered on
   * liveness would refuse credentials the consumer would have read, and the two ends would disagree
   * about the same rule. Package-visible so a test can fix it.
   */
  java.time.Clock clock = java.time.Clock.systemUTC();

  private final AtomicBoolean closed = new AtomicBoolean(false);

  IcebergRestCatalogClient(
      Catalog catalog,
      SupportsNamespaces namespaceCatalog,
      ViewCatalog viewCatalog,
      Runnable closeHook) {
    this(catalog, namespaceCatalog, viewCatalog, closeHook, Map.of());
  }

  IcebergRestCatalogClient(
      Catalog catalog,
      SupportsNamespaces namespaceCatalog,
      ViewCatalog viewCatalog,
      Runnable closeHook,
      Map<String, String> storageRoutingProperties) {
    this(
        catalog,
        namespaceCatalog,
        viewCatalog,
        closeHook,
        storageRoutingProperties,
        IcebergRestCatalogClient::validationFileIo);
  }

  IcebergRestCatalogClient(
      Catalog catalog,
      SupportsNamespaces namespaceCatalog,
      ViewCatalog viewCatalog,
      Runnable closeHook,
      Function<VendedStorageCredentials, FileIO> validationFileIoFactory) {
    this(catalog, namespaceCatalog, viewCatalog, closeHook, Map.of(), validationFileIoFactory);
  }

  IcebergRestCatalogClient(
      Catalog catalog,
      SupportsNamespaces namespaceCatalog,
      ViewCatalog viewCatalog,
      Runnable closeHook,
      Map<String, String> storageRoutingProperties,
      Function<VendedStorageCredentials, FileIO> validationFileIoFactory) {
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.namespaceCatalog = Objects.requireNonNull(namespaceCatalog, "namespaceCatalog");
    this.viewCatalog = Objects.requireNonNull(viewCatalog, "viewCatalog");
    this.closeHook = Objects.requireNonNull(closeHook, "closeHook");
    this.storageRoutingProperties =
        Map.copyOf(Objects.requireNonNull(storageRoutingProperties, "storageRoutingProperties"));
    this.validationFileIoFactory =
        Objects.requireNonNull(validationFileIoFactory, "validationFileIoFactory");
  }

  @Override
  public CatalogCapabilities capabilities() {
    return CAPABILITIES;
  }

  @Override
  public void validate() {
    IcebergRestCatalogErrors.run(
        "connection validation", () -> namespaceCatalog.listNamespaces(Namespace.empty()));
  }

  @Override
  public List<NamespacePath> listNamespaces(NamespacePath parent) {
    Objects.requireNonNull(parent, "parent");
    return IcebergRestCatalogErrors.call(
        "namespace listing",
        () ->
            namespaceCatalog.listNamespaces(toIcebergNamespace(parent)).stream()
                .map(IcebergRestCatalogClient::fromIcebergNamespace)
                .sorted()
                .toList());
  }

  @Override
  public List<CatalogObjectName> listTables(NamespacePath namespace) {
    Objects.requireNonNull(namespace, "namespace");
    return IcebergRestCatalogErrors.call(
        "table listing",
        () ->
            catalog.listTables(toIcebergNamespace(namespace)).stream()
                .map(IcebergRestCatalogClient::fromTableIdentifier)
                .sorted()
                .toList());
  }

  @Override
  public CatalogTable loadTable(CatalogObjectName name) {
    Objects.requireNonNull(name, "name");
    return IcebergRestCatalogErrors.call(
        "table loading",
        () -> {
          Table table = catalog.loadTable(toTableIdentifier(name));
          TableMetadata metadata = tableMetadata(table);
          return new CatalogTable(
              name,
              externalIdentity(name, metadata),
              "ICEBERG",
              SchemaParser.toJson(table.schema()),
              table.spec().fields().stream().map(field -> field.name()).toList(),
              metadataLocation(metadata),
              Optional.ofNullable(table.location()).filter(location -> !location.isBlank()),
              table.properties());
        });
  }

  @Override
  public List<CatalogObjectName> listViews(NamespacePath namespace) {
    Objects.requireNonNull(namespace, "namespace");
    return IcebergRestCatalogErrors.call(
        "view listing",
        () ->
            viewCatalog.listViews(toIcebergNamespace(namespace)).stream()
                .map(IcebergRestCatalogClient::fromTableIdentifier)
                .sorted()
                .toList());
  }

  @Override
  public CatalogView loadView(CatalogObjectName name) {
    Objects.requireNonNull(name, "name");
    return IcebergRestCatalogErrors.call(
        "view loading",
        () -> {
          View view = viewCatalog.loadView(toTableIdentifier(name));
          ViewVersion currentVersion =
              Objects.requireNonNull(view.currentVersion(), "currentVersion");
          Namespace defaultNamespace = currentVersion.defaultNamespace();
          List<CatalogViewDefinition> definitions =
              currentVersion.representations().stream()
                  .filter(SQLViewRepresentation.class::isInstance)
                  .map(SQLViewRepresentation.class::cast)
                  .map(
                      representation ->
                          new CatalogViewDefinition(representation.sql(), representation.dialect()))
                  .toList();
          return new CatalogView(
              name,
              Optional.ofNullable(view.uuid())
                  .map(Object::toString)
                  .map(ExternalObjectIdentity::stable)
                  .orElseGet(() -> ExternalObjectIdentity.pathFallback(name)),
              SchemaParser.toJson(view.schema()),
              definitions,
              defaultNamespace == null ? name.namespace() : fromIcebergNamespace(defaultNamespace),
              view.properties());
        });
  }

  /**
   * {@inheritDoc}
   *
   * <p>Credentials come from {@code SupportsStorageCredentials.credentials()} and nowhere else.
   * There is deliberately no fall-back to the table's FileIO properties, which is what {@code
   * IcebergConnector} does for a catalog predating the credential channel: those properties cannot
   * say whether a key was vended for this table or merged into every table's FileIO from the
   * client's own configuration, which is why the connector path has to cross-check what it finds
   * against what the connector was configured with. A Catalog Integration has no configured storage
   * credential to cross-check against, and the ambiguity would be resolved by guessing. A server
   * that answers access delegation by putting keys in the load-table config map rather than the
   * credentials list therefore does not vend through this path.
   */
  @Override
  public Optional<VendedStorageCredentials> vendStorageCredentials(CatalogObjectName name) {
    Objects.requireNonNull(name, "name");
    return IcebergRestCatalogErrors.call(
        "storage credential vending", () -> vendStorageCredentialsUnchecked(name));
  }

  private Optional<VendedStorageCredentials> vendStorageCredentialsUnchecked(
      CatalogObjectName name) {
    Table table = catalog.loadTable(toTableIdentifier(name));
    if (!(table.io() instanceof SupportsStorageCredentials credentialSource)) {
      return Optional.empty();
    }
    List<StorageCredential> credentials = credentialSource.credentials();
    if (credentials == null || credentials.isEmpty()) {
      return Optional.empty();
    }

    String tableLocation = Optional.ofNullable(table.location()).orElse("");
    // Sampled once for the whole pass. Read per candidate, two credentials carrying the same expiry
    // could be ranked against different instants and land on opposite sides of it.
    Instant rankedAt = clock.instant();
    StorageCredential selected = null;
    int selectedPrefixLength = -1;
    StorageCredential covering = null;
    int coveringPrefixLength = -1;
    StorageCredential expired = null;
    int expiredPrefixLength = -1;
    for (StorageCredential candidate : credentials) {
      if (candidate == null || !hasVendedKeyMaterial(candidate.config())) {
        continue;
      }
      String prefix = Optional.ofNullable(candidate.prefix()).orElse("");
      if (!StorageLocations.covers(prefix, tableLocation)) {
        continue;
      }
      // Length of the normalized form, so "most specific wins" is not decided by whether a catalog
      // spelled the same scope with a trailing slash or an s3a:// scheme.
      String normalizedPrefix =
          StorageLocations.stripTrailingSlash(StorageLocations.normalizeScheme(prefix.trim()));
      // The best covering candidate whatever its shape, kept only to describe the response when
      // nothing renewable covers the table. Ranking happens twice rather than once because the two
      // questions are different: which credential to use, and what to say when there is none.
      if (covering == null || normalizedPrefix.length() > coveringPrefixLength) {
        covering = candidate;
        coveringPrefixLength = normalizedPrefix.length();
      }
      // Renewability decides candidacy, not just the final answer. Iceberg returns a list so a
      // catalog can scope per prefix, and during a rotation or a reconfiguration that list can hold
      // a narrow unrenewable pair beside a broad complete session. Ranking on specificity alone
      // picked the narrow one and then refused the whole response, with a usable credential sitting
      // in it.
      if (!missingSessionFields(candidate.config()).isEmpty()) {
        continue;
      }
      // Liveness ranks, it does not exclude. The same rotation leaves a narrow expired session
      // beside a broad live one, and specificity alone picks the expired one -- which the consumer
      // then rejects, failing a read the response could have served. An expired candidate is still
      // kept as the fallback below, because refusing it here would take the skew tolerance and the
      // per-path decisions away from the consumer that owns them.
      Instant candidateExpiry = parseVendedExpiry(candidate.config());
      boolean live = candidateExpiry != null && candidateExpiry.isAfter(rankedAt);
      if (!live) {
        if (expired == null || normalizedPrefix.length() > expiredPrefixLength) {
          expired = candidate;
          expiredPrefixLength = normalizedPrefix.length();
        }
        continue;
      }
      if (selected == null || normalizedPrefix.length() > selectedPrefixLength) {
        selected = candidate;
        selectedPrefixLength = normalizedPrefix.length();
      }
    }
    // Nothing live covers the table, but something expired does: hand that one on so the consumer
    // reports the expiry with its own rules rather than seeing "no credentials at all".
    if (selected == null && expired != null) {
      selected = expired;
    }
    if (selected == null) {
      // A catalog that answered with key material but not a usable pair is a deterministic fault,
      // not a "did not vend". Returning empty here would hand the caller its no-delegation path and
      // land as a missing-authority error, which reads as a configuration gap and hides the real
      // cause; a retry cannot change it either. There is no opt-in that lets an integration fall
      // back to a storage authority, so the honest answer is to fail with the reason.
      //
      // Restricted to credentials that cover this table: a half tuple offered for some other prefix
      // says nothing about this read, and letting it decide would turn an ordinary "nothing for
      // this table" into a terminal failure.
      Optional<Map<String, String>> partialCredential =
          credentials.stream()
              .filter(Objects::nonNull)
              // No null guard, unlike the scope check above: an absent prefix covers every
              // location by contract, and the selection loop spells the same thing as "". Guarding
              // it here would drop the single unscoped half-tuple -- the clearest case of the fault
              // this check exists to name -- back onto the missing-authority path.
              .filter(candidate -> StorageLocations.covers(candidate.prefix(), tableLocation))
              .map(StorageCredential::config)
              .filter(IcebergRestCatalogClient::hasPartialVendedKeyMaterial)
              .findFirst();
      if (partialCredential.isPresent()) {
        // The fields actually absent, not a fixed pair. A partial credential is any incomplete
        // combination -- a lone session token with neither key is one -- and this message is
        // terminal, so it is the last thing an operator reads. Naming two fields when neither was
        // sent, or when the anomaly is the token, sends them to the wrong end of the response.
        throw new ai.floedb.floecat.catalog.access.CatalogAccessException(
            ai.floedb.floecat.catalog.access.CatalogAccessException.Code.INVALID_CONFIGURATION,
            "Vended storage credentials are incomplete: missing "
                + String.join(", ", missingKeyMaterialFields(partialCredential.get())));
      }
      // Something covers the table, it just cannot be renewed. A catalog integration vends only
      // when the credential it holds is itself a temporary session, and floecat does not scope one
      // down with STS, so what arrives is what would travel to a reader. Reported rather than
      // returned so validation and the vend reach the same answer -- both come through this method,
      // and a bare pair reads storage perfectly well, so accepting it here let an integration pass
      // every validation check and then fail terminally on first use.
      if (covering != null) {
        throw new ai.floedb.floecat.catalog.access.CatalogAccessException(
            ai.floedb.floecat.catalog.access.CatalogAccessException.Code.INVALID_CONFIGURATION,
            "Vended storage credentials are not a renewable session: missing "
                + String.join(", ", missingSessionFields(covering.config())));
      }
      // Scope last, once nothing that covers the table has been found. It describes the response as
      // a whole -- "some credential here is for a different prefix" -- while the two checks above
      // describe the credential meant for this table. When a response holds both, saying the
      // credentials do not cover the table is simply untrue: one of them does, and the operator
      // needs to hear that it is not a session rather than go looking at catalog scoping.
      //
      // The order used to run the other way, when scope-invalid fell back to storage-authority
      // handling and these two were terminal, so an ambiguous response took the recoverable answer.
      // An Integration no longer falls back, so all three are refusals with the same disposition
      // and the order decides nothing but which sentence is printed.
      boolean hasOutOfScopeCredential =
          credentials.stream()
              .filter(Objects::nonNull)
              .filter(candidate -> hasVendedKeyMaterial(candidate.config()))
              .map(StorageCredential::prefix)
              .filter(Objects::nonNull)
              .anyMatch(prefix -> !StorageLocations.covers(prefix, tableLocation));
      if (hasOutOfScopeCredential) {
        throw new ai.floedb.floecat.catalog.access.CatalogAccessException(
            ai.floedb.floecat.catalog.access.CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID,
            "Vended storage credentials do not cover the upstream table location");
      }
      return Optional.empty();
    }

    // Catalog-wide routing first, then whatever this table overrode, then the credential. The
    // middle layer is the one a catalog uses to put a table in a different region or bucket from
    // its own default: RESTSessionCatalog.tableFileIO builds the table's FileIO from
    // RESTUtil.merge(properties(), response.config()), so a LoadTableResponse config reaches
    // table.io() but never reaches the map captured from /v1/config at client construction.
    // Dropping it signed reads for the catalog's region against a table that does not live there.
    Map<String, String> vended = new LinkedHashMap<>(storageRoutingProperties);
    vended.putAll(perTableRoutingProperties(table));
    vended.putAll(filterVendedStorageProperties(selected.config()));
    // Not a classification: selection required both keys non-blank, VENDED_STORAGE_KEYS carries
    // both, the filter keeps every non-blank value, and the routing merge only adds -- so this
    // cannot fire. Kept as an invariant so a future change to any of those three surfaces here
    // rather than as a credential that reaches storage missing half its key material.
    if (!vended.containsKey("s3.access-key-id") || !vended.containsKey("s3.secret-access-key")) {
      // Typed, not an IllegalStateException: IcebergRestCatalogErrors.translate returns anything it
      // does not recognise unwrapped, so a raw one would leave this SPI as an untyped
      // RuntimeException -- escaping CatalogIntegrationDiscovery's catch of CatalogAccessException,
      // where it would propagate out of ValidateCatalogIntegration instead of reporting a failed
      // check, and reaching the vendor as a null accessFailure. The guard exists to be seen, and
      // untyped is the one shape that loses it.
      throw new ai.floedb.floecat.catalog.access.CatalogAccessException(
          ai.floedb.floecat.catalog.access.CatalogAccessException.Code.INTERNAL,
          "Selected vended credential lost its key material during filtering");
    }
    // Non-null by selection, which only ranks candidates whose session fields are all present.
    // Optional.ofNullable rather than Optional.of so a future change to that filter degrades to an
    // absent expiry -- which requireUsableCredentials still refuses -- instead of an NPE here.
    return Optional.of(
        new VendedStorageCredentials(
            Map.copyOf(vended),
            Optional.ofNullable(selected.prefix()).orElse(""),
            Optional.ofNullable(parseVendedExpiry(selected.config()))));
  }

  /** Which half of the key pair a partial credential did not carry. Never empty for one. */
  private static List<String> missingKeyMaterialFields(Map<String, String> config) {
    List<String> missing = new java.util.ArrayList<>();
    if (isBlank(config.get("s3.access-key-id"))) {
      missing.add("s3.access-key-id");
    }
    if (isBlank(config.get("s3.secret-access-key"))) {
      missing.add("s3.secret-access-key");
    }
    return missing;
  }

  /**
   * The renewal fields a vended credential is missing, empty when it is a complete session.
   *
   * <p>Both are required together: the token is what makes the credential a session, and the expiry
   * is what makes it renewable. A credential missing either cannot be re-vended before it lapses,
   * and the reconcile path would embed it statically and read until it did.
   */
  private static List<String> missingSessionFields(Map<String, String> config) {
    List<String> missing = new java.util.ArrayList<>();
    if (isBlank(config.get("s3.session-token"))) {
      missing.add("s3.session-token");
    }
    if (parseVendedExpiry(config) == null) {
      missing.add(VENDED_EXPIRY_KEY);
    }
    return missing;
  }

  @Override
  public void validateStorageAccess(
      CatalogObjectName name, VendedStorageCredentials vendedStorageCredentials) {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(vendedStorageCredentials, "vendedStorageCredentials");
    IcebergRestCatalogErrors.run(
        "storage access validation",
        () -> {
          Table table = catalog.loadTable(toTableIdentifier(name));
          String metadataLocation =
              metadataLocation(tableMetadata(table))
                  .orElseThrow(
                      () ->
                          new ai.floedb.floecat.catalog.access.CatalogAccessException(
                              ai.floedb.floecat.catalog.access.CatalogAccessException.Code
                                  .UNSUPPORTED,
                              "Upstream table does not expose a metadata location"));
          if (!vendedStorageCredentials.covers(metadataLocation)) {
            throw new ai.floedb.floecat.catalog.access.CatalogAccessException(
                ai.floedb.floecat.catalog.access.CatalogAccessException.Code
                    .CREDENTIAL_SCOPE_INVALID,
                "Vended storage credentials do not cover the table metadata location");
          }
          try (FileIO validationIo = validationFileIoFactory.apply(vendedStorageCredentials)) {
            validationIo.newInputFile(metadataLocation).getLength();
          }
        });
  }

  private static FileIO validationFileIo(VendedStorageCredentials credentials) {
    return CatalogUtil.loadFileIO(
        IcebergRestCatalogClientProvider.DEFAULT_S3_FILE_IO, credentials.properties(), null);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      closeHook.run();
    }
  }

  private static Namespace toIcebergNamespace(NamespacePath path) {
    return path.segments().isEmpty()
        ? Namespace.empty()
        : Namespace.of(path.segments().toArray(String[]::new));
  }

  private static NamespacePath fromIcebergNamespace(Namespace namespace) {
    return new NamespacePath(List.of(namespace.levels()));
  }

  private static CatalogObjectName fromTableIdentifier(TableIdentifier identifier) {
    return new CatalogObjectName(fromIcebergNamespace(identifier.namespace()), identifier.name());
  }

  private static TableIdentifier toTableIdentifier(CatalogObjectName name) {
    Namespace namespace = toIcebergNamespace(name.namespace());
    return namespace.isEmpty()
        ? TableIdentifier.of(name.name())
        : TableIdentifier.of(namespace, name.name());
  }

  private static TableMetadata tableMetadata(Table table) {
    if (!(table instanceof HasTableOperations hasOperations)) {
      return null;
    }
    return hasOperations.operations().current();
  }

  private static ExternalObjectIdentity externalIdentity(
      CatalogObjectName name, TableMetadata metadata) {
    return Optional.ofNullable(metadata)
        .map(TableMetadata::uuid)
        .map(String::trim)
        .filter(uuid -> !uuid.isEmpty())
        .map(ExternalObjectIdentity::stable)
        .orElseGet(() -> ExternalObjectIdentity.pathFallback(name));
  }

  private static Optional<String> metadataLocation(TableMetadata metadata) {
    return Optional.ofNullable(metadata)
        .map(TableMetadata::metadataFileLocation)
        .map(String::trim)
        .filter(location -> !location.isEmpty());
  }

  private static Map<String, String> filterVendedStorageProperties(Map<String, String> source) {
    Map<String, String> filtered = new LinkedHashMap<>();
    for (String key : VENDED_STORAGE_KEYS) {
      String value = source.get(key);
      if (value != null && !value.isBlank()) {
        filtered.put(key, value);
      }
    }
    return Map.copyOf(filtered);
  }

  /**
   * Whether a credential carries some S3 key material without carrying a usable pair.
   *
   * <p>Distinguishes a catalog that vended nothing for this table -- a normal answer -- from one
   * that vended half a tuple, which is a fault worth reporting rather than falling back on. A
   * session token on its own counts: it is only meaningful beside the pair.
   */
  private static boolean hasPartialVendedKeyMaterial(Map<String, String> properties) {
    if (properties == null || properties.isEmpty() || hasVendedKeyMaterial(properties)) {
      return false;
    }
    return !isBlank(properties.get("s3.access-key-id"))
        || !isBlank(properties.get("s3.secret-access-key"))
        || !isBlank(properties.get("s3.session-token"));
  }

  private static boolean hasVendedKeyMaterial(Map<String, String> properties) {
    return properties != null
        && !properties.isEmpty()
        && !isBlank(properties.get("s3.access-key-id"))
        && !isBlank(properties.get("s3.secret-access-key"));
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  /**
   * Routing this table overrode, empty when it overrode none.
   *
   * <p>{@code FileIO.properties()} is a default method that throws unless the implementation
   * chooses to expose its configuration -- {@code S3FileIO} does, a custom one need not -- so a
   * refusal here means "no per-table routing to read", not a failure to vend. The catalog-wide map
   * still applies either way, which is what this path had before.
   *
   * <p>Every runtime failure is absorbed, not just the {@code UnsupportedOperationException} the
   * default method declares: an implementation is free to throw something else for the same "I do
   * not expose this" reason, and this runs on the success path with a usable credential already
   * selected. Letting one escape would trade a working vend for a missing region hint -- and for an
   * Integration that is a failed read, because there is no storage authority behind it.
   */
  private static Map<String, String> perTableRoutingProperties(Table table) {
    try {
      Map<String, String> properties = table.io().properties();
      return properties == null ? Map.of() : storageRoutingProperties(properties);
    } catch (RuntimeException e) {
      LOG.log(Level.FINE, "Table FileIO did not expose per-table storage routing", e);
      return Map.of();
    }
  }

  static Map<String, String> storageRoutingProperties(Map<String, String> source) {
    Map<String, String> filtered = new LinkedHashMap<>();
    for (String key : STORAGE_ROUTING_KEYS) {
      String value = source.get(key);
      if (value != null && !value.isBlank()) {
        filtered.put(key, value);
      }
    }
    return Map.copyOf(filtered);
  }

  /**
   * Mirrors {@code FloecatConnector.VendedStorageCredentials.MAX_EXPIRY_EPOCH_MILLIS}: the last
   * instant a proto Timestamp can carry, 9999-12-31T23:59:59.999Z. Duplicated for the same reason
   * the rule below is -- this module cannot reach the connector SPI -- and cheaply, because it is a
   * value fixed by the proto specification rather than a policy either copy owns.
   */
  private static final long MAX_EXPIRY_EPOCH_MILLIS = 253402300799999L;

  /**
   * Mirrors {@code FloecatConnector.VendedStorageCredentials.MIN_EXPIRY_EPOCH_MILLIS},
   * 2000-01-01T00:00:00Z. A value below it is in the wrong unit rather than early: seconds read as
   * milliseconds land in January 1970, deterministically stale, which the refresh path re-vends on
   * every resolveCredentials. Folding it to absent gets it refused once with the field named.
   */
  private static final long MIN_EXPIRY_EPOCH_MILLIS = 946684800000L;

  private static Instant parseVendedExpiry(Map<String, String> properties) {
    String raw = properties.get(VENDED_EXPIRY_KEY);
    if (raw == null || raw.isBlank()) {
      return null;
    }
    try {
      long epochMillis = Long.parseLong(raw.trim());
      // Nearly the rule FloecatConnector.VendedStorageCredentials.expiryFromEpochMillis applies,
      // which this module cannot call: catalog-access depends on the catalog-access SPI, not the
      // connector one. The ceiling is shared. The floor is deliberately only here: folding an
      // out-of-unit value to absent is safe on this path, because a vend that reaches it without a
      // parseable expiry is refused by missingSessionFields with the field named, and nothing here
      // infers permanence from an absent expiry. The connector record cannot afford that -- see the
      // note on expiryFromEpochMillis -- so the asymmetry is the point rather than drift.
      return epochMillis < MIN_EXPIRY_EPOCH_MILLIS || epochMillis > MAX_EXPIRY_EPOCH_MILLIS
          ? null
          : Instant.ofEpochMilli(epochMillis);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }
}
