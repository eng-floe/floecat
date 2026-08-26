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
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
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
  private static final List<String> VENDED_STORAGE_KEYS =
      List.of(
          "s3.access-key-id",
          "s3.secret-access-key",
          "s3.session-token",
          "s3.region",
          "s3.endpoint",
          "s3.path-style-access");
  private static final List<String> STORAGE_ROUTING_KEYS =
      List.of("s3.region", "s3.endpoint", "s3.path-style-access");
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
    StorageCredential selected = null;
    int selectedPrefixLength = -1;
    for (StorageCredential candidate : credentials) {
      if (candidate == null || !hasVendedKeyMaterial(candidate.config())) {
        continue;
      }
      String prefix = Optional.ofNullable(candidate.prefix()).orElse("");
      String normalizedPrefix = normalizeS3Scheme(prefix);
      if (!coversLocation(tableLocation, normalizedPrefix)) {
        continue;
      }
      if (selected == null || normalizedPrefix.length() > selectedPrefixLength) {
        selected = candidate;
        selectedPrefixLength = normalizedPrefix.length();
      }
    }
    if (selected == null) {
      boolean hasOutOfScopeCredential =
          credentials.stream()
              .filter(Objects::nonNull)
              .filter(candidate -> hasVendedKeyMaterial(candidate.config()))
              .map(StorageCredential::prefix)
              .filter(Objects::nonNull)
              .anyMatch(prefix -> !coversLocation(tableLocation, prefix));
      if (hasOutOfScopeCredential) {
        throw new ai.floedb.floecat.catalog.access.CatalogAccessException(
            ai.floedb.floecat.catalog.access.CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID,
            "Vended storage credentials do not cover the upstream table location");
      }
      return Optional.empty();
    }

    Map<String, String> vended = new LinkedHashMap<>(storageRoutingProperties);
    vended.putAll(filterVendedStorageProperties(selected.config()));
    if (!vended.containsKey("s3.access-key-id") || !vended.containsKey("s3.secret-access-key")) {
      return Optional.empty();
    }
    return Optional.of(
        new VendedStorageCredentials(
            Map.copyOf(vended),
            Optional.ofNullable(selected.prefix()).orElse(""),
            Optional.ofNullable(parseVendedExpiry(selected.config()))));
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
          String scopePrefix = vendedStorageCredentials.scopePrefix();
          if (!coversLocation(metadataLocation, scopePrefix)) {
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

  private static boolean hasVendedKeyMaterial(Map<String, String> properties) {
    return properties != null
        && !properties.isEmpty()
        && !isBlank(properties.get("s3.access-key-id"))
        && !isBlank(properties.get("s3.secret-access-key"));
  }

  private static String normalizeS3Scheme(String location) {
    int schemeEnd = location.indexOf("://");
    if (schemeEnd < 0) {
      return location;
    }
    String scheme = location.substring(0, schemeEnd);
    if ("s3".equalsIgnoreCase(scheme)
        || "s3a".equalsIgnoreCase(scheme)
        || "s3n".equalsIgnoreCase(scheme)) {
      return "s3" + location.substring(schemeEnd);
    }
    return location;
  }

  private static boolean coversLocation(String location, String prefix) {
    return prefix.isEmpty() || normalizeS3Scheme(location).startsWith(normalizeS3Scheme(prefix));
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
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

  private static Instant parseVendedExpiry(Map<String, String> properties) {
    String raw = properties.get(VENDED_EXPIRY_KEY);
    if (raw == null || raw.isBlank()) {
      return null;
    }
    try {
      return Instant.ofEpochMilli(Long.parseLong(raw.trim()));
    } catch (NumberFormatException ignored) {
      return null;
    }
  }
}
