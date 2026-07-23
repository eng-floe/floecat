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

import ai.floedb.floecat.connector.common.auth.AwsGlueClientFactory;
import ai.floedb.floecat.connector.common.auth.AwsProfileSupport;
import ai.floedb.floecat.connector.common.auth.RefreshingAwsCredentialsProviderRegistry;
import ai.floedb.floecat.connector.common.auth.RegistryBackedAwsCredentialsProvider;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.RESTUtil;
import org.jboss.logging.Logger;

final class IcebergConnectorFactory {
  private static final Logger LOG = Logger.getLogger(IcebergConnectorFactory.class);
  private static final String DEFAULT_S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO";
  private static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";
  private static final String CLIENT_CREDENTIALS_PROVIDER_PREFIX =
      CLIENT_CREDENTIALS_PROVIDER + ".";

  private IcebergConnectorFactory() {}

  enum IcebergSource {
    GLUE,
    REST,
    FILESYSTEM
  }

  static FloecatConnector create(
      String uri,
      Map<String, String> options,
      String authScheme,
      Map<String, String> authProps,
      Map<String, String> headerHints) {

    Objects.requireNonNull(uri, "uri");

    Map<String, String> opts = (options == null) ? Collections.emptyMap() : options;
    Map<String, String> cleanOpts = new HashMap<>(opts);
    IcebergSource source = selectSource(cleanOpts);
    boolean ndvEnabled = IcebergConnector.parseNdvEnabled(cleanOpts);
    double ndvSampleFraction = IcebergConnector.parseNdvSampleFraction(cleanOpts);
    long ndvMaxFiles = 0L;
    try {
      ndvMaxFiles = Long.parseLong(cleanOpts.getOrDefault("stats.ndv.max_files", "0"));
      if (ndvMaxFiles < 0) {
        ndvMaxFiles = 0;
      }
    } catch (NumberFormatException ignore) {
    }
    // stats.ndv.theta_k is consumed by the Java connector path in ParquetNdvProvider.
    int thetaK = 4096;
    try {
      int parsed = Integer.parseInt(cleanOpts.getOrDefault("stats.ndv.theta_k", "4096"));
      if (parsed >= 16) thetaK = parsed;
    } catch (NumberFormatException ignore) {
    }

    validateOptions(source, uri, cleanOpts, authScheme);
    Map<String, String> baseProps = buildBaseIcebergProperties(cleanOpts);
    return switch (source) {
      case FILESYSTEM -> {
        Map<String, String> storageProps = buildStorageProperties(baseProps, authScheme, authProps);
        var loaded = IcebergConnector.loadExternalTable(uri, storageProps);
        Table table = loaded.table();
        FileIO fileIO = loaded.fileIO();
        String namespaceFq = cleanOpts.getOrDefault("external.namespace", "");
        String detectedName =
            (table.name() == null || table.name().isBlank())
                ? IcebergConnector.deriveTableName(uri)
                : table.name();
        String tableName = cleanOpts.getOrDefault("external.table-name", detectedName);
        yield new IcebergFilesystemConnector(
            "iceberg-filesystem",
            table,
            namespaceFq,
            tableName,
            ndvEnabled,
            ndvSampleFraction,
            ndvMaxFiles,
            thetaK,
            fileIO);
      }
      case GLUE, REST -> {
        Map<String, String> catalogProps = buildCatalogProperties(uri, baseProps);
        Map<String, String> storageProps = buildStorageProperties(baseProps, authScheme, authProps);
        applyStorageProperties(catalogProps, storageProps);
        applyCatalogAuth(catalogProps, authScheme, authProps);
        if (headerHints != null) {
          headerHints.forEach((k, v) -> catalogProps.put("header." + k, v));
        }

        catalogProps.putIfAbsent("rest.client.user-agent", "floecat-connector-iceberg");

        RESTSessionCatalog rawCatalog = createRestCatalog(catalogProps);
        Runnable closeCatalog = closeOnce(rawCatalog);
        try {
          SessionCatalog.SessionContext context = SessionCatalog.SessionContext.createEmpty();
          Catalog catalog = rawCatalog.asCatalog(context);
          SupportsNamespaces namespaceCatalog = (SupportsNamespaces) catalog;
          ViewCatalog viewCatalog = rawCatalog.asViewCatalog(context);
          if (source == IcebergSource.GLUE) {
            var glue = AwsGlueClientFactory.createRefreshing(catalogProps, authProps);
            var glueFilter = new GlueIcebergFilter(glue);
            yield new IcebergGlueConnector(
                "iceberg-glue",
                catalog,
                namespaceCatalog,
                viewCatalog,
                catalog,
                glueFilter,
                ndvEnabled,
                ndvSampleFraction,
                ndvMaxFiles,
                closeCatalog);
          }
          yield new IcebergRestConnector(
              "iceberg-rest",
              catalog,
              namespaceCatalog,
              viewCatalog,
              catalog,
              ndvEnabled,
              ndvSampleFraction,
              ndvMaxFiles,
              closeCatalog);
        } catch (RuntimeException | Error e) {
          closeCatalog.run();
          throw e;
        }
      }
    };
  }

  private static RESTSessionCatalog createRestCatalog(Map<String, String> catalogProperties) {
    Map<String, String> catalogProps =
        Collections.unmodifiableMap(new HashMap<>(catalogProperties));
    RESTSessionCatalog catalog =
        new RESTSessionCatalog(
            config ->
                HTTPClient.builder(config)
                    .uri(config.get(CatalogProperties.URI))
                    .withHeaders(RESTUtil.configHeaders(config))
                    .build(),
            null);
    catalog.initialize("floecat-iceberg", catalogProps);
    LOG.infof(
        "created iceberg rest catalog uri=%s warehouse=%s accessDelegationHeader=%s"
            + " headerKeys=%s storageKeyPresent=%s",
        catalogProps.get("uri"),
        catalogProps.get("warehouse"),
        catalogProps.get("header.X-Iceberg-Access-Delegation"),
        headerKeys(catalogProps),
        catalogProps.containsKey("s3.access-key-id"));
    return catalog;
  }

  private static void closeQuietly(RESTSessionCatalog catalog) {
    if (catalog == null) {
      return;
    }
    try {
      catalog.close();
    } catch (Exception ignore) {
    }
  }

  private static Runnable closeOnce(RESTSessionCatalog catalog) {
    AtomicBoolean closed = new AtomicBoolean(false);
    return () -> {
      if (closed.compareAndSet(false, true)) {
        closeQuietly(catalog);
      }
    };
  }

  private static Set<String> headerKeys(Map<String, String> props) {
    return props.keySet().stream()
        .filter(key -> key.startsWith("header."))
        .collect(java.util.stream.Collectors.toCollection(java.util.TreeSet::new));
  }

  static Map<String, String> buildBaseIcebergProperties(Map<String, String> options) {
    Map<String, String> props = new HashMap<>();
    if (options != null && !options.isEmpty()) {
      props.putAll(options);
    }
    normalizeAwsRegionProperties(props);
    return props;
  }

  static Map<String, String> buildCatalogProperties(String uri, Map<String, String> baseProps) {
    Map<String, String> props = new HashMap<>();
    props.put("type", "rest");
    props.put("uri", uri);
    if (baseProps != null && !baseProps.isEmpty()) {
      props.putAll(baseProps);
    }
    String catalogProviderId =
        props.get(RefreshingAwsCredentialsProviderRegistry.CATALOG_OPTION_PROVIDER_ID);
    props.remove(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID);
    props.remove(RefreshingAwsCredentialsProviderRegistry.PROPERTY_PROVIDER_ID);
    props.remove("s3.access-key-id");
    props.remove("s3.secret-access-key");
    props.remove("s3.session-token");
    props.remove(CLIENT_CREDENTIALS_PROVIDER);
    props
        .keySet()
        .removeIf(key -> key != null && key.startsWith(CLIENT_CREDENTIALS_PROVIDER_PREFIX));
    if (!isBlank(catalogProviderId)) {
      props.remove("rest.access-key-id");
      props.remove("rest.secret-access-key");
      props.remove("rest.session-token");
    }
    return props;
  }

  static Map<String, String> buildStorageProperties(
      Map<String, String> baseProps, String authScheme, Map<String, String> authProps) {
    Map<String, String> props = new HashMap<>();
    if (baseProps != null && !baseProps.isEmpty()) {
      props.putAll(baseProps);
    }
    props.remove(RefreshingAwsCredentialsProviderRegistry.CATALOG_OPTION_PROVIDER_ID);
    props.remove("rest.access-key-id");
    props.remove("rest.secret-access-key");
    props.remove("rest.session-token");
    applyStorageAuth(props, authScheme, authProps);
    return props;
  }

  static void applyStorageProperties(
      Map<String, String> catalogProperties, Map<String, String> storageProperties) {
    if (storageProperties != null && !storageProperties.isEmpty()) {
      catalogProperties.putAll(storageProperties);
    }
  }

  static void applyCatalogAuth(
      Map<String, String> props, String authScheme, Map<String, String> authProps) {
    Map<String, String> safeAuthProps = authProps == null ? Collections.emptyMap() : authProps;
    String scheme = (authScheme == null ? "none" : authScheme.trim().toLowerCase(Locale.ROOT));
    switch (scheme) {
      case "aws-sigv4" -> {
        props.remove("rest.sigv4-enabled");
        String signingName = safeAuthProps.getOrDefault("signing-name", "glue");
        String signingRegion =
            safeAuthProps.getOrDefault(
                "signing-region",
                props.getOrDefault(
                    "rest.signing-region", props.getOrDefault("s3.region", "us-east-1")));
        props.put("rest.auth.type", CatalogSigV4AuthManager.class.getName());
        props.put("rest.signing-name", signingName);
        props.put("rest.signing-region", signingRegion);
      }

      case "oauth2" -> {
        String token =
            Objects.requireNonNull(
                safeAuthProps.get("token"), "authProps.token required for oauth2");
        props.put("rest.auth.type", "oauth2");
        props.put("token", token);
        String oauth2ServerUri = safeAuthProps.get("oauth2-server-uri");
        if (!isBlank(oauth2ServerUri)) {
          props.put("oauth2-server-uri", oauth2ServerUri);
        }
      }

      case "none" -> {}

      default -> throw new IllegalArgumentException("Unsupported auth scheme: " + authScheme);
    }
  }

  static void applyStorageAuth(
      Map<String, String> props, String authScheme, Map<String, String> authProps) {
    String refreshProviderId =
        props.get(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID);
    if (!isBlank(refreshProviderId)) {
      props.putIfAbsent("io-impl", DEFAULT_S3_FILE_IO);
      props.put(CLIENT_CREDENTIALS_PROVIDER, RegistryBackedAwsCredentialsProvider.class.getName());
      props.put(
          CLIENT_CREDENTIALS_PROVIDER_PREFIX
              + RefreshingAwsCredentialsProviderRegistry.PROPERTY_PROVIDER_ID,
          refreshProviderId);
      props.put(
          CLIENT_CREDENTIALS_PROVIDER_PREFIX
              + RefreshingAwsCredentialsProviderRegistry.PROPERTY_CREDENTIAL_SCOPE,
          "storage");
      props.remove("s3.access-key-id");
      props.remove("s3.secret-access-key");
      props.remove("s3.session-token");
      normalizeAwsRegionProperties(props);
      return;
    }
    Map<String, String> safeAuthProps = authProps == null ? Collections.emptyMap() : authProps;
    String scheme = (authScheme == null ? "none" : authScheme.trim().toLowerCase(Locale.ROOT));
    switch (scheme) {
      case "aws":
      case "aws-assume-role":
      case "aws-web-identity":
      case "aws-sigv4":
        AwsProfileSupport.applyProfileProperties(props, safeAuthProps);
        props.putIfAbsent("io-impl", DEFAULT_S3_FILE_IO);
        if ("aws-sigv4".equals(scheme)) {
          String signingRegion =
              safeAuthProps.getOrDefault(
                  "signing-region",
                  props.getOrDefault(
                      "rest.signing-region",
                      props.getOrDefault(
                          "client.region", props.getOrDefault("s3.region", "us-east-1"))));
          props.putIfAbsent("s3.region", signingRegion);
          props.putIfAbsent("client.region", signingRegion);
        }
        normalizeAwsRegionProperties(props);
        break;
      case "none":
      case "oauth2":
        normalizeAwsRegionProperties(props);
        break;
      default:
        normalizeAwsRegionProperties(props);
        break;
    }
  }

  private static void normalizeAwsRegionProperties(Map<String, String> props) {
    if (props.isEmpty()) {
      return;
    }
    String region = props.get("client.region");
    if (region == null || region.isBlank()) {
      region = props.get("s3.region");
    }
    if (region == null || region.isBlank()) {
      region = props.get("aws.region");
    }
    if (region == null || region.isBlank()) {
      return;
    }
    props.putIfAbsent("client.region", region);
    props.putIfAbsent("s3.region", region);
  }

  static IcebergSource selectSource(Map<String, String> options) {
    if (options == null) {
      return IcebergSource.GLUE;
    }
    String source = options.get("iceberg.source");
    if (source != null && !source.isBlank()) {
      String normalized = source.trim().toLowerCase(Locale.ROOT);
      return switch (normalized) {
        case "glue" -> IcebergSource.GLUE;
        case "rest" -> IcebergSource.REST;
        case "filesystem" -> IcebergSource.FILESYSTEM;
        default -> throw new IllegalArgumentException("Unsupported iceberg.source: " + source);
      };
    }
    return IcebergSource.GLUE;
  }

  static void validateOptions(
      IcebergSource source, String uri, Map<String, String> options, String authScheme) {
    boolean hasFilesystemUri = uri != null && !uri.isBlank();
    if (source == IcebergSource.FILESYSTEM && !hasFilesystemUri) {
      throw new IllegalArgumentException("uri is required for iceberg.source=filesystem");
    }
    if (source != IcebergSource.FILESYSTEM || !isS3Uri(uri)) {
      return;
    }
    if (!isSupportedFilesystemStorageScheme(authScheme)) {
      throw new IllegalArgumentException(
          "Unsupported filesystem storage auth scheme for S3 URI: " + authScheme);
    }
  }

  private static boolean isS3Uri(String uri) {
    return uri != null && uri.trim().toLowerCase(Locale.ROOT).startsWith("s3://");
  }

  private static boolean isSupportedFilesystemStorageScheme(String authScheme) {
    String scheme = authScheme == null ? "none" : authScheme.trim().toLowerCase(Locale.ROOT);
    return switch (scheme) {
      case "none", "aws", "aws-assume-role", "aws-web-identity", "aws-sigv4" -> true;
      default -> false;
    };
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
