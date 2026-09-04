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

import ai.floedb.floecat.aws.RefreshingAwsClient;
import ai.floedb.floecat.client.unity.HttpUnityCatalogClient;
import ai.floedb.floecat.connector.common.auth.AwsGlueClientFactory;
import ai.floedb.floecat.connector.common.auth.RefreshingAwsCredentialsProviderRegistry;
import ai.floedb.floecat.connector.common.auth.RegistryBackedAwsCredentialsProvider;
import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.parquet.io.InputFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

final class DeltaConnectorFactory {
  static final String DELTA_SOURCE_OPTION = "delta.source";

  private static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";
  private static final String CLIENT_CREDENTIALS_PROVIDER_PREFIX =
      CLIENT_CREDENTIALS_PROVIDER + ".";

  private DeltaConnectorFactory() {}

  enum DeltaSource {
    UNITY,
    GLUE,
    FILESYSTEM
  }

  static FloecatConnector create(
      String uri,
      Map<String, String> options,
      AuthProvider authProvider,
      Map<String, String> authProps) {
    Map<String, String> opts = (options == null) ? Collections.emptyMap() : options;
    Map<String, String> effectiveOptions = new LinkedHashMap<>(opts);
    Map<String, String> effectiveAuthProps = authProps == null ? Map.of() : authProps;

    DeltaSource source = selectSource(effectiveOptions);
    try {
      return createForSource(uri, effectiveOptions, authProvider, effectiveAuthProps, source);
    } catch (RuntimeException | Error failure) {
      if (source == DeltaSource.UNITY) {
        closeAfterFailedConstruction(null, authProvider, failure);
      }
      throw failure;
    }
  }

  private static FloecatConnector createForSource(
      String uri,
      Map<String, String> effectiveOptions,
      AuthProvider authProvider,
      Map<String, String> effectiveAuthProps,
      DeltaSource source) {
    String storageLocation = effectiveOptions.getOrDefault("delta.table-root", "");
    validateOptions(source, storageLocation);

    EngineContext engineContext = buildEngine(effectiveOptions);
    boolean ndvEnabled =
        Boolean.parseBoolean(effectiveOptions.getOrDefault("stats.ndv.enabled", "false"));

    double ndvSampleFraction = 1.0;
    try {
      ndvSampleFraction =
          Double.parseDouble(effectiveOptions.getOrDefault("stats.ndv.sample_fraction", "1.0"));
      if (ndvSampleFraction <= 0.0 || ndvSampleFraction > 1.0) {
        ndvSampleFraction = 1.0;
      }
    } catch (NumberFormatException ignore) {
    }

    long ndvMaxFiles = 0L;
    try {
      ndvMaxFiles = Long.parseLong(effectiveOptions.getOrDefault("stats.ndv.max_files", "0"));
      if (ndvMaxFiles < 0) ndvMaxFiles = 0;
    } catch (NumberFormatException ignore) {
    }

    // buildEngine ran above and its refreshing client is released only by
    // DeltaConnector.close(), which never runs if construction throws below.
    try {
      return switch (source) {
        case FILESYSTEM -> {
          String namespaceFq = effectiveOptions.getOrDefault("external.namespace", "");
          String tableName =
              effectiveOptions.getOrDefault(
                  "external.table-name", deriveTableName(storageLocation));
          yield new DeltaFilesystemConnector(
              "delta-filesystem",
              engineContext.engine(),
              engineContext.parquetInput(),
              ndvEnabled,
              ndvSampleFraction,
              ndvMaxFiles,
              storageLocation,
              namespaceFq,
              tableName,
              engineContext.engineResources());
        }
        case GLUE -> {
          Map<String, String> catalogOptions = buildGlueCatalogOptions(effectiveOptions);
          var glue = AwsGlueClientFactory.createRefreshing(catalogOptions, effectiveAuthProps);
          yield new DeltaGlueConnector(
              "delta-glue",
              new GlueDeltaCatalog(glue),
              engineContext.engine(),
              engineContext.parquetInput(),
              ndvEnabled,
              ndvSampleFraction,
              ndvMaxFiles,
              engineContext.engineResources());
        }
        case UNITY -> {
          Objects.requireNonNull(uri, "Unity base uri");
          String host = uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
          int connectMs = positiveIntOption(effectiveOptions, "http.connect.ms", 10_000);
          int readMs = positiveIntOption(effectiveOptions, "http.read.ms", 60_000);
          HttpUnityCatalogClient uc = null;
          try {
            // Databricks serves vending under 2.0 and OSS Unity Catalog under 2.1; everything else
            // is 2.1 on both. Defaulting to Databricks keeps the supported target working with no
            // configuration, while an OSS or proxied endpoint can name its own route.
            // Blank reads as unset, matching positiveIntOption above: an empty string is how config
            // UIs and persisted-property round-trips commonly encode "not set".
            String configuredVendPath = effectiveOptions.get("unity.temporary-table-vend-path");
            String credentialsPath =
                configuredVendPath == null || configuredVendPath.isBlank()
                    ? HttpUnityCatalogClient.DATABRICKS_TEMPORARY_TABLE_CREDENTIALS_PATH
                    : configuredVendPath.trim();
            uc =
                new HttpUnityCatalogClient(
                    parseUriOption(host, "Unity Catalog base URI"),
                    Duration.ofMillis(connectMs),
                    Duration.ofMillis(readMs),
                    () -> authProvider.applyHeaders(Map.of()),
                    credentialsPath);
            yield new UnityDeltaConnector(
                "delta-unity",
                uc,
                authProvider,
                engineContext.engine(),
                engineContext.parquetInput(),
                ndvEnabled,
                ndvSampleFraction,
                ndvMaxFiles,
                engineContext.engineResources());
          } catch (RuntimeException | Error failure) {
            closeAfterFailedConstruction(uc, null, failure);
            throw failure;
          }
        }
      };
    } catch (RuntimeException | Error failure) {
      closeEngineResources(engineContext, failure);
      throw failure;
    }
  }

  /**
   * Parses a URI-valued connector option without quoting it back on failure.
   *
   * <p>{@code URI.create} puts the entire input in its message, and validateConnector walks the
   * cause chain appending every message into the summary it logs and returns over RPC. A value
   * carrying userinfo would publish that credential. The client rejects userinfo on the base URI,
   * but only once the string parses at all, so a malformed one never reaches that check.
   */
  private static URI parseUriOption(String value, String what) {
    try {
      return URI.create(value);
    } catch (IllegalArgumentException malformed) {
      // No cause and no input: both carry the text being rejected.
      throw new IllegalArgumentException(what + " is not a valid URI");
    }
  }

  /**
   * A timeout option, reported against the key an operator can act on. The client rejects a
   * non-positive timeout by its own parameter name, and Integer.parseInt reports nothing at all,
   * neither of which appears in connector configuration or the docs.
   */
  private static int positiveIntOption(Map<String, String> options, String key, int defaultValue) {
    String raw = options.get(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    int value;
    try {
      value = Integer.parseInt(raw.trim());
    } catch (NumberFormatException notAnInteger) {
      throw new IllegalArgumentException(
          "Option '" + key + "' must be a positive integer (got: '" + raw + "')", notAnInteger);
    }
    if (value <= 0) {
      throw new IllegalArgumentException(
          "Option '" + key + "' must be a positive integer (got: '" + raw + "')");
    }
    return value;
  }

  private static void closeEngineResources(EngineContext engineContext, Throwable failure) {
    AutoCloseable resources = engineContext.engineResources();
    if (resources == null) {
      return;
    }
    try {
      resources.close();
    } catch (Exception closeFailure) {
      failure.addSuppressed(closeFailure);
    }
  }

  private static void closeAfterFailedConstruction(
      HttpUnityCatalogClient catalog, AuthProvider authProvider, Throwable failure) {
    if (catalog != null) {
      try {
        catalog.close();
      } catch (RuntimeException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
    }
    if (authProvider instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception closeFailure) {
        failure.addSuppressed(closeFailure);
      }
    }
  }

  static Map<String, String> buildGlueCatalogOptions(Map<String, String> options) {
    Map<String, String> catalogOptions =
        options == null ? new LinkedHashMap<>() : new LinkedHashMap<>(options);
    copyIfAbsent(catalogOptions, "s3.access-key-id", "rest.access-key-id");
    copyIfAbsent(catalogOptions, "s3.secret-access-key", "rest.secret-access-key");
    copyIfAbsent(catalogOptions, "s3.session-token", "rest.session-token");

    String catalogProviderId =
        resolveOption(
            catalogOptions,
            RefreshingAwsCredentialsProviderRegistry.CATALOG_OPTION_PROVIDER_ID,
            null);
    if (catalogProviderId == null) {
      catalogProviderId =
          resolveOption(
              catalogOptions, RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID, null);
      if (catalogProviderId != null) {
        catalogOptions.put(
            RefreshingAwsCredentialsProviderRegistry.CATALOG_OPTION_PROVIDER_ID, catalogProviderId);
      }
    }

    catalogOptions.remove(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID);
    catalogOptions.remove(RefreshingAwsCredentialsProviderRegistry.PROPERTY_PROVIDER_ID);
    catalogOptions.remove("s3.access-key-id");
    catalogOptions.remove("s3.secret-access-key");
    catalogOptions.remove("s3.session-token");
    catalogOptions.remove(CLIENT_CREDENTIALS_PROVIDER);
    catalogOptions
        .keySet()
        .removeIf(key -> key != null && key.startsWith(CLIENT_CREDENTIALS_PROVIDER_PREFIX));
    return Map.copyOf(catalogOptions);
  }

  private static void copyIfAbsent(
      Map<String, String> properties, String sourceKey, String targetKey) {
    String value = properties.get(sourceKey);
    if (value != null && !value.isBlank()) {
      properties.putIfAbsent(targetKey, value);
    }
  }

  static DeltaSource selectSource(Map<String, String> options) {
    if (options == null) {
      return DeltaSource.UNITY;
    }
    String source = options.get(DELTA_SOURCE_OPTION);
    if (source != null && !source.isBlank()) {
      String normalized = source.trim().toLowerCase(Locale.ROOT);
      return switch (normalized) {
        case "unity" -> DeltaSource.UNITY;
        case "glue" -> DeltaSource.GLUE;
        case "filesystem" -> DeltaSource.FILESYSTEM;
        default -> throw new IllegalArgumentException("Unsupported delta.source: " + source);
      };
    }
    return DeltaSource.UNITY;
  }

  static void validateOptions(DeltaSource source, String storageLocation) {
    boolean hasTableRoot = storageLocation != null && !storageLocation.isBlank();
    if (source == DeltaSource.FILESYSTEM && !hasTableRoot) {
      throw new IllegalArgumentException(
          "delta.table-root is required for delta.source=filesystem");
    }
    if (source != DeltaSource.FILESYSTEM && hasTableRoot) {
      throw new IllegalArgumentException(
          "delta.table-root is only valid with delta.source=filesystem");
    }
  }

  /**
   * @param engineResources the refreshing S3 client the engine and parquet reader were built on, or
   *     null for a local-filesystem engine. Nothing else retains it, so the connector closes it.
   */
  private record EngineContext(
      Engine engine, Function<String, InputFile> parquetInput, AutoCloseable engineResources) {}

  private static EngineContext buildEngine(Map<String, String> options) {
    String localRoot = options.getOrDefault("fs.floecat.test-root", "");
    if (localRoot != null && !localRoot.isBlank()) {
      var root = java.nio.file.Path.of(localRoot).toAbsolutePath();
      Engine engine = DefaultEngine.create(new LocalFileSystemClient(root));
      Function<String, InputFile> inputFn = p -> new ParquetLocalInputFile(root, p);
      return new EngineContext(engine, inputFn, null);
    }

    var region = Region.of(resolveOption(options, "s3.region", "aws.region", "us-east-1"));

    boolean pathStyle =
        Boolean.parseBoolean(resolveOption(options, "s3.path-style-access", "false"));

    var credentials = credentialsProviderFactory(options);

    String endpoint = resolveOption(options, "s3.endpoint", null);
    var s3Client =
        RefreshingAwsClient.withResourceFactory(
            () -> {
              AwsCredentialsProvider provider = credentials.get();
              var s3Builder =
                  S3Client.builder()
                      .region(region)
                      .serviceConfiguration(
                          S3Configuration.builder().pathStyleAccessEnabled(pathStyle).build())
                      .credentialsProvider(provider);
              try {
                if (endpoint != null && !endpoint.isBlank()) {
                  s3Builder.endpointOverride(parseUriOption(endpoint, "s3.endpoint"));
                }
                return RefreshingAwsClient.clientResource(
                    s3Builder.build(), RefreshingAwsClient.closeableResource(provider));
              } catch (RuntimeException | Error e) {
                RefreshingAwsClient.closeQuietly(RefreshingAwsClient.closeableResource(provider));
                throw e;
              }
            });
    Engine engine = DefaultEngine.create(new S3V2FileSystemClient(s3Client));
    Function<String, InputFile> inputFn = p -> new ParquetS3V2InputFile(s3Client, p);
    return new EngineContext(engine, inputFn, s3Client);
  }

  private static AwsCredentialsProvider resolveCredentials(Map<String, String> options) {
    String providerId =
        resolveOption(options, RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID, null);
    if (providerId != null && !providerId.isBlank()) {
      return new RegistryBackedAwsCredentialsProvider(providerId);
    }
    String access = resolveOption(options, "s3.access-key-id", null);
    String secret = resolveOption(options, "s3.secret-access-key", null);
    String token = resolveOption(options, "s3.session-token", null);

    if (access != null && !access.isBlank() && secret != null && !secret.isBlank()) {
      AwsCredentials creds =
          (token != null && !token.isBlank())
              ? AwsSessionCredentials.create(access, secret, token)
              : AwsBasicCredentials.create(access, secret);
      return StaticCredentialsProvider.create(creds);
    }
    return DefaultCredentialsProvider.builder().build();
  }

  static Supplier<AwsCredentialsProvider> credentialsProviderFactory(Map<String, String> options) {
    return () -> resolveCredentials(options);
  }

  private static String resolveOption(
      Map<String, String> options, String key, String defaultValue) {
    if (options != null) {
      String opt = options.get(key);
      if (opt != null && !opt.isBlank()) {
        return opt;
      }
    }
    return defaultValue;
  }

  private static String resolveOption(
      Map<String, String> options, String key, String fallbackKey, String defaultValue) {
    if (options != null) {
      String opt = options.get(key);
      if (opt != null && !opt.isBlank()) {
        return opt;
      }
      String fallback = options.get(fallbackKey);
      if (fallback != null && !fallback.isBlank()) {
        return fallback;
      }
    }
    return defaultValue;
  }

  private static String deriveTableName(String storageLocation) {
    if (storageLocation == null || storageLocation.isBlank()) {
      return "";
    }
    String trimmed =
        storageLocation.endsWith("/")
            ? storageLocation.substring(0, storageLocation.length() - 1)
            : storageLocation;
    int slash = trimmed.lastIndexOf('/');
    if (slash < 0 || slash == trimmed.length() - 1) {
      return trimmed;
    }
    return trimmed.substring(slash + 1);
  }
}
