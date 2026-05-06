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
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;

final class IcebergConnectorFactory {
  private static final long REST_CATALOG_TTL_MS = Duration.ofMinutes(5).toMillis();
  private static final ConcurrentMap<CatalogCacheKey, CatalogCacheEntry> REST_CATALOG_CACHE =
      new ConcurrentHashMap<>();
  private static final String DEFAULT_S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO";

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
            fileIO);
      }
      case GLUE, REST -> {
        Map<String, String> props = buildCatalogProperties(uri, baseProps);
        applyCatalogAuth(props, authScheme, authProps);
        applyStorageAuth(props, authScheme, authProps);

        if (headerHints != null) {
          headerHints.forEach((k, v) -> props.put("header." + k, v));
        }

        props.putIfAbsent("rest.client.user-agent", "floecat-connector-iceberg");

        CatalogLease catalogLease = acquireRestCatalog(props);
        RESTCatalog rawCatalog = catalogLease.rawCatalog();
        Catalog tableCatalog = catalogLease.tableCatalog();
        try {
          if (source == IcebergSource.GLUE) {
            var glue = AwsGlueClientFactory.create(props, authProps);
            var glueFilter = new GlueIcebergFilter(glue);
            yield new IcebergGlueConnector(
                "iceberg-glue",
                rawCatalog,
                tableCatalog,
                glueFilter,
                ndvEnabled,
                ndvSampleFraction,
                ndvMaxFiles,
                false,
                catalogLease::release);
          }
          yield new IcebergRestConnector(
              "iceberg-rest",
              rawCatalog,
              tableCatalog,
              ndvEnabled,
              ndvSampleFraction,
              ndvMaxFiles,
              false,
              catalogLease::release);
        } catch (RuntimeException e) {
          catalogLease.release();
          throw e;
        }
      }
    };
  }

  private static CatalogLease acquireRestCatalog(Map<String, String> props) {
    long now = System.currentTimeMillis();
    evictExpiredCatalogs(now);
    CatalogCacheKey key = CatalogCacheKey.of(props);
    CatalogCacheEntry cached = REST_CATALOG_CACHE.get(key);
    if (cached != null && !cached.isExpired(now)) {
      CatalogLease lease = cached.tryAcquire(now + REST_CATALOG_TTL_MS);
      if (lease != null) {
        return lease;
      }
    }
    while (true) {
      CatalogCacheEntry entry =
          REST_CATALOG_CACHE.compute(
              key,
              (ignored, existing) -> {
                long refreshNow = System.currentTimeMillis();
                if (existing != null && !existing.isExpired(refreshNow)) {
                  existing.touch(refreshNow + REST_CATALOG_TTL_MS);
                  return existing;
                }
                if (existing != null) {
                  existing.retire();
                }
                RESTCatalog created = new RESTCatalog();
                Map<String, String> catalogProps =
                    Collections.unmodifiableMap(new HashMap<>(props));
                created.initialize("floecat-iceberg", catalogProps);
                return new CatalogCacheEntry(created, created, refreshNow + REST_CATALOG_TTL_MS);
              });
      CatalogLease lease = entry.tryAcquire(System.currentTimeMillis() + REST_CATALOG_TTL_MS);
      if (lease != null) {
        return lease;
      }
    }
  }

  private static void evictExpiredCatalogs(long now) {
    for (Map.Entry<CatalogCacheKey, CatalogCacheEntry> entry : REST_CATALOG_CACHE.entrySet()) {
      CatalogCacheEntry cached = entry.getValue();
      if (!cached.isExpired(now)) {
        continue;
      }
      if (REST_CATALOG_CACHE.remove(entry.getKey(), cached)) {
        cached.retire();
      }
    }
  }

  private static void closeQuietly(RESTCatalog catalog) {
    if (catalog == null) {
      return;
    }
    try {
      catalog.close();
    } catch (Exception ignore) {
    }
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
    return props;
  }

  static Map<String, String> buildStorageProperties(
      Map<String, String> baseProps, String authScheme, Map<String, String> authProps) {
    Map<String, String> props = new HashMap<>();
    if (baseProps != null && !baseProps.isEmpty()) {
      props.putAll(baseProps);
    }
    applyStorageAuth(props, authScheme, authProps);
    return props;
  }

  static void applyCatalogAuth(
      Map<String, String> props, String authScheme, Map<String, String> authProps) {
    Map<String, String> safeAuthProps = authProps == null ? Collections.emptyMap() : authProps;
    String scheme = (authScheme == null ? "none" : authScheme.trim().toLowerCase(Locale.ROOT));
    switch (scheme) {
      case "aws-sigv4" -> {
        String signingName = safeAuthProps.getOrDefault("signing-name", "glue");
        String signingRegion =
            safeAuthProps.getOrDefault(
                "signing-region",
                props.getOrDefault(
                    "rest.signing-region", props.getOrDefault("s3.region", "us-east-1")));
        props.put("rest.auth.type", "sigv4");
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

  private record CatalogCacheKey(Map<String, String> props) {
    static CatalogCacheKey of(Map<String, String> props) {
      if (props == null || props.isEmpty()) {
        return new CatalogCacheKey(Map.of());
      }
      Map<String, String> safe = new LinkedHashMap<>();
      props.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .forEach(
              entry ->
                  safe.put(
                      entry.getKey(),
                      isSensitiveCacheKey(entry.getKey())
                          ? fingerprintValue(entry.getValue())
                          : entry.getValue()));
      return new CatalogCacheKey(Map.copyOf(safe));
    }
  }

  private static boolean isSensitiveCacheKey(String key) {
    if (key == null || key.isBlank()) {
      return false;
    }
    String lower = key.toLowerCase(Locale.ROOT);
    return lower.contains("secret")
        || lower.contains("token")
        || lower.contains("password")
        || lower.contains("credential")
        || lower.contains("authorization")
        || lower.contains("bearer")
        || lower.contains("assertion")
        || lower.contains("private")
        || lower.contains("session-token")
        || lower.contains("access-key")
        || lower.contains("secret-access-key");
  }

  private static String fingerprintValue(String value) {
    String safe = value == null ? "" : value;
    return "sha256:"
        + ai.floedb.floecat.types.Hashing.sha256Hex(
            safe.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private static final class CatalogCacheEntry {
    private final RESTCatalog rawCatalog;
    private final Catalog catalog;
    private volatile long expiresAtMs;
    private int activeLeases;
    private boolean retired;
    private boolean closed;

    private CatalogCacheEntry(RESTCatalog rawCatalog, Catalog catalog, long expiresAtMs) {
      this.rawCatalog = rawCatalog;
      this.catalog = catalog;
      this.expiresAtMs = expiresAtMs;
    }

    private RESTCatalog rawCatalog() {
      return rawCatalog;
    }

    private Catalog tableCatalog() {
      return catalog;
    }

    private boolean isExpired(long now) {
      return now >= expiresAtMs;
    }

    private void touch(long nextExpiryMs) {
      this.expiresAtMs = nextExpiryMs;
    }

    private CatalogLease tryAcquire(long nextExpiryMs) {
      synchronized (this) {
        if (retired || closed) {
          return null;
        }
        this.expiresAtMs = nextExpiryMs;
        activeLeases++;
      }
      return new CatalogLease(this);
    }

    private void release() {
      RESTCatalog toClose = null;
      synchronized (this) {
        if (activeLeases > 0) {
          activeLeases--;
        }
        if (retired && activeLeases == 0 && !closed) {
          closed = true;
          toClose = rawCatalog;
        }
      }
      closeQuietly(toClose);
    }

    private void retire() {
      RESTCatalog toClose = null;
      synchronized (this) {
        if (retired) {
          return;
        }
        retired = true;
        if (activeLeases == 0 && !closed) {
          closed = true;
          toClose = rawCatalog;
        }
      }
      closeQuietly(toClose);
    }
  }

  private static final class CatalogLease {
    private final CatalogCacheEntry entry;
    private final AtomicBoolean released = new AtomicBoolean(false);

    private CatalogLease(CatalogCacheEntry entry) {
      this.entry = entry;
    }

    private RESTCatalog rawCatalog() {
      return entry.rawCatalog();
    }

    private Catalog tableCatalog() {
      return entry.tableCatalog();
    }

    private void release() {
      if (released.compareAndSet(false, true)) {
        entry.release();
      }
    }
  }
}
