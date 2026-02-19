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

import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
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

  private DeltaConnectorFactory() {}

  enum DeltaSource {
    UNITY,
    FILESYSTEM
  }

  static FloecatConnector create(
      String uri, Map<String, String> options, AuthProvider authProvider) {
    Map<String, String> opts = (options == null) ? Collections.emptyMap() : options;
    Map<String, String> effectiveOptions = new LinkedHashMap<>(opts);

    DeltaSource source = selectSource(effectiveOptions);
    String tableRoot = effectiveOptions.getOrDefault("delta.table-root", "");
    validateOptions(source, tableRoot);

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

    return switch (source) {
      case FILESYSTEM -> {
        String namespaceFq = effectiveOptions.getOrDefault("external.namespace", "");
        String tableName =
            effectiveOptions.getOrDefault("external.table-name", deriveTableName(tableRoot));
        yield new DeltaFilesystemConnector(
            "delta-filesystem",
            engineContext.engine(),
            engineContext.parquetInput(),
            ndvEnabled,
            ndvSampleFraction,
            ndvMaxFiles,
            tableRoot,
            namespaceFq,
            tableName);
      }
      case UNITY -> {
        Objects.requireNonNull(uri, "Unity base uri");
        String host = uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
        int connectMs = Integer.parseInt(effectiveOptions.getOrDefault("http.connect.ms", "10000"));
        int readMs = Integer.parseInt(effectiveOptions.getOrDefault("http.read.ms", "60000"));
        String warehouse = effectiveOptions.getOrDefault("databricks.sql.warehouse_id", "");
        var uc = new UcHttp(host, connectMs, readMs, authProvider);
        var sql =
            warehouse.isBlank() ? null : new SqlStmtClient(host, authProvider, warehouse, readMs);
        yield new UnityDeltaConnector(
            "delta-unity",
            uc,
            sql,
            engineContext.engine(),
            engineContext.parquetInput(),
            ndvEnabled,
            ndvSampleFraction,
            ndvMaxFiles);
      }
    };
  }

  static DeltaSource selectSource(Map<String, String> options) {
    if (options == null) {
      return DeltaSource.UNITY;
    }
    String source = options.get("delta.source");
    if (source != null && !source.isBlank()) {
      String normalized = source.trim().toLowerCase(Locale.ROOT);
      return switch (normalized) {
        case "unity" -> DeltaSource.UNITY;
        case "filesystem" -> DeltaSource.FILESYSTEM;
        default -> throw new IllegalArgumentException("Unsupported delta.source: " + source);
      };
    }
    return DeltaSource.UNITY;
  }

  static void validateOptions(DeltaSource source, String tableRoot) {
    boolean hasTableRoot = tableRoot != null && !tableRoot.isBlank();
    if (source == DeltaSource.FILESYSTEM && !hasTableRoot) {
      throw new IllegalArgumentException(
          "delta.table-root is required for delta.source=filesystem");
    }
    if (source != DeltaSource.FILESYSTEM && hasTableRoot) {
      throw new IllegalArgumentException(
          "delta.table-root is only valid with delta.source=filesystem");
    }
  }

  private record EngineContext(Engine engine, Function<String, InputFile> parquetInput) {}

  private static EngineContext buildEngine(Map<String, String> options) {
    String localRoot = options.getOrDefault("fs.floecat.test-root", "");
    if (localRoot != null && !localRoot.isBlank()) {
      var root = java.nio.file.Path.of(localRoot).toAbsolutePath();
      Engine engine = DefaultEngine.create(new LocalFileSystemClient(root));
      Function<String, InputFile> inputFn = p -> new ParquetLocalInputFile(root, p);
      return new EngineContext(engine, inputFn);
    }

    var region = Region.of(resolveOption(options, "s3.region", "aws.region", "us-east-1"));

    boolean pathStyle =
        Boolean.parseBoolean(resolveOption(options, "s3.path-style-access", "false"));

    var s3Builder =
        S3Client.builder()
            .region(region)
            .serviceConfiguration(
                S3Configuration.builder().pathStyleAccessEnabled(pathStyle).build())
            .credentialsProvider(resolveCredentials(options));

    String endpoint = resolveOption(options, "s3.endpoint", null);
    if (endpoint != null && !endpoint.isBlank()) {
      s3Builder.endpointOverride(URI.create(endpoint));
    }

    var s3 = s3Builder.build();
    Engine engine = DefaultEngine.create(new S3V2FileSystemClient(s3));
    Function<String, InputFile> inputFn = p -> new ParquetS3V2InputFile(s3, p);
    return new EngineContext(engine, inputFn);
  }

  private static AwsCredentialsProvider resolveCredentials(Map<String, String> options) {
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

  private static String deriveTableName(String tableRoot) {
    if (tableRoot == null || tableRoot.isBlank()) {
      return "";
    }
    String trimmed =
        tableRoot.endsWith("/") ? tableRoot.substring(0, tableRoot.length() - 1) : tableRoot;
    int slash = trimmed.lastIndexOf('/');
    if (slash < 0 || slash == trimmed.length() - 1) {
      return trimmed;
    }
    return trimmed.substring(slash + 1);
  }
}
