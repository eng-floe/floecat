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
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTCatalog;

final class IcebergConnectorFactory {

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
    if (cleanOpts != null) {
      try {
        ndvMaxFiles = Long.parseLong(cleanOpts.getOrDefault("stats.ndv.max_files", "0"));
        if (ndvMaxFiles < 0) {
          ndvMaxFiles = 0;
        }
      } catch (NumberFormatException ignore) {
      }
    }

    validateOptions(source, uri, cleanOpts);
    return switch (source) {
      case FILESYSTEM -> {
        var loaded = IcebergConnector.loadExternalTable(uri, cleanOpts);
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
        Map<String, String> props = buildRestProps(uri, cleanOpts);
        applyAuth(props, authScheme, authProps);

        if (headerHints != null) {
          headerHints.forEach((k, v) -> props.put("header." + k, v));
        }

        props.putIfAbsent("rest.client.user-agent", "floecat-connector-iceberg");

        RESTCatalog cat = new RESTCatalog();
        cat.initialize("floecat-iceberg", Collections.unmodifiableMap(props));
        if (source == IcebergSource.GLUE) {
          var glue = AwsGlueClientFactory.create(props, authProps);
          var glueFilter = new GlueIcebergFilter(glue);
          yield new IcebergGlueConnector(
              "iceberg-glue", cat, glueFilter, ndvEnabled, ndvSampleFraction, ndvMaxFiles);
        }
        yield new IcebergRestConnector(
            "iceberg-rest", cat, ndvEnabled, ndvSampleFraction, ndvMaxFiles);
      }
    };
  }

  private static Map<String, String> buildRestProps(String uri, Map<String, String> cleanOpts) {
    Map<String, String> props = new HashMap<>();
    props.put("type", "rest");
    props.put("uri", uri);
    if (cleanOpts != null && !cleanOpts.isEmpty()) {
      props.putAll(cleanOpts);
    }
    normalizeAwsRegionProperties(props);
    return props;
  }

  private static void applyAuth(
      Map<String, String> props, String authScheme, Map<String, String> authProps) {
    String scheme = (authScheme == null ? "none" : authScheme.trim().toLowerCase(Locale.ROOT));
    switch (scheme) {
      case "aws-sigv4" -> {
        String signingName = authProps.getOrDefault("signing-name", "glue");
        String signingRegion =
            authProps.getOrDefault(
                "signing-region",
                props.getOrDefault(
                    "rest.signing-region", props.getOrDefault("s3.region", "us-east-1")));
        props.put("rest.auth.type", "sigv4");
        props.put("rest.signing-name", signingName);
        props.put("rest.signing-region", signingRegion);

        props.putIfAbsent("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        props.putIfAbsent("s3.region", signingRegion);
        props.putIfAbsent("client.region", signingRegion);

        AwsProfileSupport.applyProfileProperties(props, authProps);
      }

      case "oauth2" -> {
        String token =
            Objects.requireNonNull(authProps.get("token"), "authProps.token required for oauth2");
        props.put("token", token);
      }

      case "none" -> {}

      default -> throw new IllegalArgumentException("Unsupported auth scheme: " + authScheme);
    }
  }

  private static void normalizeAwsRegionProperties(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
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

  static void validateOptions(IcebergSource source, String uri, Map<String, String> options) {
    boolean hasFilesystemUri = uri != null && !uri.isBlank();
    if (source == IcebergSource.FILESYSTEM && !hasFilesystemUri) {
      throw new IllegalArgumentException("uri is required for iceberg.source=filesystem");
    }
  }
}
