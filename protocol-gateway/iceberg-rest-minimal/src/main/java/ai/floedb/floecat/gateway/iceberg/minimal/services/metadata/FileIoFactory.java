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

package ai.floedb.floecat.gateway.iceberg.minimal.services.metadata;

import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.io.FileIO;

public final class FileIoFactory {
  private static final String DEFAULT_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";
  private static final Set<String> IO_PROP_PREFIXES =
      Set.of("s3.", "s3a.", "s3n.", "fs.", "client.", "aws.", "hadoop.");

  private FileIoFactory() {}

  public static FileIO createFileIo(Map<String, String> props, MinimalGatewayConfig config) {
    Map<String, String> normalized =
        props == null ? new LinkedHashMap<>() : new LinkedHashMap<>(props);
    if (config != null) {
      config.metadataFileIoRoot().ifPresent(root -> normalized.put("fs.floecat.test-root", root));
      config
          .metadataS3Endpoint()
          .map(FileIoFactory::normalizeEndpoint)
          .ifPresent(
              endpoint -> {
                normalized.put("s3.endpoint", endpoint);
                normalized.put("aws.endpoint-url.s3", endpoint);
              });
      normalized.put("s3.path-style-access", Boolean.toString(config.metadataS3PathStyleAccess()));
      config
          .metadataS3Region()
          .ifPresent(
              region -> {
                normalized.put("s3.region", region);
                normalized.put("region", region);
              });
      config.metadataClientRegion().ifPresent(region -> normalized.put("client.region", region));
      config
          .metadataS3AccessKeyId()
          .ifPresent(
              accessKeyId -> {
                normalized.put("s3.access-key-id", accessKeyId);
                normalized.put("aws.access-key-id", accessKeyId);
              });
      config
          .metadataS3SecretAccessKey()
          .ifPresent(
              secretAccessKey -> {
                normalized.put("s3.secret-access-key", secretAccessKey);
                normalized.put("aws.secret-access-key", secretAccessKey);
              });
    }
    String impl =
        config == null
            ? normalized.getOrDefault("io-impl", DEFAULT_IO_IMPL)
            : config.metadataFileIo().orElse(normalized.getOrDefault("io-impl", DEFAULT_IO_IMPL));
    try {
      Class<?> clazz = Class.forName(impl);
      Object instance = clazz.getDeclaredConstructor().newInstance();
      if (!(instance instanceof FileIO fileIO)) {
        throw new IllegalArgumentException(impl + " does not implement FileIO");
      }
      fileIO.initialize(filterIoProperties(normalized));
      return fileIO;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to instantiate FileIO " + impl, e);
    }
  }

  public static Map<String, String> filterIoProperties(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return Map.of();
    }
    Map<String, String> filtered = new LinkedHashMap<>();
    props.forEach(
        (key, value) -> {
          if (key == null || value == null) {
            return;
          }
          if (isFileIoProperty(key)) {
            filtered.put(key, value);
          }
        });
    return filtered;
  }

  public static boolean isFileIoProperty(String key) {
    if (key == null || key.isBlank()) {
      return false;
    }
    if ("io-impl".equals(key)) {
      return true;
    }
    for (String prefix : IO_PROP_PREFIXES) {
      if (key.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static String normalizeEndpoint(String endpoint) {
    if (endpoint == null || endpoint.isBlank()) {
      return endpoint;
    }
    try {
      URI uri = URI.create(endpoint);
      String host = uri.getHost();
      if (host == null || !host.equals("floecat.localstack")) {
        return endpoint;
      }
      URI normalized =
          new URI(
              uri.getScheme(),
              uri.getUserInfo(),
              "localstack",
              uri.getPort(),
              uri.getPath(),
              uri.getQuery(),
              uri.getFragment());
      return normalized.toString();
    } catch (Exception ignored) {
      return endpoint;
    }
  }
}
