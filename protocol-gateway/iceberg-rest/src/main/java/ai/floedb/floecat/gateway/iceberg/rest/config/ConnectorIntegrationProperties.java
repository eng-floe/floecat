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

package ai.floedb.floecat.gateway.iceberg.rest.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;

public final class ConnectorIntegrationProperties {
  private ConnectorIntegrationProperties() {}

  public static Map<String, String> defaultTableConfig(
      ConnectorIntegrationConfig config, StorageAwsConfig storageAwsConfig) {
    LinkedHashMap<String, String> computed = new LinkedHashMap<>();
    if (config == null) {
      return storageConfig(storageAwsConfig);
    }
    Map<String, String> storageCredentialDefaults = config.storageCredentialProperties();
    if (storageCredentialDefaults != null && !storageCredentialDefaults.isEmpty()) {
      computed.putAll(clientSafeStorageConfig(storageCredentialDefaults));
    }
    config.metadataFileIo().ifPresent(ioImpl -> computed.putIfAbsent("io-impl", ioImpl));
    config
        .metadataFileIoRoot()
        .ifPresent(root -> computed.putIfAbsent("fs.floecat.test-root", root));
    config
        .defaultRegion()
        .filter(region -> region != null && !region.isBlank())
        .ifPresent(
            region -> {
              computed.putIfAbsent("s3.region", region);
              computed.putIfAbsent("region", region);
              computed.putIfAbsent("client.region", region);
            });
    storageConfig(storageAwsConfig).forEach(computed::put);
    return computed.isEmpty() ? Map.of() : Map.copyOf(computed);
  }

  public static Map<String, String> defaultFileIoProperties(
      ConnectorIntegrationConfig config,
      StorageAwsConfig storageAwsConfig,
      Predicate<String> fileIoPropertyFilter) {
    LinkedHashMap<String, String> merged = new LinkedHashMap<>();
    defaultTableConfig(config, storageAwsConfig)
        .forEach(
            (key, value) -> {
              if (isFileIoProperty(fileIoPropertyFilter, key)) {
                merged.put(key, value);
              }
            });
    return merged.isEmpty() ? Map.of() : Map.copyOf(merged);
  }

  public static boolean isUsableValue(String value) {
    if (value == null) {
      return false;
    }
    String trimmed = value.trim();
    return !trimmed.isEmpty() && !(trimmed.startsWith("<") && trimmed.endsWith(">"));
  }

  public static Map<String, String> clientSafeStorageConfig(Map<String, String> source) {
    if (source == null || source.isEmpty()) {
      return Map.of();
    }
    LinkedHashMap<String, String> computed = new LinkedHashMap<>();
    source.forEach((key, value) -> addClientSafeConfig(computed, key, value));
    return computed.isEmpty() ? Map.of() : Map.copyOf(computed);
  }

  private static void addClientSafeConfig(Map<String, String> target, String key, String value) {
    if (!isUsableValue(value) || key == null || key.isBlank()) {
      return;
    }
    String normalized = key.toLowerCase();
    if (normalized.contains("secret")
        || normalized.contains("access-key")
        || normalized.contains("session-token")
        || normalized.contains("token")) {
      return;
    }
    if ("s3.endpoint".equals(normalized)
        || "s3.path-style-access".equals(normalized)
        || "s3.region".equals(normalized)
        || "region".equals(normalized)
        || "client.region".equals(normalized)) {
      target.putIfAbsent(key, value.trim());
    }
  }

  private static boolean isFileIoProperty(Predicate<String> fileIoPropertyFilter, String key) {
    return fileIoPropertyFilter != null && key != null && fileIoPropertyFilter.test(key);
  }

  private static Map<String, String> storageConfig(StorageAwsConfig storageAwsConfig) {
    if (storageAwsConfig == null) {
      return Map.of();
    }
    LinkedHashMap<String, String> computed = new LinkedHashMap<>();
    storageAwsConfig
        .region()
        .filter(ConnectorIntegrationProperties::isUsableValue)
        .map(String::trim)
        .ifPresent(
            region -> {
              computed.put("s3.region", region);
              computed.put("region", region);
              computed.put("client.region", region);
            });
    storageAwsConfig
        .s3()
        .endpoint()
        .filter(ConnectorIntegrationProperties::isUsableValue)
        .map(String::trim)
        .ifPresent(endpoint -> computed.put("s3.endpoint", endpoint));
    if (storageAwsConfig.s3().pathStyleAccess()) {
      computed.put("s3.path-style-access", "true");
    }
    return computed.isEmpty() ? Map.of() : Map.copyOf(computed);
  }
}
