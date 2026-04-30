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

package ai.floedb.floecat.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public final class ConnectorIntegrationProperties {
  private ConnectorIntegrationProperties() {}

  public static Map<String, String> credentialProperties(ConnectorIntegrationConfig config) {
    if (config == null) {
      return Map.of();
    }
    LinkedHashMap<String, String> props = new LinkedHashMap<>();
    config
        .storageCredential()
        .ifPresent(
            credential ->
                credential
                    .properties()
                    .forEach(
                        (key, value) -> {
                          if (isUsableValue(value) && key != null && !key.isBlank()) {
                            props.put(key, value.trim());
                          }
                        }));
    return props.isEmpty() ? Map.of() : Map.copyOf(props);
  }

  public static Optional<String> credentialScope(ConnectorIntegrationConfig config) {
    if (config == null) {
      return Optional.empty();
    }
    return config
        .storageCredential()
        .flatMap(ConnectorIntegrationConfig.StorageCredentialConfig::scope);
  }

  public static Map<String, String> defaultTableConfig(ConnectorIntegrationConfig config) {
    LinkedHashMap<String, String> computed = new LinkedHashMap<>();
    if (config == null) {
      return Map.of();
    }
    config.metadataFileIo().ifPresent(ioImpl -> computed.putIfAbsent("io-impl", ioImpl));
    config
        .metadataFileIoRoot()
        .ifPresent(root -> computed.putIfAbsent("fs.floecat.test-root", root));
    credentialProperties(config).forEach((key, value) -> addClientSafeConfig(computed, key, value));
    config
        .defaultRegion()
        .filter(region -> region != null && !region.isBlank())
        .ifPresent(
            region -> {
              computed.putIfAbsent("s3.region", region);
              computed.putIfAbsent("region", region);
              computed.putIfAbsent("client.region", region);
            });
    return computed.isEmpty() ? Map.of() : Map.copyOf(computed);
  }

  public static Map<String, String> defaultFileIoProperties(
      ConnectorIntegrationConfig config, Predicate<String> fileIoPropertyFilter) {
    LinkedHashMap<String, String> merged = new LinkedHashMap<>();
    credentialProperties(config)
        .forEach(
            (key, value) -> {
              if (isFileIoProperty(fileIoPropertyFilter, key)) {
                merged.put(key, value);
              }
            });
    defaultTableConfig(config)
        .forEach(
            (key, value) -> {
              if (isFileIoProperty(fileIoPropertyFilter, key)) {
                merged.put(key, value);
              }
            });
    return merged.isEmpty() ? Map.of() : Map.copyOf(merged);
  }

  public static boolean hasConfiguredCredentials(ConnectorIntegrationConfig config) {
    return !credentialProperties(config).isEmpty();
  }

  public static boolean isUsableValue(String value) {
    if (value == null) {
      return false;
    }
    String trimmed = value.trim();
    return !trimmed.isEmpty() && !(trimmed.startsWith("<") && trimmed.endsWith(">"));
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
}
