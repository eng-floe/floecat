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

package ai.floedb.floecat.gateway.iceberg.rest.catalog;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.support.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.config.Config;

@ApplicationScoped
public class StorageAccessService {
  private static final List<StorageCredentialDto> STATIC_STORAGE_CREDENTIALS =
      List.of(new StorageCredentialDto("*", Map.of("type", "static")));

  private final IcebergGatewayConfig config;
  private final Config mpConfig;

  private volatile Map<String, String> tableConfigCache;
  private volatile List<StorageCredentialDto> storageCredentialCache;

  @Inject
  public StorageAccessService(IcebergGatewayConfig config, Config mpConfig) {
    this.config = config;
    this.mpConfig = mpConfig;
  }

  public Map<String, String> defaultTableConfig() {
    Map<String, String> cached = tableConfigCache;
    if (cached != null) {
      return cached;
    }
    Map<String, String> computed = new LinkedHashMap<>();
    config.metadataFileIo().ifPresent(ioImpl -> computed.putIfAbsent("io-impl", ioImpl));
    config
        .metadataFileIoRoot()
        .ifPresent(root -> computed.putIfAbsent("fs.floecat.test-root", root));
    config
        .storageCredential()
        .ifPresent(cfg -> cfg.properties().forEach((k, v) -> addClientSafeConfig(computed, k, v)));
    readPrefixedConfig("floecat.gateway.storage-credential.properties.")
        .forEach((k, v) -> addClientSafeConfig(computed, k, v));
    config
        .defaultRegion()
        .filter(region -> region != null && !region.isBlank())
        .ifPresent(
            region -> {
              computed.putIfAbsent("s3.region", region);
              computed.putIfAbsent("region", region);
              computed.putIfAbsent("client.region", region);
            });
    Map<String, String> normalized = computed.isEmpty() ? Map.of() : Map.copyOf(computed);
    tableConfigCache = normalized;
    return normalized;
  }

  public Map<String, String> resolveRegisterFileIoProperties(
      Map<String, String> requestProperties) {
    Map<String, String> resolved = new LinkedHashMap<>(defaultFileIoProperties());
    if (requestProperties != null && !requestProperties.isEmpty()) {
      requestProperties.forEach(
          (k, v) -> {
            if (FileIoFactory.isFileIoProperty(k) && isUsableIoValue(v)) {
              resolved.put(k, v.trim());
            }
          });
    }
    return resolved.isEmpty() ? Map.of() : Map.copyOf(resolved);
  }

  public Map<String, String> defaultFileIoProperties() {
    Map<String, String> merged = new LinkedHashMap<>();
    defaultCredentials().stream()
        .findFirst()
        .map(StorageCredentialDto::config)
        .ifPresent(
            credentialProps ->
                credentialProps.forEach(
                    (k, v) -> {
                      if (FileIoFactory.isFileIoProperty(k) && isUsableIoValue(v)) {
                        merged.put(k, v.trim());
                      }
                    }));
    defaultTableConfig()
        .forEach(
            (k, v) -> {
              if (FileIoFactory.isFileIoProperty(k) && isUsableIoValue(v)) {
                merged.put(k, v.trim());
              }
            });
    return merged.isEmpty() ? Map.of() : Map.copyOf(merged);
  }

  public List<StorageCredentialDto> defaultCredentials() {
    List<StorageCredentialDto> cached = storageCredentialCache;
    if (cached != null) {
      return cached;
    }
    Map<String, String> props = new LinkedHashMap<>();
    config.storageCredential().ifPresent(cfg -> props.putAll(cfg.properties()));
    readPrefixedConfig("floecat.gateway.storage-credential.properties.")
        .forEach(
            (k, v) -> {
              if (v != null && !v.isBlank()) {
                props.put(k, v);
              }
            });
    if (props.isEmpty()) {
      storageCredentialCache = STATIC_STORAGE_CREDENTIALS;
      return STATIC_STORAGE_CREDENTIALS;
    }
    String scope =
        config
            .storageCredential()
            .flatMap(IcebergGatewayConfig.StorageCredentialConfig::scope)
            .filter(s -> !s.isBlank())
            .orElseGet(
                () ->
                    mpConfig
                        .getOptionalValue("floecat.gateway.storage-credential.scope", String.class)
                        .filter(s -> s != null && !s.isBlank())
                        .orElse("*"));
    List<StorageCredentialDto> computed =
        List.of(new StorageCredentialDto(scope, Map.copyOf(props)));
    storageCredentialCache = computed;
    return computed;
  }

  public List<StorageCredentialDto> credentialsForAccessDelegation(String accessDelegationMode) {
    if (accessDelegationMode == null || accessDelegationMode.isBlank()) {
      return null;
    }
    boolean vended = false;
    for (String raw : accessDelegationMode.split(",")) {
      String mode = raw == null ? "" : raw.trim();
      if (mode.isEmpty()) {
        continue;
      }
      if ("vended-credentials".equalsIgnoreCase(mode)) {
        vended = true;
        continue;
      }
      throw new IllegalArgumentException("Unsupported access delegation mode: " + mode);
    }
    if (!vended) {
      return null;
    }
    if (!hasConfiguredCredentials()) {
      throw new IllegalArgumentException(
          "Credential vending was requested but no credentials are available");
    }
    return defaultCredentials();
  }

  public String metadataLocationFromCreate(TableRequests.Create req) {
    return MetadataLocationUtil.metadataLocation(req == null ? null : req.properties());
  }

  public String resolveTableLocation(String requestedLocation, String metadataLocation) {
    if (requestedLocation != null && !requestedLocation.isBlank()) {
      return requestedLocation;
    }
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return null;
    }
    int idx = metadataLocation.indexOf("/metadata/");
    String base;
    if (idx > 0) {
      base = metadataLocation.substring(0, idx);
    } else {
      int slash = metadataLocation.lastIndexOf('/');
      base = slash > 0 ? metadataLocation.substring(0, slash) : metadataLocation;
    }
    return base;
  }

  private void addClientSafeConfig(Map<String, String> target, String key, String value) {
    if (!isUsableIoValue(value) || key == null || key.isBlank()) {
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

  private boolean hasConfiguredCredentials() {
    Map<String, String> props = new LinkedHashMap<>();
    config.storageCredential().ifPresent(cfg -> props.putAll(cfg.properties()));
    readPrefixedConfig("floecat.gateway.storage-credential.properties.")
        .forEach(
            (k, v) -> {
              if (v != null && !v.isBlank()) {
                props.put(k, v);
              }
            });
    return !props.isEmpty();
  }

  private Map<String, String> readPrefixedConfig(String prefix) {
    Map<String, String> out = new LinkedHashMap<>();
    for (String name : mpConfig.getPropertyNames()) {
      if (name.startsWith(prefix)) {
        mpConfig
            .getOptionalValue(name, String.class)
            .ifPresent(value -> out.put(name.substring(prefix.length()), value));
      }
    }
    return out;
  }

  private static boolean isUsableIoValue(String value) {
    if (value == null) {
      return false;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return false;
    }
    return !(trimmed.startsWith("<") && trimmed.endsWith(">"));
  }
}
