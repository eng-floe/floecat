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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityForAccountLocationRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthoritiesGrpc;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class ServerSideStorageConfigResolver {
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORRELATION_ID =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  private final Optional<String> headerName;
  private final Optional<String> staticToken;

  @GrpcClient("floecat")
  StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub storageAuthorities;

  public ServerSideStorageConfigResolver(
      @ConfigProperty(name = "floecat.reconciler.authorization.header") Optional<String> headerName,
      @ConfigProperty(name = "floecat.reconciler.authorization.token") Optional<String> staticToken) {
    this.headerName = headerName.map(String::trim).filter(v -> !v.isBlank());
    this.staticToken = staticToken.map(String::trim).filter(v -> !v.isBlank());
  }

  public ConnectorConfig resolve(Connector connector, ConnectorConfig config) {
    return resolve(Optional.empty(), connector, config);
  }

  public ConnectorConfig resolve(
      Optional<ReconcileContext> ctx, Connector connector, ConnectorConfig config) {
    if (connector == null || config == null || connector.getKindValue() == 0) {
      return config;
    }
    if (config.kind() != ConnectorConfig.Kind.ICEBERG || !connector.hasResourceId()) {
      return config;
    }
    String locationPrefix = storageAuthorityLookupLocation(config);
    if (locationPrefix == null) {
      return config;
    }
    ResolveStorageAuthorityResponse response =
        withHeaders(storageAuthorities, ctx)
            .resolveStorageAuthorityForAccountLocation(
                ResolveStorageAuthorityForAccountLocationRequest.newBuilder()
                    .setAccountId(connector.getResourceId().getAccountId())
                    .setLocationPrefix(locationPrefix)
                    .setIncludeCredentials(true)
                    .setRequired(false)
                    .build());
    if (response == null) {
      return config;
    }
    Map<String, String> merged = mergeResolvedStorageConfig(config.options(), response);
    if (merged.equals(config.options())) {
      return config;
    }
    return new ConnectorConfig(config.kind(), config.displayName(), config.uri(), merged, config.auth());
  }

  static Map<String, String> mergeResolvedStorageConfig(
      Map<String, String> options, ResolveStorageAuthorityResponse response) {
    Map<String, String> base = options == null ? Map.of() : options;
    if (response == null
        || (response.getClientSafeConfigCount() == 0 && response.getStorageCredentialsCount() == 0)) {
      return base;
    }
    LinkedHashMap<String, String> merged = new LinkedHashMap<>(base);
    response.getClientSafeConfigMap().forEach((key, value) -> putStorageProperty(merged, key, value));
    if (response.getStorageCredentialsCount() > 0) {
      response
          .getStorageCredentials(0)
          .getConfigMap()
          .forEach((key, value) -> putStorageProperty(merged, key, value));
    }
    return Map.copyOf(merged);
  }

  static String storageAuthorityLookupLocation(ConnectorConfig config) {
    if (config == null || config.kind() != ConnectorConfig.Kind.ICEBERG) {
      return null;
    }
    Map<String, String> options = config.options();
    String source = normalize(options.get("iceberg.source"));
    if ("filesystem".equals(source)) {
      String uri = normalizeS3Location(config.uri(), false);
      if (uri != null) {
        return uri;
      }
      return normalizeS3Location(options.get("metadata-location"), false);
    }
    if ("rest".equals(source)) {
      String warehouse = options.get("warehouse");
      if (warehouse == null || warehouse.isBlank()) {
        return null;
      }
      boolean hasS3Endpoint = isNonBlank(options.get("s3.endpoint"));
      return normalizeWarehouseLocation(warehouse, hasS3Endpoint);
    }
    return null;
  }

  private static void putStorageProperty(Map<String, String> target, String key, String value) {
    if (!isNonBlank(key) || !isNonBlank(value) || "type".equalsIgnoreCase(key)) {
      return;
    }
    target.put(key, value);
  }

  private static String normalizeWarehouseLocation(String warehouse, boolean allowBareWarehouse) {
    String normalized = normalizeS3Location(warehouse, true);
    if (normalized != null) {
      return normalized;
    }
    if (!allowBareWarehouse || warehouse == null || warehouse.isBlank() || warehouse.contains("://")) {
      return null;
    }
    return "s3://" + trimTrailingSlash(warehouse) + "/";
  }

  private static String normalizeS3Location(String location, boolean ensureTrailingSlash) {
    if (!isNonBlank(location)) {
      return null;
    }
    String trimmed = trimTrailingSlash(location.trim());
    String lower = trimmed.toLowerCase(Locale.ROOT);
    if (!lower.startsWith("s3://") && !lower.startsWith("s3a://") && !lower.startsWith("s3n://")) {
      return null;
    }
    return ensureTrailingSlash ? trimmed + "/" : trimmed;
  }

  private static String trimTrailingSlash(String value) {
    String trimmed = value == null ? "" : value.trim();
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  private static String normalize(String value) {
    return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
  }

  private static boolean isNonBlank(String value) {
    return value != null && !value.isBlank();
  }

  private <T extends AbstractStub<T>> T withHeaders(T stub, Optional<ReconcileContext> ctx) {
    return stub.withInterceptors(
        MetadataUtils.newAttachHeadersInterceptor(metadataForContext(ctx)));
  }

  private Metadata metadataForContext(Optional<ReconcileContext> ctx) {
    Metadata metadata = new Metadata();
    ctx.map(ReconcileContext::correlationId).ifPresent(value -> metadata.put(CORRELATION_ID, value));
    Optional<String> token = ctx.flatMap(ReconcileContext::authorizationToken);
    if (token.isEmpty()) {
      token = staticToken;
    }
    token.ifPresent(value -> metadata.put(authHeaderKey(), GrpcReconcilerBackend.withBearerPrefix(value)));
    return metadata;
  }

  private Metadata.Key<String> authHeaderKey() {
    if (headerName.isPresent() && !"authorization".equalsIgnoreCase(headerName.get())) {
      return Metadata.Key.of(headerName.get(), Metadata.ASCII_STRING_MARSHALLER);
    }
    return AUTHORIZATION;
  }
}
