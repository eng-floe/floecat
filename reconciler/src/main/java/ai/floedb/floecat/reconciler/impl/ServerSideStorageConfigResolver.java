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
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ServerSideStorageConfigResolver {
  private static final Logger LOG = Logger.getLogger(ServerSideStorageConfigResolver.class);
  private static final String ACCESS_DELEGATION_HEADER = "X-Iceberg-Access-Delegation";
  private static final String VENDED_CREDENTIALS_MODE = "vended-credentials";
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORRELATION_ID =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  private final Optional<String> headerName;

  @GrpcClient("floecat")
  StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub storageAuthorities;

  @Inject
  public ServerSideStorageConfigResolver(
      @ConfigProperty(name = "floecat.reconciler.authorization.header")
          Optional<String> headerName) {
    this.headerName = headerName.map(String::trim).filter(v -> !v.isBlank());
  }

  ServerSideStorageConfigResolver(
      Optional<String> headerName, Optional<String> ignoredStaticToken) {
    this(headerName);
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
    ResolveStorageAuthorityResponse response;
    try {
      response =
          withHeaders(storageAuthorities, ctx)
              .resolveStorageAuthorityForAccountLocation(
                  ResolveStorageAuthorityForAccountLocationRequest.newBuilder()
                      .setAccountId(connector.getResourceId().getAccountId())
                      .setLocationPrefix(locationPrefix)
                      .setIncludeCredentials(true)
                      .setRequired(false)
                      .build());
    } catch (StatusRuntimeException e) {
      if (!shouldFallbackToVendedCredentials(connector, config, null)) {
        throw e;
      }
      LOG.infof(
          "storage authority lookup failed for iceberg rest connector=%s warehouse=%s; falling"
              + " back to vended credentials: %s",
          connector.getResourceId().getId(), config.options().get("warehouse"), e.getStatus());
      return maybeEnableVendedCredentials(connector, config, null);
    }
    if (response == null) {
      return maybeEnableVendedCredentials(connector, config, null);
    }
    boolean preferVendedCredentials = hasAccessDelegationHeader(config.auth().headerHints());
    Map<String, String> merged =
        mergeResolvedStorageConfig(config.options(), response, !preferVendedCredentials);
    ConnectorConfig resolved =
        merged.equals(config.options())
            ? config
            : new ConnectorConfig(
                config.kind(), config.displayName(), config.uri(), merged, config.auth());
    if (preferVendedCredentials || response.getStorageCredentialsCount() == 0) {
      return maybeEnableVendedCredentials(connector, resolved, response);
    }
    return resolved;
  }

  static Map<String, String> mergeResolvedStorageConfig(
      Map<String, String> options, ResolveStorageAuthorityResponse response) {
    return mergeResolvedStorageConfig(options, response, true);
  }

  static Map<String, String> mergeResolvedStorageConfig(
      Map<String, String> options,
      ResolveStorageAuthorityResponse response,
      boolean includeStorageCredentials) {
    Map<String, String> base = options == null ? Map.of() : options;
    if (response == null
        || (response.getClientSafeConfigCount() == 0
            && (response.getStorageCredentialsCount() == 0 || !includeStorageCredentials))) {
      return base;
    }
    LinkedHashMap<String, String> merged = new LinkedHashMap<>(base);
    response
        .getClientSafeConfigMap()
        .forEach((key, value) -> putStorageProperty(merged, key, value));
    if (includeStorageCredentials && response.getStorageCredentialsCount() > 0) {
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
    if (!allowBareWarehouse
        || warehouse == null
        || warehouse.isBlank()
        || warehouse.contains("://")) {
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

  private static ConnectorConfig maybeEnableVendedCredentials(
      Connector connector, ConnectorConfig config, ResolveStorageAuthorityResponse response) {
    if (!shouldFallbackToVendedCredentials(connector, config, response)) {
      return config;
    }
    Map<String, String> headerHints = new LinkedHashMap<>(config.auth().headerHints());
    if (hasAccessDelegationHeader(headerHints)) {
      return config;
    }
    headerHints.put(ACCESS_DELEGATION_HEADER, VENDED_CREDENTIALS_MODE);
    LOG.infof(
        "enabling vended credential fallback connector=%s source=%s warehouse=%s existingHeaders=%s",
        connector.getResourceId().getId(),
        config.options().get("iceberg.source"),
        config.options().get("warehouse"),
        config.auth().headerHints().keySet());
    return new ConnectorConfig(
        config.kind(),
        config.displayName(),
        config.uri(),
        config.options(),
        new ConnectorConfig.Auth(config.auth().scheme(), config.auth().props(), headerHints));
  }

  private static boolean shouldFallbackToVendedCredentials(
      Connector connector, ConnectorConfig config, ResolveStorageAuthorityResponse response) {
    if (connector == null || config == null || config.kind() != ConnectorConfig.Kind.ICEBERG) {
      return false;
    }
    if (!"rest".equals(normalize(config.options().get("iceberg.source")))) {
      return false;
    }
    return hasAccessDelegationHeader(config.auth().headerHints())
        || response == null
        || response.getStorageCredentialsCount() == 0;
  }

  private static boolean hasAccessDelegationHeader(Map<String, String> headerHints) {
    if (headerHints == null || headerHints.isEmpty()) {
      return false;
    }
    return headerHints.keySet().stream()
        .filter(ServerSideStorageConfigResolver::isNonBlank)
        .map(value -> value.trim().toLowerCase(Locale.ROOT))
        .anyMatch("x-iceberg-access-delegation"::equals);
  }

  private <T extends AbstractStub<T>> T withHeaders(T stub, Optional<ReconcileContext> ctx) {
    return stub.withInterceptors(
        MetadataUtils.newAttachHeadersInterceptor(metadataForContext(ctx)));
  }

  private Metadata metadataForContext(Optional<ReconcileContext> ctx) {
    Metadata metadata = new Metadata();
    ctx.map(ReconcileContext::correlationId)
        .ifPresent(value -> metadata.put(CORRELATION_ID, value));
    ctx.flatMap(ReconcileContext::authorizationToken)
        .ifPresent(
            value -> metadata.put(authHeaderKey(), GrpcReconcilerBackend.withBearerPrefix(value)));
    return metadata;
  }

  private Metadata.Key<String> authHeaderKey() {
    if (headerName.isPresent() && !"authorization".equalsIgnoreCase(headerName.get())) {
      return Metadata.Key.of(headerName.get(), Metadata.ASCII_STRING_MARSHALLER);
    }
    return AUTHORIZATION;
  }
}
