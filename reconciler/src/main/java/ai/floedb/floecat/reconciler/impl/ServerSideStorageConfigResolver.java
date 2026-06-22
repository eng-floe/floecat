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

import ai.floedb.floecat.connector.common.auth.RefreshingAwsCredentialsProviderRegistry;
import ai.floedb.floecat.connector.common.auth.ResolvedStorageCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthoritiesGrpc;
import ai.floedb.floecat.storage.rpc.StorageCredentialUsage;
import ai.floedb.floecat.storage.rpc.VendStorageCredentialsRequest;
import ai.floedb.floecat.storage.rpc.VendedStorageCredential;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class ServerSideStorageConfigResolver {
  public record ResolvedConnectorConfig(ConnectorConfig config, Runnable closeAction)
      implements AutoCloseable {
    public ResolvedConnectorConfig {
      config = config == null ? emptyConfig() : config;
      closeAction = closeAction == null ? () -> {} : closeAction;
    }

    @Override
    public void close() {
      closeAction.run();
    }

    private static ConnectorConfig emptyConfig() {
      return new ConnectorConfig(
          ConnectorConfig.Kind.ICEBERG,
          "",
          "",
          Map.of(),
          new ConnectorConfig.Auth("none", Map.of(), Map.of()));
    }
  }

  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORRELATION_ID =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  private final Optional<String> headerName;

  @GrpcClient("floecat")
  StorageAuthoritiesGrpc.StorageAuthoritiesBlockingStub storageAuthorities;

  @Inject
  public ServerSideStorageConfigResolver(
      @ConfigProperty(name = "floecat.interceptor.session.header")
          Optional<String> sessionHeaderName,
      @ConfigProperty(name = "floecat.reconciler.authorization.header")
          Optional<String> headerName) {
    this.headerName =
        ReconcileRpcAuthHeaderSupport.resolveHeaderName(sessionHeaderName, headerName);
  }

  public ConnectorConfig resolve(Connector connector, ConnectorConfig config) {
    return resolve(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        connector,
        config,
        false);
  }

  public ConnectorConfig resolve(
      Optional<ReconcileContext> ctx, Connector connector, ConnectorConfig config) {
    return resolve(
        ctx.map(ReconcileContext::correlationId),
        ctx.flatMap(ReconcileContext::authorizationToken),
        ctx.flatMap(ReconcileContext::executionJobId),
        ctx.flatMap(ReconcileContext::executionLeaseEpoch),
        Optional.empty(),
        connector,
        config,
        false);
  }

  public ConnectorConfig resolveWithAuthorization(
      Optional<String> authorizationToken,
      Optional<String> executionJobId,
      Optional<String> executionLeaseEpoch,
      Optional<String> storageLocation,
      Connector connector,
      ConnectorConfig config) {
    return resolve(
        Optional.empty(),
        authorizationToken,
        executionJobId,
        executionLeaseEpoch,
        storageLocation,
        connector,
        config,
        false);
  }

  public ResolvedConnectorConfig resolveManagedWithAuthorization(
      Optional<String> authorizationToken,
      Optional<String> executionJobId,
      Optional<String> executionLeaseEpoch,
      Optional<String> storageLocation,
      Connector connector,
      ConnectorConfig config) {
    return resolveManaged(
        Optional.empty(),
        authorizationToken,
        executionJobId,
        executionLeaseEpoch,
        storageLocation,
        connector,
        config);
  }

  private ResolvedConnectorConfig resolveManaged(
      Optional<String> correlationId,
      Optional<String> authorizationToken,
      Optional<String> executionJobId,
      Optional<String> executionLeaseEpoch,
      Optional<String> storageLocation,
      Connector connector,
      ConnectorConfig config) {
    ConnectorConfig resolved =
        resolve(
            correlationId,
            authorizationToken,
            executionJobId,
            executionLeaseEpoch,
            storageLocation,
            connector,
            config,
            true);
    String providerId =
        resolved == null
            ? null
            : resolved.options().get(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID);
    if (!isNonBlank(providerId)) {
      return new ResolvedConnectorConfig(resolved, () -> {});
    }
    String capturedProviderId = providerId;
    return new ResolvedConnectorConfig(
        resolved, () -> RefreshingAwsCredentialsProviderRegistry.unregister(capturedProviderId));
  }

  private ConnectorConfig resolve(
      Optional<String> correlationId,
      Optional<String> authorizationToken,
      Optional<String> executionJobId,
      Optional<String> executionLeaseEpoch,
      Optional<String> storageLocation,
      Connector connector,
      ConnectorConfig config,
      boolean refreshableExecutionCredentials) {
    if (connector == null || config == null || connector.getKindValue() == 0) {
      return config;
    }
    if (config.kind() == ConnectorConfig.Kind.DELTA) {
      if (!connector.hasResourceId()) {
        return config;
      }
      String locationPrefix = storageAuthorityLookupLocation(storageLocation, config);
      if (locationPrefix == null) {
        return config;
      }
      ResolveStorageAuthorityResponse response =
          withHeaders(storageAuthorities, correlationId, authorizationToken)
              .vendStorageCredentials(
                  resolveRequest(
                      connector.getResourceId().getAccountId(),
                      locationPrefix,
                      executionJobId,
                      executionLeaseEpoch));
      if (response == null) {
        return config;
      }
      Map<String, String> merged =
          mergeResolvedStorageConfig(
              config.options(),
              response,
              true,
              refreshableExecutionCredentials
                  && executionJobId.isPresent()
                  && executionLeaseEpoch.isPresent(),
              () ->
                  refreshExecutionBoundCredentials(
                      correlationId,
                      authorizationToken,
                      connector.getResourceId().getAccountId(),
                      locationPrefix,
                      executionJobId,
                      executionLeaseEpoch));
      return merged.equals(config.options())
          ? config
          : new ConnectorConfig(
              config.kind(), config.displayName(), config.uri(), merged, config.auth());
    }
    if (config.kind() != ConnectorConfig.Kind.ICEBERG || !connector.hasResourceId()) {
      return config;
    }
    String locationPrefix = storageAuthorityLookupLocation(storageLocation, config);
    if (locationPrefix == null) {
      return config;
    }
    ResolveStorageAuthorityResponse response =
        withHeaders(storageAuthorities, correlationId, authorizationToken)
            .vendStorageCredentials(
                resolveRequest(
                    connector.getResourceId().getAccountId(),
                    locationPrefix,
                    executionJobId,
                    executionLeaseEpoch));
    if (response == null) {
      return config;
    }
    Map<String, String> merged =
        mergeResolvedStorageConfig(
            config.options(),
            response,
            true,
            refreshableExecutionCredentials
                && executionJobId.isPresent()
                && executionLeaseEpoch.isPresent(),
            () ->
                refreshExecutionBoundCredentials(
                    correlationId,
                    authorizationToken,
                    connector.getResourceId().getAccountId(),
                    locationPrefix,
                    executionJobId,
                    executionLeaseEpoch));
    ConnectorConfig resolved =
        merged.equals(config.options())
            ? config
            : new ConnectorConfig(
                config.kind(), config.displayName(), config.uri(), merged, config.auth());
    return resolved;
  }

  private static VendStorageCredentialsRequest resolveRequest(
      String accountId,
      String locationPrefix,
      Optional<String> executionJobId,
      Optional<String> executionLeaseEpoch) {
    VendStorageCredentialsRequest.Builder request =
        VendStorageCredentialsRequest.newBuilder()
            .setAccountId(accountId)
            .setLocationPrefix(locationPrefix)
            .setUsage(StorageCredentialUsage.SCU_SERVER);
    if (executionJobId.isPresent() ^ executionLeaseEpoch.isPresent()) {
      throw new IllegalArgumentException(
          "executionJobId and executionLeaseEpoch must both be present together");
    }
    if (executionJobId.isPresent() && executionLeaseEpoch.isPresent()) {
      request.setExecutionBinding(
          ai.floedb.floecat.storage.rpc.ExecutionBinding.newBuilder()
              .setReconcileLease(
                  ai.floedb.floecat.storage.rpc.ReconcileLeaseBinding.newBuilder()
                      .setJobId(executionJobId.orElse(""))
                      .setLeaseEpoch(executionLeaseEpoch.orElse("")))
              .build());
    }
    return request.build();
  }

  static Map<String, String> mergeResolvedStorageConfig(
      Map<String, String> options, ResolveStorageAuthorityResponse response) {
    return mergeResolvedStorageConfig(options, response, true, false, null);
  }

  static Map<String, String> mergeResolvedStorageConfig(
      Map<String, String> options,
      ResolveStorageAuthorityResponse response,
      boolean includeStorageCredentials) {
    return mergeResolvedStorageConfig(options, response, includeStorageCredentials, false, null);
  }

  static Map<String, String> mergeResolvedStorageConfig(
      Map<String, String> options,
      ResolveStorageAuthorityResponse response,
      boolean includeStorageCredentials,
      boolean refreshableExecutionCredentials,
      java.util.function.Supplier<ResolvedStorageCredentials> refresher) {
    Map<String, String> base = options == null ? Map.of() : options;
    if (response == null) {
      return base;
    }
    LinkedHashMap<String, String> merged = new LinkedHashMap<>(base);
    if (refreshableExecutionCredentials) {
      clearStorageCredentialProperties(merged);
    }
    response
        .getClientSafeConfigMap()
        .forEach((key, value) -> putStorageProperty(merged, key, value));
    if (!includeStorageCredentials || response.getStorageCredentialsCount() == 0) {
      return Map.copyOf(merged);
    }
    if (refreshableExecutionCredentials) {
      ResolvedStorageCredentials temporaryExecutionCredentials =
          resolvedStorageCredentials(response)
              .filter(ServerSideStorageConfigResolver::isExecutionBoundStorageCredential)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Execution-bound storage credentials must include access key and secret key"));
      if (isRefreshableExecutionCredential(temporaryExecutionCredentials)) {
        String providerId =
            RefreshingAwsCredentialsProviderRegistry.register(
                temporaryExecutionCredentials, refresher);
        merged.putAll(RefreshingAwsCredentialsProviderRegistry.propertiesFor(providerId));
      } else {
        merged.putAll(temporaryExecutionCredentials.asS3Properties());
      }
      return Map.copyOf(merged);
    }

    response
        .getStorageCredentials(0)
        .getConfigMap()
        .forEach((key, value) -> putStorageProperty(merged, key, value));
    return Map.copyOf(merged);
  }

  private ResolvedStorageCredentials refreshExecutionBoundCredentials(
      Optional<String> correlationId,
      Optional<String> authorizationToken,
      String accountId,
      String locationPrefix,
      Optional<String> executionJobId,
      Optional<String> executionLeaseEpoch) {
    ResolveStorageAuthorityResponse response =
        withHeaders(storageAuthorities, correlationId, authorizationToken)
            .vendStorageCredentials(
                resolveRequest(accountId, locationPrefix, executionJobId, executionLeaseEpoch));
    return resolvedStorageCredentials(response)
        .filter(ServerSideStorageConfigResolver::isRefreshableExecutionCredential)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Expected refreshable execution-bound credentials with access key, secret key,"
                        + " session token, and expiresAt"));
  }

  static Optional<ResolvedStorageCredentials> resolvedStorageCredentials(
      ResolveStorageAuthorityResponse response) {
    if (response == null || response.getStorageCredentialsCount() == 0) {
      return Optional.empty();
    }
    VendedStorageCredential credential = response.getStorageCredentials(0);
    String accessKeyId = blankToNull(credential.getConfigMap().get("s3.access-key-id"));
    String secretAccessKey = blankToNull(credential.getConfigMap().get("s3.secret-access-key"));
    String sessionToken = blankToNull(credential.getConfigMap().get("s3.session-token"));
    if (!isNonBlank(accessKeyId) || !isNonBlank(secretAccessKey)) {
      return Optional.empty();
    }
    Instant expiresAt =
        credential.hasExpiresAt()
            ? Instant.ofEpochMilli(
                com.google.protobuf.util.Timestamps.toMillis(credential.getExpiresAt()))
            : null;
    return Optional.of(
        new ResolvedStorageCredentials(accessKeyId, secretAccessKey, sessionToken, expiresAt));
  }

  static String storageAuthorityLookupLocation(
      Optional<String> storageLocation, ConnectorConfig config) {
    if (config == null) {
      return null;
    }
    if (storageLocation != null && storageLocation.isPresent()) {
      String explicitLocation = normalizeS3Location(storageLocation.orElse(null), false);
      if (explicitLocation != null) {
        return explicitLocation;
      }
    }
    Map<String, String> options = config.options();
    if (config.kind() == ConnectorConfig.Kind.DELTA) {
      String tableRoot = options.get("delta.table-root");
      if (isNonBlank(tableRoot)) {
        return normalizeS3Location(tableRoot, false);
      }
      return null;
    }
    if (config.kind() != ConnectorConfig.Kind.ICEBERG) {
      return null;
    }
    String source = normalize(options.get("iceberg.source"));
    if ("filesystem".equals(source)) {
      return normalizeS3Location(config.uri(), false);
    }
    if ("rest".equals(source)) {
      return null;
    }
    return null;
  }

  static String storageAuthorityLookupLocation(ConnectorConfig config) {
    return storageAuthorityLookupLocation(Optional.empty(), config);
  }

  private static void putStorageProperty(Map<String, String> target, String key, String value) {
    if (!isNonBlank(key) || !isNonBlank(value) || "type".equalsIgnoreCase(key)) {
      return;
    }
    target.put(key, value);
  }

  private static void clearStorageCredentialProperties(Map<String, String> target) {
    target.remove("s3.access-key-id");
    target.remove("s3.secret-access-key");
    target.remove("s3.session-token");
    target.remove(RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID);
    target.remove(RefreshingAwsCredentialsProviderRegistry.PROPERTY_PROVIDER_ID);
    target.remove(RefreshingAwsCredentialsProviderRegistry.ICEBERG_CLIENT_CREDENTIALS_PROVIDER);
    target
        .keySet()
        .removeIf(
            key ->
                key != null
                    && key.startsWith(
                        RefreshingAwsCredentialsProviderRegistry
                            .ICEBERG_CLIENT_CREDENTIALS_PROVIDER_PREFIX));
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

  private static String blankToNull(String value) {
    return isNonBlank(value) ? value.trim() : null;
  }

  private static boolean isRefreshableExecutionCredential(ResolvedStorageCredentials credentials) {
    return credentials != null
        && isNonBlank(credentials.accessKeyId())
        && isNonBlank(credentials.secretAccessKey())
        && isNonBlank(credentials.sessionToken())
        && credentials.expiresAt() != null;
  }

  private static boolean isExecutionBoundStorageCredential(ResolvedStorageCredentials credentials) {
    return credentials != null
        && isNonBlank(credentials.accessKeyId())
        && isNonBlank(credentials.secretAccessKey());
  }

  private <T extends AbstractStub<T>> T withHeaders(
      T stub, Optional<String> correlationId, Optional<String> authorizationToken) {
    return stub.withInterceptors(
        MetadataUtils.newAttachHeadersInterceptor(
            metadataForContext(correlationId, authorizationToken)));
  }

  private Metadata metadataForContext(
      Optional<String> correlationId, Optional<String> authorizationToken) {
    Metadata metadata = new Metadata();
    correlationId.ifPresent(value -> metadata.put(CORRELATION_ID, value));
    authorizationToken.ifPresent(
        value -> metadata.put(authHeaderKey(), GrpcReconcilerBackend.withBearerPrefix(value)));
    return metadata;
  }

  private Metadata.Key<String> authHeaderKey() {
    if (headerName.isPresent() && !"authorization".equalsIgnoreCase(headerName.get())) {
      return ReconcileRpcAuthHeaderSupport.headerKey(headerName.get());
    }
    return AUTHORIZATION;
  }
}
