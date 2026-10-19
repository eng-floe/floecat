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

package ai.floedb.floecat.storage.aws.secrets;

import ai.floedb.floecat.aws.RefreshingAwsClient;
import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.cache.CaffeineCoalescingExpiringCache;
import ai.floedb.floecat.cache.CoalescingExpiringCache;
import ai.floedb.floecat.storage.aws.AwsClients;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import io.quarkus.arc.profile.IfBuildProfile;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.CreateSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.DeleteSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.DescribeSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.InvalidRequestException;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.ResourceExistsException;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
import software.amazon.awssdk.services.secretsmanager.model.Tag;
import software.amazon.awssdk.services.secretsmanager.model.TagResourceRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

@ApplicationScoped
@IfBuildProfile("prod")
public class ProdSecretsManager implements SecretsManager {
  private static final String ACCOUNT_ID_TAG = "AccountId";
  private static final int MAX_ROLE_SESSION_LENGTH = 64;
  private static final String ROLE_SESSION_PREFIX = "floecat-";
  private static final long DEFAULT_RESULT_CACHE_MAX_ENTRIES = 1024L;
  private static final Duration DEFAULT_RESULT_CACHE_TTL = Duration.ofSeconds(30);

  @ConfigProperty(name = "floecat.secrets.aws.role-arn")
  Optional<String> roleArn = Optional.empty();

  private final RefreshingAwsClient<SecretsManagerClient> secretsClient;
  private final RefreshingAwsClient<StsClient> stsClient;
  private final ConcurrentMap<String, RefreshingAwsClient<SecretsManagerClient>> perAccountClients =
      new ConcurrentHashMap<>();
  private final Object perAccountClientLock = new Object();
  private final Object stsRefreshLock = new Object();
  private final CoalescingExpiringCache<SecretCacheKey, Optional<byte[]>> resultCache;

  @Inject
  public ProdSecretsManager(
      AwsClients awsClients,
      @ConfigProperty(name = "floecat.secrets.cache.max-entries", defaultValue = "1024")
          long resultCacheMaxEntries,
      @ConfigProperty(name = "floecat.secrets.cache.ttl", defaultValue = "PT30S")
          Duration resultCacheTtl,
      @SecretsManagerResultCache Instance<CacheEvents> resultCacheEvents) {
    this(
        awsClients,
        resultCacheMaxEntries,
        resultCacheTtl,
        System::nanoTime,
        resultCacheEvents.isResolvable() ? resultCacheEvents.get() : CacheEvents.none());
  }

  ProdSecretsManager(AwsClients awsClients) {
    this(
        awsClients,
        DEFAULT_RESULT_CACHE_MAX_ENTRIES,
        DEFAULT_RESULT_CACHE_TTL,
        System::nanoTime,
        CacheEvents.none());
  }

  private ProdSecretsManager(
      AwsClients awsClients,
      long resultCacheMaxEntries,
      Duration resultCacheTtl,
      LongSupplier resultCacheTicker,
      CacheEvents resultCacheEvents) {
    this.awsClients = awsClients;
    this.secretsClient =
        RefreshingAwsClient.withResourceFactory(
            "Secrets Manager", () -> awsClients.newSecretsManagerClientResource());
    this.stsClient =
        RefreshingAwsClient.withResourceFactory("STS", () -> awsClients.newStsClientResource());
    this.resultCache =
        buildResultCache(
            resultCacheMaxEntries, resultCacheTtl, resultCacheTicker, resultCacheEvents);
  }

  ProdSecretsManager(SecretsManagerClient secretsClient, StsClient stsClient) {
    this(
        null,
        RefreshingAwsClient.clientResource(secretsClient),
        RefreshingAwsClient.clientResource(stsClient),
        DEFAULT_RESULT_CACHE_MAX_ENTRIES,
        DEFAULT_RESULT_CACHE_TTL,
        System::nanoTime,
        CacheEvents.none());
  }

  ProdSecretsManager(
      SecretsManagerClient secretsClient,
      StsClient stsClient,
      long resultCacheMaxEntries,
      Duration resultCacheTtl,
      LongSupplier resultCacheTicker) {
    this(
        null,
        RefreshingAwsClient.clientResource(secretsClient),
        RefreshingAwsClient.clientResource(stsClient),
        resultCacheMaxEntries,
        resultCacheTtl,
        resultCacheTicker,
        CacheEvents.none());
  }

  ProdSecretsManager(
      AwsClients awsClients, SecretsManagerClient secretsClient, StsClient stsClient) {
    this(
        awsClients,
        RefreshingAwsClient.clientResource(secretsClient),
        RefreshingAwsClient.clientResource(stsClient),
        DEFAULT_RESULT_CACHE_MAX_ENTRIES,
        DEFAULT_RESULT_CACHE_TTL,
        System::nanoTime,
        CacheEvents.none());
  }

  private ProdSecretsManager(
      AwsClients awsClients,
      RefreshingAwsClient.ClientResource<SecretsManagerClient> secretsClient,
      RefreshingAwsClient.ClientResource<StsClient> stsClient,
      long resultCacheMaxEntries,
      Duration resultCacheTtl,
      LongSupplier resultCacheTicker,
      CacheEvents resultCacheEvents) {
    this.awsClients = awsClients;
    AtomicReference<RefreshingAwsClient.ClientResource<SecretsManagerClient>> initialSecretsClient =
        new AtomicReference<>(secretsClient);
    AtomicReference<RefreshingAwsClient.ClientResource<StsClient>> initialStsClient =
        new AtomicReference<>(stsClient);
    this.secretsClient =
        RefreshingAwsClient.withResourceFactory(
            "Secrets Manager",
            () ->
                initialOrBuildResource(
                    "Secrets Manager",
                    initialSecretsClient,
                    () -> awsClients.newSecretsManagerClientResource()));
    this.stsClient =
        RefreshingAwsClient.withResourceFactory(
            "STS",
            () ->
                initialOrBuildResource(
                    "STS", initialStsClient, () -> awsClients.newStsClientResource()));
    this.resultCache =
        buildResultCache(
            resultCacheMaxEntries, resultCacheTtl, resultCacheTicker, resultCacheEvents);
  }

  private final AwsClients awsClients;

  @PreDestroy
  void shutdown() {
    resultCache.invalidateAll();
    synchronized (perAccountClientLock) {
      closePerAccountClients();
      stsClient.close();
    }
    secretsClient.close();
  }

  @Override
  public void put(String accountId, String secretType, String secretId, byte[] payload) {
    mutateAndInvalidate(
        accountId,
        secretType,
        secretId,
        client -> {
          putOnce(client, accountId, secretType, secretId, payload);
          return null;
        });
  }

  @Override
  public boolean putIfAbsent(String accountId, String secretType, String secretId, byte[] payload) {
    return mutateAndInvalidate(
        accountId,
        secretType,
        secretId,
        client -> putIfAbsentOnce(client, accountId, secretType, secretId, payload));
  }

  @Override
  public Optional<byte[]> get(String accountId, String secretType, String secretId) {
    SecretCacheKey cacheKey = SecretCacheKey.of(accountId, secretType, secretId);
    Optional<byte[]> cached =
        resultCache.get(
            cacheKey,
            key ->
                copy(
                    withClient(
                        key.accountId(),
                        client ->
                            getOnce(client, key.accountId(), key.secretType(), key.secretId()))));
    return copy(cached);
  }

  @Override
  public void update(String accountId, String secretType, String secretId, byte[] payload) {
    mutateAndInvalidate(
        accountId,
        secretType,
        secretId,
        client -> {
          updateOnce(client, accountId, secretType, secretId, payload);
          return null;
        });
  }

  @Override
  public void delete(String accountId, String secretType, String secretId) {
    mutateAndInvalidate(
        accountId,
        secretType,
        secretId,
        client -> {
          deleteOnce(client, accountId, secretType, secretId);
          return null;
        });
  }

  @Override
  public void deleteImmediately(String accountId, String secretType, String secretId) {
    mutateAndInvalidate(
        accountId,
        secretType,
        secretId,
        client -> {
          deleteImmediatelyOnce(client, accountId, secretType, secretId);
          return null;
        });
  }

  long resultCacheSize() {
    return resultCache.estimatedSize();
  }

  private <T> T mutateAndInvalidate(
      String accountId, String secretType, String secretId, ClientOperation<T> operation) {
    SecretCacheKey cacheKey = SecretCacheKey.of(accountId, secretType, secretId);
    try {
      return withClient(accountId, operation);
    } finally {
      resultCache.invalidate(cacheKey);
    }
  }

  private static CoalescingExpiringCache<SecretCacheKey, Optional<byte[]>> buildResultCache(
      long maximumEntries, Duration ttl, LongSupplier ticker, CacheEvents events) {
    if (maximumEntries <= 0) {
      throw new IllegalArgumentException("result cache maximum entries must be positive");
    }
    if (ttl == null || ttl.isZero() || ttl.isNegative()) {
      throw new IllegalArgumentException("result cache TTL must be positive");
    }
    Objects.requireNonNull(ticker, "resultCacheTicker");
    return CaffeineCoalescingExpiringCache.create(
        maximumEntries,
        ignored -> ttl,
        ticker,
        Objects.requireNonNull(events, "resultCacheEvents"));
  }

  private static Optional<byte[]> copy(Optional<byte[]> value) {
    return value.map(payload -> Arrays.copyOf(payload, payload.length));
  }

  private record SecretCacheKey(String accountId, String secretType, String secretId) {
    private static SecretCacheKey of(String accountId, String secretType, String secretId) {
      SecretsManager.buildSecretKey(accountId, secretType, secretId);
      return new SecretCacheKey(accountId, secretType, secretId);
    }
  }

  private void putOnce(
      SecretsManagerClient client,
      String accountId,
      String secretType,
      String secretId,
      byte[] payload) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    String encoded = encodePayload(payload);
    List<Tag> tags = buildTags(accountId, secretName);
    try {
      client.createSecret(
          CreateSecretRequest.builder().name(secretName).secretString(encoded).tags(tags).build());
    } catch (ResourceExistsException exists) {
      client.putSecretValue(
          PutSecretValueRequest.builder().secretId(secretName).secretString(encoded).build());
      tagSecret(client, secretName, tags);
    }
  }

  private boolean putIfAbsentOnce(
      SecretsManagerClient client,
      String accountId,
      String secretType,
      String secretId,
      byte[] payload) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    String encoded = encodePayload(payload);
    List<Tag> tags = buildTags(accountId, secretName);
    try {
      client.createSecret(
          CreateSecretRequest.builder().name(secretName).secretString(encoded).tags(tags).build());
      return true;
    } catch (ResourceExistsException exists) {
      return false;
    } catch (InvalidRequestException unavailable) {
      // AWS retains a force-deleted secret name briefly while background deletion completes.
      // Treat that immutable generation as occupied so callers can reserve the next generation.
      if (isPendingDeletion(unavailable)) return false;
      throw unavailable;
    }
  }

  private Optional<byte[]> getOnce(
      SecretsManagerClient client, String accountId, String secretType, String secretId) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    try {
      var response =
          client.getSecretValue(GetSecretValueRequest.builder().secretId(secretName).build());
      if (response == null
          || response.secretString() == null
          || response.secretString().isBlank()) {
        return Optional.empty();
      }
      return Optional.of(decodePayload(response.secretString()));
    } catch (ResourceNotFoundException missing) {
      return Optional.empty();
    } catch (InvalidRequestException unavailable) {
      if (isPendingDeletion(unavailable)) return Optional.empty();
      throw unavailable;
    }
  }

  private void updateOnce(
      SecretsManagerClient client,
      String accountId,
      String secretType,
      String secretId,
      byte[] payload) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    String encoded = encodePayload(payload);
    List<Tag> tags = buildTags(accountId, secretName);
    try {
      client.putSecretValue(
          PutSecretValueRequest.builder().secretId(secretName).secretString(encoded).build());
      tagSecret(client, secretName, tags);
    } catch (ResourceNotFoundException missing) {
      client.createSecret(
          CreateSecretRequest.builder().name(secretName).secretString(encoded).tags(tags).build());
    }
  }

  private void deleteOnce(
      SecretsManagerClient client, String accountId, String secretType, String secretId) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    try {
      client.deleteSecret(
          DeleteSecretRequest.builder().secretId(secretName).recoveryWindowInDays(7L).build());
    } catch (ResourceNotFoundException ignored) {
    } catch (InvalidRequestException invalidState) {
      try {
        var description =
            client.describeSecret(DescribeSecretRequest.builder().secretId(secretName).build());
        if (description != null && description.deletedDate() != null) {
          return;
        }
      } catch (ResourceNotFoundException ignored) {
        return;
      } catch (RuntimeException describeFailure) {
        invalidState.addSuppressed(describeFailure);
      }
      throw invalidState;
    }
  }

  private void deleteImmediatelyOnce(
      SecretsManagerClient client, String accountId, String secretType, String secretId) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    try {
      client.deleteSecret(
          DeleteSecretRequest.builder()
              .secretId(secretName)
              .forceDeleteWithoutRecovery(true)
              .build());
    } catch (ResourceNotFoundException ignored) {
    }
  }

  private static boolean isPendingDeletion(InvalidRequestException failure) {
    String message = failure.getMessage();
    if (message == null) return false;
    String normalized = message.toLowerCase(java.util.Locale.ROOT);
    return normalized.contains("marked for deletion")
        || normalized.contains("scheduled for deletion");
  }

  private static List<Tag> buildTags(String accountId, String secretName) {
    return List.of(Tag.builder().key(ACCOUNT_ID_TAG).value(accountId).build());
  }

  private void tagSecret(SecretsManagerClient client, String secretName, List<Tag> tags) {
    if (tags == null || tags.isEmpty()) {
      return;
    }
    client.tagResource(TagResourceRequest.builder().secretId(secretName).tags(tags).build());
  }

  private RefreshingAwsClient<SecretsManagerClient> clientForAccount(String accountId) {
    String role = roleArn.map(String::trim).filter(value -> !value.isBlank()).orElse("");
    if (role.isEmpty()) {
      return secretsClient;
    }
    synchronized (perAccountClientLock) {
      RefreshingAwsClient<SecretsManagerClient> existing = perAccountClients.get(accountId);
      if (existing != null) {
        return existing;
      }
      RefreshingAwsClient<SecretsManagerClient> client =
          RefreshingAwsClient.withResourceFactory(
              "Secrets Manager account " + accountId,
              () -> newPerAccountSecretsClientResource(accountId, role),
              failure -> {
                if (isStsCredentialPoolFailure(failure)) {
                  invalidateStsAndDependentClients(accountId);
                }
              },
              stsRefreshLock);
      perAccountClients.put(accountId, client);
      return client;
    }
  }

  private <T> T withClient(String accountId, ClientOperation<T> operation) {
    return clientForAccount(accountId).callUnchecked(operation::run);
  }

  private RefreshingAwsClient.ClientResource<SecretsManagerClient>
      newPerAccountSecretsClientResource(String accountId, String role) {
    StsAssumeRoleCredentialsProvider creds =
        StsAssumeRoleCredentialsProvider.builder()
            .stsClient(stsClient.current())
            .refreshRequest(
                builder ->
                    builder
                        .roleArn(role)
                        .roleSessionName(buildRoleSessionName(accountId))
                        .tags(
                            software.amazon.awssdk.services.sts.model.Tag.builder()
                                .key(ACCOUNT_ID_TAG)
                                .value(accountId)
                                .build()))
            .build();
    try {
      SecretsManagerClient client =
          awsClients != null
              ? awsClients.secretsManagerClient(creds)
              : SecretsManagerClient.builder().credentialsProvider(creds).build();
      return RefreshingAwsClient.clientResource(
          client, RefreshingAwsClient.closeableResource(creds));
    } catch (RuntimeException | Error e) {
      RefreshingAwsClient.closeQuietly(RefreshingAwsClient.closeableResource(creds));
      throw e;
    }
  }

  private void invalidateStsAndDependentClients(String currentAccountId) {
    synchronized (perAccountClientLock) {
      stsClient.invalidate();
      perAccountClients
          .entrySet()
          .forEach(
              entry -> {
                if (!Objects.equals(entry.getKey(), currentAccountId)) {
                  entry.getValue().invalidate();
                }
              });
    }
  }

  private <T extends AutoCloseable> RefreshingAwsClient.ClientResource<T> initialOrBuildResource(
      String name,
      AtomicReference<RefreshingAwsClient.ClientResource<T>> initial,
      java.util.function.Supplier<RefreshingAwsClient.ClientResource<T>> factory) {
    RefreshingAwsClient.ClientResource<T> value = initial.getAndSet(null);
    if (value != null) {
      return value;
    }
    if (awsClients == null) {
      throw new IllegalStateException(name + " client cannot be refreshed");
    }
    return factory.get();
  }

  private void closePerAccountClients() {
    for (RefreshingAwsClient<SecretsManagerClient> client : perAccountClients.values()) {
      client.close();
    }
    perAccountClients.clear();
  }

  private static boolean isStsCredentialPoolFailure(Throwable failure) {
    Throwable current = failure;
    while (current != null) {
      if (current instanceof SdkClientException) {
        String message = current.getMessage();
        if (message != null && message.toLowerCase().contains("credential")) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
  }

  private static String buildRoleSessionName(String accountId) {
    String raw = ROLE_SESSION_PREFIX + accountId;
    if (raw.length() <= MAX_ROLE_SESSION_LENGTH) {
      return raw;
    }
    return raw.substring(0, MAX_ROLE_SESSION_LENGTH);
  }

  private static String encodePayload(byte[] payload) {
    return Base64.getEncoder().encodeToString(payload == null ? new byte[0] : payload);
  }

  private static byte[] decodePayload(String encoded) {
    try {
      return Base64.getDecoder().decode(encoded);
    } catch (IllegalArgumentException ignored) {
      return encoded.getBytes(StandardCharsets.UTF_8);
    }
  }

  @FunctionalInterface
  private interface ClientOperation<T> {
    T run(SecretsManagerClient client);
  }
}
