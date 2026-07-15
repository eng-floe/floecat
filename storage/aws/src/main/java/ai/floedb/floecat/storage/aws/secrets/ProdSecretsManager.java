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
import ai.floedb.floecat.storage.aws.AwsClients;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import io.quarkus.arc.profile.IfBuildProfile;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.CreateSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.DeleteSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
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

  @ConfigProperty(name = "floecat.secrets.aws.role-arn")
  Optional<String> roleArn = Optional.empty();

  private final RefreshingAwsClient<SecretsManagerClient> secretsClient;
  private final RefreshingAwsClient<StsClient> stsClient;
  private final ConcurrentMap<String, RefreshingAwsClient<SecretsManagerClient>> perAccountClients =
      new ConcurrentHashMap<>();
  private final Object perAccountClientLock = new Object();
  private final Object stsRefreshLock = new Object();

  @Inject
  public ProdSecretsManager(AwsClients awsClients) {
    this(awsClients, awsClients.secretsManagerClient(), awsClients.stsClient());
  }

  ProdSecretsManager(SecretsManagerClient secretsClient, StsClient stsClient) {
    this(null, secretsClient, stsClient);
  }

  ProdSecretsManager(
      AwsClients awsClients, SecretsManagerClient secretsClient, StsClient stsClient) {
    this.awsClients = awsClients;
    AtomicReference<SecretsManagerClient> initialSecretsClient =
        new AtomicReference<>(secretsClient);
    AtomicReference<StsClient> initialStsClient = new AtomicReference<>(stsClient);
    this.secretsClient =
        RefreshingAwsClient.withResourceFactory(
            "Secrets Manager",
            () ->
                RefreshingAwsClient.clientResource(
                    takeInitialOrBuild(initialSecretsClient, this::newSecretsClient)));
    this.stsClient =
        RefreshingAwsClient.withResourceFactory(
            "STS",
            () ->
                RefreshingAwsClient.clientResource(
                    takeInitialOrBuild(initialStsClient, this::newStsClient)));
  }

  private final AwsClients awsClients;

  @PreDestroy
  void shutdown() {
    synchronized (perAccountClientLock) {
      closePerAccountClients();
      stsClient.close();
    }
    secretsClient.close();
  }

  @Override
  public void put(String accountId, String secretType, String secretId, byte[] payload) {
    withClient(
        accountId,
        client -> {
          putOnce(client, accountId, secretType, secretId, payload);
          return null;
        });
  }

  @Override
  public Optional<byte[]> get(String accountId, String secretType, String secretId) {
    return withClient(accountId, client -> getOnce(client, accountId, secretType, secretId));
  }

  @Override
  public void update(String accountId, String secretType, String secretId, byte[] payload) {
    withClient(
        accountId,
        client -> {
          updateOnce(client, accountId, secretType, secretId, payload);
          return null;
        });
  }

  @Override
  public void delete(String accountId, String secretType, String secretId) {
    withClient(
        accountId,
        client -> {
          deleteOnce(client, accountId, secretType, secretId);
          return null;
        });
  }

  private void putOnce(
      SecretsManagerClient client,
      String accountId,
      String secretType,
      String secretId,
      byte[] payload) {
    ensureAwsCredentialsAvailable();
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

  private Optional<byte[]> getOnce(
      SecretsManagerClient client, String accountId, String secretType, String secretId) {
    ensureAwsCredentialsAvailable();
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
    }
  }

  private void updateOnce(
      SecretsManagerClient client,
      String accountId,
      String secretType,
      String secretId,
      byte[] payload) {
    ensureAwsCredentialsAvailable();
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
    ensureAwsCredentialsAvailable();
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    try {
      client.deleteSecret(
          DeleteSecretRequest.builder().secretId(secretName).recoveryWindowInDays(7L).build());
    } catch (ResourceNotFoundException ignored) {
    }
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

  private SecretsManagerClient newSecretsClient() {
    if (awsClients == null) {
      throw new IllegalStateException("Secrets Manager client cannot be refreshed");
    }
    return awsClients.secretsManagerClient();
  }

  private StsClient newStsClient() {
    if (awsClients == null) {
      throw new IllegalStateException("STS client cannot be refreshed");
    }
    return awsClients.stsClient();
  }

  private static <T> T takeInitialOrBuild(
      AtomicReference<T> initial, java.util.function.Supplier<T> factory) {
    T value = initial.getAndSet(null);
    if (value != null) {
      return value;
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

  private void ensureAwsCredentialsAvailable() {
    if (awsClients == null) {
      return;
    }
    awsClients.ensureCredentialsAvailable();
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
