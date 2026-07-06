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

import ai.floedb.floecat.storage.aws.AwsClients;
import ai.floedb.floecat.storage.aws.ClosedAwsClientDetector;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import io.quarkus.arc.profile.IfBuildProfile;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.microprofile.config.inject.ConfigProperty;
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

  private final AtomicReference<SecretsManagerClient> secretsClient;
  private final AtomicReference<StsClient> stsClient;
  private final ConcurrentMap<String, SecretsManagerClient> perAccountClients =
      new ConcurrentHashMap<>();
  private final Object perAccountClientLock = new Object();

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
    this.secretsClient = new AtomicReference<>(secretsClient);
    this.stsClient = new AtomicReference<>(stsClient);
  }

  private final AwsClients awsClients;

  @PreDestroy
  void shutdown() {
    synchronized (perAccountClientLock) {
      closePerAccountClients();
      closeQuietly(stsClient.getAndSet(null));
    }
    closeQuietly(secretsClient.getAndSet(null));
  }

  @Override
  public void put(String accountId, String secretType, String secretId, byte[] payload) {
    withClientRefresh(
        accountId,
        client -> {
          putOnce(client, accountId, secretType, secretId, payload);
          return null;
        });
  }

  @Override
  public Optional<byte[]> get(String accountId, String secretType, String secretId) {
    return withClientRefresh(accountId, client -> getOnce(client, accountId, secretType, secretId));
  }

  @Override
  public void update(String accountId, String secretType, String secretId, byte[] payload) {
    withClientRefresh(
        accountId,
        client -> {
          updateOnce(client, accountId, secretType, secretId, payload);
          return null;
        });
  }

  @Override
  public void delete(String accountId, String secretType, String secretId) {
    withClientRefresh(
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

  private SecretsManagerClient clientForAccount(String accountId) {
    String role = roleArn.map(String::trim).filter(value -> !value.isBlank()).orElse("");
    if (role.isEmpty()) {
      return requireOpen(secretsClient.get());
    }
    synchronized (perAccountClientLock) {
      SecretsManagerClient existing = perAccountClients.get(accountId);
      if (existing != null) {
        return existing;
      }
      StsClient currentSts = requireOpen(stsClient.get());
      StsAssumeRoleCredentialsProvider creds =
          StsAssumeRoleCredentialsProvider.builder()
              .stsClient(currentSts)
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
      SecretsManagerClient client =
          awsClients != null
              ? awsClients.secretsManagerClient(creds)
              : SecretsManagerClient.builder().credentialsProvider(creds).build();
      perAccountClients.put(accountId, client);
      return client;
    }
  }

  private <T> T withClientRefresh(String accountId, ClientOperation<T> operation) {
    SecretsManagerClient client = clientForAccount(accountId);
    try {
      return operation.run(client);
    } catch (RuntimeException e) {
      if (awsClients == null || !ClosedAwsClientDetector.isConnectionPoolShutdown(e)) {
        throw e;
      }
      refreshAfterClosedPool(accountId, client);
      return operation.run(clientForAccount(accountId));
    }
  }

  private void refreshAfterClosedPool(String accountId, SecretsManagerClient failedClient) {
    String role = roleArn.map(String::trim).filter(value -> !value.isBlank()).orElse("");
    if (role.isEmpty()) {
      if (failedClient == null) {
        return;
      }
      SecretsManagerClient next = awsClients.secretsManagerClient();
      if (secretsClient.compareAndSet(failedClient, next)) {
        closeQuietly(failedClient);
      } else {
        closeQuietly(next);
      }
      return;
    }

    synchronized (perAccountClientLock) {
      if (accountId == null || accountId.isBlank() || failedClient == null) {
        return;
      }
      if (!perAccountClients.remove(accountId, failedClient)) {
        return;
      }
      closeQuietly(failedClient);
      StsClient previousSts = stsClient.get();
      if (previousSts == null) {
        return;
      }
      StsClient nextSts = awsClients.stsClient();
      if (stsClient.compareAndSet(previousSts, nextSts)) {
        closeQuietly(previousSts);
        closePerAccountClients();
      } else {
        closeQuietly(nextSts);
      }
    }
  }

  private void closePerAccountClients() {
    for (SecretsManagerClient client : perAccountClients.values()) {
      closeQuietly(client);
    }
    perAccountClients.clear();
  }

  private static <T> T requireOpen(T client) {
    if (client == null) {
      throw new IllegalStateException("secrets manager is shut down");
    }
    return client;
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

  private static void closeQuietly(AutoCloseable client) {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (Exception ignored) {
    }
  }

  @FunctionalInterface
  private interface ClientOperation<T> {
    T run(SecretsManagerClient client);
  }
}
