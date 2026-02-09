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

package ai.floedb.floecat.storage.secrets;

import io.quarkus.arc.profile.IfBuildProfile;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.core.SdkBytes;
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
  Optional<String> roleArn;

  private final SecretsManagerClient secretsClient;
  private final StsClient stsClient;
  private final ConcurrentMap<String, SecretsManagerClient> perAccountClients =
      new ConcurrentHashMap<>();

  public ProdSecretsManager() {
    this(SecretsManagerClient.builder().build(), StsClient.builder().build());
  }

  ProdSecretsManager(SecretsManagerClient secretsClient, StsClient stsClient) {
    this.secretsClient = secretsClient;
    this.stsClient = stsClient;
  }

  @PreDestroy
  void shutdown() {
    for (SecretsManagerClient client : perAccountClients.values()) {
      client.close();
    }
    perAccountClients.clear();
    secretsClient.close();
    stsClient.close();
  }

  @Override
  public void put(String accountId, String secretType, String secretId, byte[] payload) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    SdkBytes sdkBytes = SdkBytes.fromByteArray(payload == null ? new byte[0] : payload);
    List<Tag> tags = buildTags(accountId, secretName);
    try {
      clientForAccount(accountId)
          .createSecret(
              CreateSecretRequest.builder()
                  .name(secretName)
                  .secretBinary(sdkBytes)
                  .tags(tags)
                  .build());
    } catch (ResourceExistsException exists) {
      clientForAccount(accountId)
          .putSecretValue(
              PutSecretValueRequest.builder().secretId(secretName).secretBinary(sdkBytes).build());
      tagSecret(accountId, secretName, tags);
    }
  }

  @Override
  public Optional<byte[]> get(String accountId, String secretType, String secretId) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    try {
      var response =
          clientForAccount(accountId)
              .getSecretValue(GetSecretValueRequest.builder().secretId(secretName).build());
      if (response == null || response.secretBinary() == null) {
        return Optional.empty();
      }
      return Optional.of(response.secretBinary().asByteArray());
    } catch (ResourceNotFoundException missing) {
      return Optional.empty();
    }
  }

  @Override
  public void update(String accountId, String secretType, String secretId, byte[] payload) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    SdkBytes sdkBytes = SdkBytes.fromByteArray(payload == null ? new byte[0] : payload);
    List<Tag> tags = buildTags(accountId, secretName);
    try {
      clientForAccount(accountId)
          .putSecretValue(
              PutSecretValueRequest.builder().secretId(secretName).secretBinary(sdkBytes).build());
      tagSecret(accountId, secretName, tags);
    } catch (ResourceNotFoundException missing) {
      clientForAccount(accountId)
          .createSecret(
              CreateSecretRequest.builder()
                  .name(secretName)
                  .secretBinary(sdkBytes)
                  .tags(tags)
                  .build());
    }
  }

  @Override
  public void delete(String accountId, String secretType, String secretId) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    try {
      clientForAccount(accountId)
          .deleteSecret(
              DeleteSecretRequest.builder().secretId(secretName).recoveryWindowInDays(7L).build());
    } catch (ResourceNotFoundException ignored) {
    }
  }

  private static List<Tag> buildTags(String accountId, String secretName) {
    return List.of(Tag.builder().key(ACCOUNT_ID_TAG).value(accountId).build());
  }

  private void tagSecret(String accountId, String secretName, List<Tag> tags) {
    if (tags == null || tags.isEmpty()) {
      return;
    }
    clientForAccount(accountId)
        .tagResource(TagResourceRequest.builder().secretId(secretName).tags(tags).build());
  }

  private SecretsManagerClient clientForAccount(String accountId) {
    String role = roleArn.map(String::trim).filter(value -> !value.isBlank()).orElse("");
    if (role.isEmpty()) {
      return secretsClient;
    }
    return perAccountClients.computeIfAbsent(
        accountId,
        id -> {
          StsAssumeRoleCredentialsProvider creds =
              StsAssumeRoleCredentialsProvider.builder()
                  .stsClient(stsClient)
                  .refreshRequest(
                      builder ->
                          builder
                              .roleArn(role)
                              .roleSessionName(buildRoleSessionName(id))
                              .tags(
                                  software.amazon.awssdk.services.sts.model.Tag.builder()
                                      .key(ACCOUNT_ID_TAG)
                                      .value(id)
                                      .build()))
                  .build();
          return SecretsManagerClient.builder().credentialsProvider(creds).build();
        });
  }

  private static String buildRoleSessionName(String accountId) {
    String raw = ROLE_SESSION_PREFIX + accountId;
    if (raw.length() <= MAX_ROLE_SESSION_LENGTH) {
      return raw;
    }
    return raw.substring(0, MAX_ROLE_SESSION_LENGTH);
  }
}
