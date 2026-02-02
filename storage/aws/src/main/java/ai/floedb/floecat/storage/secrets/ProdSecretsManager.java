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
import java.util.Optional;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.CreateSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.DeleteSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.PutSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.ResourceExistsException;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

@ApplicationScoped
@IfBuildProfile("prod")
public class ProdSecretsManager implements SecretsManager {
  private final SecretsManagerClient secretsClient;

  public ProdSecretsManager() {
    this.secretsClient = SecretsManagerClient.builder().build();
  }

  ProdSecretsManager(SecretsManagerClient secretsClient) {
    this.secretsClient = secretsClient;
  }

  @PreDestroy
  void shutdown() {
    secretsClient.close();
  }

  @Override
  public void put(String accountId, String secretType, String secretId, byte[] payload) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    SdkBytes sdkBytes = SdkBytes.fromByteArray(payload == null ? new byte[0] : payload);
    try {
      secretsClient.createSecret(
          CreateSecretRequest.builder().name(secretName).secretBinary(sdkBytes).build());
    } catch (ResourceExistsException exists) {
      secretsClient.putSecretValue(
          PutSecretValueRequest.builder().secretId(secretName).secretBinary(sdkBytes).build());
    }
  }

  @Override
  public Optional<byte[]> get(String accountId, String secretType, String secretId) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    try {
      var response =
          secretsClient.getSecretValue(
              GetSecretValueRequest.builder().secretId(secretName).build());
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
    try {
      secretsClient.putSecretValue(
          PutSecretValueRequest.builder().secretId(secretName).secretBinary(sdkBytes).build());
    } catch (ResourceNotFoundException missing) {
      secretsClient.createSecret(
          CreateSecretRequest.builder().name(secretName).secretBinary(sdkBytes).build());
    }
  }

  @Override
  public void delete(String accountId, String secretType, String secretId) {
    String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
    try {
      secretsClient.deleteSecret(
          DeleteSecretRequest.builder().secretId(secretName).recoveryWindowInDays(7L).build());
    } catch (ResourceNotFoundException ignored) {
    }
  }
}
