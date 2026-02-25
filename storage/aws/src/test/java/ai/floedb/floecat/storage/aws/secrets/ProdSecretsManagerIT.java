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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.storage.secrets.SecretsManager;
import java.net.URI;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.DescribeSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.Tag;
import software.amazon.awssdk.services.sts.StsClient;

class ProdSecretsManagerIT {

  @Test
  void put_get_update_delete_round_trip() {
    SecretsManagerClient client = buildClient();
    StsClient stsClient = buildStsClient();
    ProdSecretsManager manager = new ProdSecretsManager(client, stsClient);

    String accountId = "acct-it";
    String secretType = "connectors";
    String secretId = "conn-" + UUID.randomUUID();
    byte[] payload = "alpha".getBytes();

    try {
      manager.put(accountId, secretType, secretId, payload);

      String secretName = SecretsManager.buildSecretKey(accountId, secretType, secretId);
      byte[] stored =
          client
              .getSecretValue(GetSecretValueRequest.builder().secretId(secretName).build())
              .secretBinary()
              .asByteArray();
      assertArrayEquals(payload, stored);
      assertTrue(hasTag(client, secretName, "AccountId", accountId));

      byte[] updated = "beta".getBytes();
      manager.update(accountId, secretType, secretId, updated);
      Optional<byte[]> resolved = manager.get(accountId, secretType, secretId);
      assertTrue(resolved.isPresent());
      assertArrayEquals(updated, resolved.get());
    } finally {
      manager.delete(accountId, secretType, secretId);
      client.close();
      stsClient.close();
    }
  }

  private static SecretsManagerClient buildClient() {
    String endpoint = firstNonBlank(env("LOCALSTACK_ENDPOINT"), env("AWS_ENDPOINT_URL"));
    if (endpoint == null) {
      endpoint = "http://localhost:4566";
    }
    String region = firstNonBlank(env("AWS_REGION"), env("AWS_DEFAULT_REGION"));
    if (region == null) {
      region = "us-east-1";
    }
    AwsCredentialsProvider provider = StaticCredentialsProvider.create(resolveCredentials());
    return SecretsManagerClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region))
        .credentialsProvider(provider)
        .build();
  }

  private static StsClient buildStsClient() {
    String endpoint = firstNonBlank(env("LOCALSTACK_ENDPOINT"), env("AWS_ENDPOINT_URL"));
    if (endpoint == null) {
      endpoint = "http://localhost:4566";
    }
    String region = firstNonBlank(env("AWS_REGION"), env("AWS_DEFAULT_REGION"));
    if (region == null) {
      region = "us-east-1";
    }
    AwsCredentialsProvider provider = StaticCredentialsProvider.create(resolveCredentials());
    return StsClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.of(region))
        .credentialsProvider(provider)
        .build();
  }

  private static AwsCredentials resolveCredentials() {
    String accessKey = firstNonBlank(env("AWS_ACCESS_KEY_ID"), "test");
    String secretKey = firstNonBlank(env("AWS_SECRET_ACCESS_KEY"), "test");
    String session = env("AWS_SESSION_TOKEN");
    if (session != null && !session.isBlank()) {
      return AwsSessionCredentials.create(accessKey, secretKey, session);
    }
    return AwsBasicCredentials.create(accessKey, secretKey);
  }

  private static String env(String name) {
    String value = System.getenv(name);
    return (value == null || value.isBlank()) ? null : value;
  }

  private static String firstNonBlank(String first, String second) {
    if (first != null && !first.isBlank()) {
      return first;
    }
    if (second != null && !second.isBlank()) {
      return second;
    }
    return null;
  }

  private static boolean hasTag(
      SecretsManagerClient client, String secretName, String key, String value) {
    for (Tag tag :
        client
            .describeSecret(DescribeSecretRequest.builder().secretId(secretName).build())
            .tags()) {
      if (key.equals(tag.key()) && value.equals(tag.value())) {
        return true;
      }
    }
    return false;
  }
}
