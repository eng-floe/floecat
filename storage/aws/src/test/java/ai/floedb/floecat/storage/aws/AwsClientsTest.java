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
package ai.floedb.floecat.storage.aws;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

public class AwsClientsTest {

  @Test
  void uses_DefaultCredentialsProvider_when_access_and_secret_not_set() throws Exception {
    AwsClients clients = baseClients();
    AwsCredentialsProvider provider = resolveCredentials(clients);
    assertTrue(provider instanceof DefaultCredentialsProvider);
  }

  @Test
  void uses_StaticCredentialsProvider_when_access_and_secret_set() throws Exception {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.of("secret");

    AwsCredentialsProvider provider = resolveCredentials(clients);
    assertTrue(provider instanceof StaticCredentialsProvider);
    AwsCredentials creds = provider.resolveCredentials();
    assertTrue(creds instanceof AwsBasicCredentials);
  }

  @Test
  void uses_session_credentials_when_session_token_set() throws Exception {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.of("secret");
    clients.sessionToken = Optional.of("token");

    AwsCredentialsProvider provider = resolveCredentials(clients);
    assertTrue(provider instanceof StaticCredentialsProvider);
    AwsCredentials creds = provider.resolveCredentials();
    assertTrue(creds instanceof AwsSessionCredentials);
    assertEquals("token", ((AwsSessionCredentials) creds).sessionToken());
  }

  @Test
  void uses_default_credentials_when_access_or_secret_blank() throws Exception {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("  ");
    clients.secretKey = Optional.of(" ");

    AwsCredentialsProvider provider = resolveCredentials(clients);
    assertTrue(provider instanceof DefaultCredentialsProvider);
  }

  @Test
  void uses_default_credentials_when_secret_missing() throws Exception {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.empty();

    AwsCredentialsProvider provider = resolveCredentials(clients);
    assertTrue(provider instanceof DefaultCredentialsProvider);
  }

  @Test
  void uses_basic_credentials_when_session_token_blank() throws Exception {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.of("secret");
    clients.sessionToken = Optional.of(" ");

    AwsCredentialsProvider provider = resolveCredentials(clients);
    assertTrue(provider instanceof StaticCredentialsProvider);
    AwsCredentials creds = provider.resolveCredentials();
    assertTrue(creds instanceof AwsBasicCredentials);
  }

  @Test
  void endpoint_override_applied_when_config_present() {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.of("secret");
    URI endpoint = URI.create("http://localhost:8000");
    clients.dynamoEndpoint = Optional.of(endpoint);

    try (DynamoDbClient client = clients.dynamoDbClient()) {
      assertEquals(endpoint, client.serviceClientConfiguration().endpointOverride().orElseThrow());
    }
  }

  @Test
  void region_from_config_is_used() {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.of("secret");
    clients.region = Region.US_WEST_2;

    try (DynamoDbClient client = clients.dynamoDbClient()) {
      assertEquals(Region.US_WEST_2, client.serviceClientConfiguration().region());
    }
  }

  @Test
  void s3_endpoint_override_and_path_style_applied() throws Exception {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.of("secret");
    clients.s3Endpoint = Optional.of(URI.create("http://localhost:9000"));
    clients.forcePathStyle = true;

    try (S3Client client = clients.s3Client()) {
      assertEquals(
          URI.create("http://localhost:9000"),
          client.serviceClientConfiguration().endpointOverride().orElseThrow());
      S3Configuration cfg = s3ServiceConfiguration(client);
      assertTrue(cfg.pathStyleAccessEnabled());
    }
  }

  private static AwsClients baseClients() {
    AwsClients clients = new AwsClients();
    clients.region = Region.US_EAST_1;
    clients.accessKey = Optional.empty();
    clients.secretKey = Optional.empty();
    clients.sessionToken = Optional.empty();
    clients.dynamoEndpoint = Optional.empty();
    clients.s3Endpoint = Optional.empty();
    clients.forcePathStyle = false;
    return clients;
  }

  private static AwsCredentialsProvider resolveCredentials(AwsClients clients) throws Exception {
    Method method = AwsClients.class.getDeclaredMethod("resolveCredentials");
    method.setAccessible(true);
    return (AwsCredentialsProvider) method.invoke(clients);
  }

  private static S3Configuration s3ServiceConfiguration(S3Client client) throws Exception {
    java.lang.reflect.Field field = client.getClass().getDeclaredField("clientConfiguration");
    field.setAccessible(true);
    Object sdkConfig = field.get(client);
    Method option =
        sdkConfig
            .getClass()
            .getMethod("option", software.amazon.awssdk.core.client.config.ClientOption.class);
    Object serviceConfig = option.invoke(sdkConfig, SdkClientOption.SERVICE_CONFIGURATION);
    return (S3Configuration) serviceConfig;
  }
}
