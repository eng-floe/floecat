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

import ai.floedb.floecat.telemetry.StorageTelemetry;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.TestObservability;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.sts.StsClient;

public class AwsClientsTest {

  @Test
  void storageMetricPublisherIsInstalledAndSurvivesStorageClientClose() throws Exception {
    AwsClients clients = baseClients();
    TestObservability observability = new TestObservability();
    clients.storageMetricPublisher =
        new AwsStorageMetricPublisher(new StorageTelemetry(observability));
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.of("secret");

    try (DynamoDbClient dynamo = clients.newDynamoDbClient();
        DynamoDbAsyncClient dynamoAsync = clients.newDynamoDbAsyncClient();
        S3Client s3 = clients.newS3Client()) {
      assertEquals(List.of(clients.storageMetricPublisher), metricPublishers(dynamo));
      assertEquals(List.of(clients.storageMetricPublisher), metricPublishers(dynamoAsync));
      assertEquals(List.of(clients.storageMetricPublisher), metricPublishers(s3));
    }

    MetricCollector afterClose = MetricCollector.create("ApiCall");
    afterClose.reportMetric(CoreMetric.SERVICE_ID, "DynamoDb");
    afterClose.reportMetric(CoreMetric.OPERATION_NAME, "GetItem");
    afterClose.reportMetric(CoreMetric.API_CALL_SUCCESSFUL, true);
    clients.storageMetricPublisher.publish(afterClose.collect());

    assertEquals(1d, observability.counterValue(Telemetry.Metrics.STORE_REQUESTS));
  }

  @Test
  void uses_DefaultCredentialsProvider_when_access_and_secret_not_set() throws Exception {
    AwsClients clients = baseClients();
    AwsCredentialsProvider provider = clients.resolveCredentials();
    assertTrue(provider instanceof DefaultCredentialsProvider);
  }

  @Test
  void uses_StaticCredentialsProvider_when_access_and_secret_set() throws Exception {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.of("secret");

    AwsCredentialsProvider provider = clients.resolveCredentials();
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

    AwsCredentialsProvider provider = clients.resolveCredentials();
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

    AwsCredentialsProvider provider = clients.resolveCredentials();
    assertTrue(provider instanceof DefaultCredentialsProvider);
  }

  @Test
  void uses_default_credentials_when_secret_missing() throws Exception {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.empty();

    AwsCredentialsProvider provider = clients.resolveCredentials();
    assertTrue(provider instanceof DefaultCredentialsProvider);
  }

  @Test
  void uses_basic_credentials_when_session_token_blank() throws Exception {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.of("secret");
    clients.sessionToken = Optional.of(" ");

    AwsCredentialsProvider provider = clients.resolveCredentials();
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

  @Test
  void secrets_manager_endpoint_override_applied_when_config_present() {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.of("secret");
    clients.secretsManagerEndpoint = Optional.of(URI.create("http://localhost:4566"));

    try (SecretsManagerClient client = clients.secretsManagerClient()) {
      assertEquals(
          URI.create("http://localhost:4566"),
          client.serviceClientConfiguration().endpointOverride().orElseThrow());
    }
  }

  @Test
  void sts_endpoint_override_applied_when_config_present() {
    AwsClients clients = baseClients();
    clients.accessKey = Optional.of("access");
    clients.secretKey = Optional.of("secret");
    clients.stsEndpoint = Optional.of(URI.create("http://localhost:4566"));

    try (StsClient client = clients.stsClient()) {
      assertEquals(
          URI.create("http://localhost:4566"),
          client.serviceClientConfiguration().endpointOverride().orElseThrow());
    }
  }

  private static AwsClients baseClients() {
    AwsClients clients = new AwsClients();
    clients.storageMetricPublisher =
        new AwsStorageMetricPublisher(new StorageTelemetry(new TestObservability()));
    clients.region = Region.US_EAST_1;
    clients.accessKey = Optional.empty();
    clients.secretKey = Optional.empty();
    clients.sessionToken = Optional.empty();
    clients.dynamoEndpoint = Optional.empty();
    clients.s3Endpoint = Optional.empty();
    clients.secretsManagerEndpoint = Optional.empty();
    clients.stsEndpoint = Optional.empty();
    clients.forcePathStyle = false;
    return clients;
  }

  @SuppressWarnings("unchecked")
  private static List<software.amazon.awssdk.metrics.MetricPublisher> metricPublishers(
      Object client) throws Exception {
    java.lang.reflect.Field field = client.getClass().getDeclaredField("clientConfiguration");
    field.setAccessible(true);
    Object sdkConfig = field.get(client);
    Method option =
        sdkConfig
            .getClass()
            .getMethod("option", software.amazon.awssdk.core.client.config.ClientOption.class);
    return (List<software.amazon.awssdk.metrics.MetricPublisher>)
        option.invoke(sdkConfig, SdkClientOption.METRIC_PUBLISHERS);
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
