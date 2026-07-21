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

import ai.floedb.floecat.aws.RefreshingAwsClient;
import ai.floedb.floecat.aws.RefreshingAwsClient.ClientResource;
import ai.floedb.floecat.storage.AwsCredentialsUnavailableException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.net.URI;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.sts.StsClient;

@ApplicationScoped
public class AwsClients {

  @ConfigProperty(name = "floecat.storage.aws.region", defaultValue = "us-east-1")
  Region region;

  @ConfigProperty(name = "floecat.storage.aws.access-key-id")
  Optional<String> accessKey;

  @ConfigProperty(name = "floecat.storage.aws.secret-access-key")
  Optional<String> secretKey;

  @ConfigProperty(name = "floecat.storage.aws.session-token")
  Optional<String> sessionToken;

  @ConfigProperty(name = "floecat.storage.aws.dynamodb.endpoint-override")
  Optional<URI> dynamoEndpoint;

  @ConfigProperty(name = "floecat.storage.aws.s3.endpoint")
  Optional<URI> s3Endpoint;

  @ConfigProperty(name = "floecat.storage.aws.secretsmanager.endpoint-override")
  Optional<URI> secretsManagerEndpoint;

  @ConfigProperty(name = "floecat.storage.aws.sts.endpoint-override")
  Optional<URI> stsEndpoint;

  @ConfigProperty(name = "floecat.storage.aws.s3.path-style-access", defaultValue = "false")
  boolean forcePathStyle;

  @Produces
  @Singleton
  DynamoDbClient dynamoDbClient() {
    return newDynamoDbClient();
  }

  public DynamoDbClient newDynamoDbClient() {
    return newDynamoDbClientResource().client();
  }

  public ClientResource<DynamoDbClient> newDynamoDbClientResource() {
    AwsCredentialsProvider credentials = resolveCredentials();
    var builder =
        DynamoDbClient.builder()
            .region(region)
            .httpClient(UrlConnectionHttpClient.create())
            .credentialsProvider(credentials)
            .overrideConfiguration(ClientOverrideConfiguration.builder().build());
    dynamoEndpoint.ifPresent(builder::endpointOverride);
    try {
      return RefreshingAwsClient.clientResource(
          builder.build(), RefreshingAwsClient.closeableResource(credentials));
    } catch (RuntimeException | Error e) {
      RefreshingAwsClient.closeQuietly(RefreshingAwsClient.closeableResource(credentials));
      throw e;
    }
  }

  @Produces
  @Singleton
  public DynamoDbAsyncClient dynamoDbAsyncClient() {
    return newDynamoDbAsyncClient();
  }

  public DynamoDbAsyncClient newDynamoDbAsyncClient() {
    return newDynamoDbAsyncClientResource().client();
  }

  public ClientResource<DynamoDbAsyncClient> newDynamoDbAsyncClientResource() {
    AwsCredentialsProvider credentials = resolveCredentials();
    var builder =
        DynamoDbAsyncClient.builder()
            .region(region)
            .credentialsProvider(credentials)
            .overrideConfiguration(ClientOverrideConfiguration.builder().build());
    dynamoEndpoint.ifPresent(builder::endpointOverride);
    try {
      return RefreshingAwsClient.clientResource(
          builder.build(), RefreshingAwsClient.closeableResource(credentials));
    } catch (RuntimeException | Error e) {
      RefreshingAwsClient.closeQuietly(RefreshingAwsClient.closeableResource(credentials));
      throw e;
    }
  }

  @Produces
  @Singleton
  S3Client s3Client() {
    return newS3Client();
  }

  public S3Client newS3Client() {
    return newS3ClientResource().client();
  }

  public ClientResource<S3Client> newS3ClientResource() {
    AwsCredentialsProvider credentials = resolveCredentials();
    var s3Cfg = S3Configuration.builder().pathStyleAccessEnabled(forcePathStyle).build();
    var builder =
        S3Client.builder()
            .region(region)
            .serviceConfiguration(s3Cfg)
            .httpClient(UrlConnectionHttpClient.create())
            .credentialsProvider(credentials)
            .overrideConfiguration(ClientOverrideConfiguration.builder().build());
    s3Endpoint.ifPresent(builder::endpointOverride);
    try {
      return RefreshingAwsClient.clientResource(
          builder.build(), RefreshingAwsClient.closeableResource(credentials));
    } catch (RuntimeException | Error e) {
      RefreshingAwsClient.closeQuietly(RefreshingAwsClient.closeableResource(credentials));
      throw e;
    }
  }

  @Produces
  @Singleton
  public SecretsManagerClient secretsManagerClient() {
    return newSecretsManagerClientResource().client();
  }

  @Produces
  @Singleton
  public StsClient stsClient() {
    return newStsClientResource().client();
  }

  public ClientResource<SecretsManagerClient> newSecretsManagerClientResource() {
    AwsCredentialsProvider credentials = resolveCredentials();
    try {
      return RefreshingAwsClient.clientResource(
          secretsManagerClient(credentials), RefreshingAwsClient.closeableResource(credentials));
    } catch (RuntimeException | Error e) {
      RefreshingAwsClient.closeQuietly(RefreshingAwsClient.closeableResource(credentials));
      throw e;
    }
  }

  public ClientResource<StsClient> newStsClientResource() {
    AwsCredentialsProvider credentials = resolveCredentials();
    try {
      return RefreshingAwsClient.clientResource(
          stsClient(credentials), RefreshingAwsClient.closeableResource(credentials));
    } catch (RuntimeException | Error e) {
      RefreshingAwsClient.closeQuietly(RefreshingAwsClient.closeableResource(credentials));
      throw e;
    }
  }

  public SecretsManagerClient secretsManagerClient(AwsCredentialsProvider credentialsProvider) {
    var builder =
        SecretsManagerClient.builder()
            .region(region)
            .httpClient(UrlConnectionHttpClient.create())
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration(ClientOverrideConfiguration.builder().build());
    secretsManagerEndpoint.ifPresent(builder::endpointOverride);
    return builder.build();
  }

  public StsClient stsClient(AwsCredentialsProvider credentialsProvider) {
    var builder =
        StsClient.builder()
            .region(region)
            .httpClient(UrlConnectionHttpClient.create())
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration(ClientOverrideConfiguration.builder().build());
    stsEndpoint.ifPresent(builder::endpointOverride);
    return builder.build();
  }

  public void ensureCredentialsAvailable() {
    try {
      resolveCredentials().resolveCredentials();
    } catch (SdkClientException e) {
      throw new AwsCredentialsUnavailableException("AWS credentials are unavailable", e);
    }
  }

  AwsCredentialsProvider resolveCredentials() {
    String trimmedAccess = trim(accessKey.orElse(null));
    String trimmedSecret = trim(secretKey.orElse(null));
    if (trimmedAccess != null && trimmedSecret != null) {
      AwsCredentials creds =
          sessionToken
              .filter(token -> !token.isBlank())
              .map(token -> AwsSessionCredentials.create(trimmedAccess, trimmedSecret, token))
              .map(AwsCredentials.class::cast)
              .orElseGet(() -> AwsBasicCredentials.create(trimmedAccess, trimmedSecret));
      return StaticCredentialsProvider.create(creds);
    }
    return DefaultCredentialsProvider.builder().build();
  }

  private static String trim(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }
}
