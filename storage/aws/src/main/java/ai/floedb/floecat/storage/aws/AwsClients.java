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
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

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

  @ConfigProperty(name = "floecat.storage.aws.s3.path-style-access", defaultValue = "false")
  boolean forcePathStyle;

  @Produces
  @Singleton
  DynamoDbClient dynamoDbClient() {
    var builder =
        DynamoDbClient.builder()
            .region(region)
            .httpClient(UrlConnectionHttpClient.create())
            .credentialsProvider(resolveCredentials())
            .overrideConfiguration(ClientOverrideConfiguration.builder().build());
    dynamoEndpoint.ifPresent(builder::endpointOverride);
    return builder.build();
  }

  @Produces
  @Singleton
  public DynamoDbAsyncClient dynamoDbAsyncClient() {
    var builder =
        DynamoDbAsyncClient.builder()
            .region(region)
            .credentialsProvider(resolveCredentials())
            .overrideConfiguration(ClientOverrideConfiguration.builder().build());
    dynamoEndpoint.ifPresent(builder::endpointOverride);
    return builder.build();
  }

  @Produces
  @Singleton
  S3Client s3Client() {
    var s3Cfg = S3Configuration.builder().pathStyleAccessEnabled(forcePathStyle).build();
    var builder =
        S3Client.builder()
            .region(region)
            .serviceConfiguration(s3Cfg)
            .httpClient(UrlConnectionHttpClient.create())
            .credentialsProvider(resolveCredentials())
            .overrideConfiguration(ClientOverrideConfiguration.builder().build());
    s3Endpoint.ifPresent(builder::endpointOverride);
    return builder.build();
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
