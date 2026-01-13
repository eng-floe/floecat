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
package ai.floedb.floecat.kv.dynamodb;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

@ApplicationScoped
public class DynamoDbClientProducer {
  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(DynamoDbClientProducer.class);

  @ConfigProperty(name = "floecat.fileio.override.s3.region", defaultValue = "us-east-1")
  Region region;

  @ConfigProperty(name = "floecat.fileio.override.s3.access-key-id", defaultValue = "test")
  String accessKey;

  @ConfigProperty(name = "floecat.fileio.override.s3.secret-access-key", defaultValue = "test")
  String secretKey;

  @ConfigProperty(name = "floecat.fileio.override.s3.session-token")
  Optional<String> sessionToken;

  @ConfigProperty(name = "floecat.fileio.override.aws.dynamodb.endpoint-override")
  Optional<URI> dynamoEndpoint;

  @Produces
  @ApplicationScoped
  public DynamoDbAsyncClient dynamoDbAsyncClient() {
    LOG.info("DynamoDbClientProducer: producing client, region={}", region);
    var builder =
        DynamoDbAsyncClient.builder()
            .region(region)
            .credentialsProvider(resolveCredentials())
            .overrideConfiguration(ClientOverrideConfiguration.builder().build());
    dynamoEndpoint.ifPresent(builder::endpointOverride);
    return builder.build();
  }

  private AwsCredentialsProvider resolveCredentials() {
    String trimmedAccess = trim(accessKey);
    String trimmedSecret = trim(secretKey);
    if (trimmedAccess != null && trimmedSecret != null) {
      AwsCredentials creds =
          sessionToken
              .filter(token -> !token.isBlank())
              .map(token -> AwsSessionCredentials.create(trimmedAccess, trimmedSecret, token))
              .map(AwsCredentials.class::cast)
              .orElseGet(() -> AwsBasicCredentials.create(trimmedAccess, trimmedSecret));
      return StaticCredentialsProvider.create(creds);
    }
    return DefaultCredentialsProvider.create();
  }

  private static String trim(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }
}
