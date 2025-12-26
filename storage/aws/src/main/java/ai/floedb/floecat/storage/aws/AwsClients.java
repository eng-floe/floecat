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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

@ApplicationScoped
public class AwsClients {

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

  @ConfigProperty(name = "floecat.fileio.override.s3.endpoint")
  Optional<URI> s3Endpoint;

  @ConfigProperty(name = "floecat.fileio.override.s3.path-style-access", defaultValue = "false")
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
