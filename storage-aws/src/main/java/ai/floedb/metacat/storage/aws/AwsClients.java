package ai.floedb.metacat.storage.aws;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.net.URI;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

@ApplicationScoped
public class AwsClients {

  @ConfigProperty(name = "aws.region", defaultValue = "us-east-1")
  Region region;

  @ConfigProperty(name = "aws.accessKeyId", defaultValue = "test")
  String accessKey;

  @ConfigProperty(name = "aws.secretAccessKey", defaultValue = "test")
  String secretKey;

  @ConfigProperty(name = "aws.dynamodb.endpoint-override")
  URI dynamoEndpoint;

  @ConfigProperty(name = "aws.s3.endpoint-override")
  URI s3Endpoint;

  @Produces
  @Singleton
  DynamoDbClient dynamoDbClient() {
    var b =
        DynamoDbClient.builder()
            .region(region)
            .httpClient(UrlConnectionHttpClient.create())
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
            .overrideConfiguration(ClientOverrideConfiguration.builder().build());
    if (dynamoEndpoint != null) b = b.endpointOverride(dynamoEndpoint);
    return b.build();
  }

  @Produces
  @Singleton
  S3Client s3Client() {
    var b =
        S3Client.builder()
            .region(region)
            .httpClient(UrlConnectionHttpClient.create())
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
            .overrideConfiguration(ClientOverrideConfiguration.builder().build());
    if (s3Endpoint != null) b = b.endpointOverride(s3Endpoint);
    return b.build();
  }
}
