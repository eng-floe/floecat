package ai.floedb.floecat.storage.aws.dynamodb;

import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;

@ApplicationScoped
@IfBuildProperty(name = "floecat.kv", stringValue = "dynamodb")
@IfBuildProperty(name = "floecat.kv.auto-create", stringValue = "true")
public class DynamoPointerTableBootstrap {

  private static final String ATTR_PK = "pk";
  private static final String ATTR_SK = "sk";
  private static final String ATTR_EXPIRES_AT = "expires_at";

  @Inject DynamoDbClient dynamoDb;

  @ConfigProperty(name = "floecat.kv.table")
  String table;

  @ConfigProperty(name = "floecat.kv.ttl-enabled", defaultValue = "true")
  boolean ttlEnabled;

  void onStart(@Observes StartupEvent ev) {
    ensureTable();
    if (ttlEnabled) ensureTtl();
  }

  private void ensureTable() {
    try {
      dynamoDb.describeTable(DescribeTableRequest.builder().tableName(table).build());
    } catch (ResourceNotFoundException rnfe) {
      dynamoDb.createTable(
          CreateTableRequest.builder()
              .tableName(table)
              .attributeDefinitions(
                  AttributeDefinition.builder()
                      .attributeName(ATTR_PK)
                      .attributeType(ScalarAttributeType.S)
                      .build(),
                  AttributeDefinition.builder()
                      .attributeName(ATTR_SK)
                      .attributeType(ScalarAttributeType.S)
                      .build())
              .keySchema(
                  KeySchemaElement.builder().attributeName(ATTR_PK).keyType(KeyType.HASH).build(),
                  KeySchemaElement.builder().attributeName(ATTR_SK).keyType(KeyType.RANGE).build())
              .billingMode(BillingMode.PAY_PER_REQUEST)
              .build());

      dynamoDb
          .waiter()
          .waitUntilTableExists(DescribeTableRequest.builder().tableName(table).build());
    }
  }

  private void ensureTtl() {
    try {
      var ttlDesc =
          dynamoDb
              .describeTimeToLive(DescribeTimeToLiveRequest.builder().tableName(table).build())
              .timeToLiveDescription();
      boolean enabled = ttlDesc != null && ttlDesc.timeToLiveStatus() == TimeToLiveStatus.ENABLED;
      boolean enabling = ttlDesc != null && ttlDesc.timeToLiveStatus() == TimeToLiveStatus.ENABLING;
      if (!enabled && !enabling) {
        dynamoDb.updateTimeToLive(
            UpdateTimeToLiveRequest.builder()
                .tableName(table)
                .timeToLiveSpecification(
                    TimeToLiveSpecification.builder()
                        .attributeName(ATTR_EXPIRES_AT)
                        .enabled(true)
                        .build())
                .build());
      }
    } catch (ResourceNotFoundException e) {
      // ignore
    }
  }
}
