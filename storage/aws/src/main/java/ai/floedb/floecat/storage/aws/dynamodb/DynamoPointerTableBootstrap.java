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
