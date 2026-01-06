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

import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter;

@ApplicationScoped
@Startup
public class DynamoDbTablesBootstrap implements DynamoDbSchema {

  private static final Logger log = LoggerFactory.getLogger(DynamoDbTablesBootstrap.class);

  @Inject DynamoDbAsyncClient ddb;

  // default off; enable per profile
  @ConfigProperty(name = "floecat.kv.auto-create", defaultValue = "false")
  boolean enabled;

  @ConfigProperty(name = "floecat.kv.bootstrap.wait-seconds", defaultValue = "20")
  int waitSeconds;

  public void ensureTableExists(String tableName, boolean withTtl) {
    ensureTableExists(tableName);
    ensureTtlEnabled(tableName, ATTR_TTL);
  }

  private void ensureTableExists(String tableName) {
    if (!enabled) {
      log.info("DynamoDB table bootstrap is disabled.");
      return;
    }
    if (tableName == null || tableName.isBlank()) {
      throw new IllegalStateException("Table name is blank");
    }

    if (tableExists(tableName)) {
      log.info("DynamoDB table already exists: {}", tableName);
      return;
    }

    log.info("Creating DynamoDB table: {}", tableName);

    var req =
        CreateTableRequest.builder()
            .tableName(tableName)
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(ATTR_PARTITION_KEY)
                    .attributeType(ScalarAttributeType.S)
                    .build(),
                AttributeDefinition.builder()
                    .attributeName(ATTR_SORT_KEY)
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName(ATTR_PARTITION_KEY)
                    .keyType(KeyType.HASH)
                    .build(),
                KeySchemaElement.builder()
                    .attributeName(ATTR_SORT_KEY)
                    .keyType(KeyType.RANGE)
                    .build())
            .build();

    ddb.createTable(req).join();

    DynamoDbAsyncWaiter waiter = ddb.waiter();
    waiter
        .waitUntilTableExists(DescribeTableRequest.builder().tableName(tableName).build())
        .orTimeout(waitSeconds, TimeUnit.SECONDS)
        .join();

    // Optional: also wait until ACTIVE (Local usually is, AWS can lag briefly)
    waitUntilActive(tableName);

    log.info("DynamoDB table ready: {}", tableName);
  }

  private void ensureTtlEnabled(String tableName, String ttlAttrName) {
    try {
      var current =
          ddb.describeTimeToLive(DescribeTimeToLiveRequest.builder().tableName(tableName).build())
              .join();

      var status =
          current.timeToLiveDescription() != null
              ? current.timeToLiveDescription().timeToLiveStatus()
              : null;

      // ENABLING can take time; treat it as "good enough" for bootstrap
      if (TimeToLiveStatus.ENABLED.equals(status) || TimeToLiveStatus.ENABLING.equals(status)) {
        log.info(
            "TTL already enabled (or enabling) for table {} on attribute {}",
            tableName,
            ttlAttrName);
        return;
      }
    } catch (Throwable t) {
      // Some environments can throw if TTL isn't configured yet; fall through and try enabling.
      log.debug(
          "describeTimeToLive failed for {} (will try enabling): {}", tableName, t.toString());
    }

    log.info("Enabling TTL for table {} on attribute {}", tableName, ttlAttrName);

    var update =
        UpdateTimeToLiveRequest.builder()
            .tableName(tableName)
            .timeToLiveSpecification(
                TimeToLiveSpecification.builder().attributeName(ttlAttrName).enabled(true).build())
            .build();

    ddb.updateTimeToLive(update).join();
  }

  private boolean tableExists(String tableName) {
    try {
      ddb.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).join();
      return true;
    } catch (Throwable t) {
      return isResourceNotFound(t) ? false : rethrow(t);
    }
  }

  private void waitUntilActive(String tableName) {
    try {
      // quick polling loop; avoids more waiter APIs
      long deadline = System.currentTimeMillis() + (waitSeconds * 1000L);
      while (System.currentTimeMillis() < deadline) {
        var desc =
            ddb.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).join();
        if (desc.table() != null && TableStatus.ACTIVE.equals(desc.table().tableStatus())) {
          return;
        }
        Thread.sleep(250);
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      // ignore and continue; existence waiter already ran
      log.debug("waitUntilActive ignored error: {}", t.toString());
    }
  }

  private static boolean isResourceNotFound(Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      if (cur instanceof ResourceNotFoundException) return true;
      cur = cur.getCause();
    }
    return false;
  }

  private static <T> T rethrow(Throwable t) {
    if (t instanceof RuntimeException re) throw re;
    throw new RuntimeException(t);
  }
}
