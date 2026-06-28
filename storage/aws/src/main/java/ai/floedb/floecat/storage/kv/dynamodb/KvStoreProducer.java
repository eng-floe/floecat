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
package ai.floedb.floecat.storage.kv.dynamodb;

import ai.floedb.floecat.storage.aws.DynamoDbAsyncClientManager;
import ai.floedb.floecat.storage.kv.KvStore;
import ai.floedb.floecat.storage.kv.cdi.KvTable;
import io.quarkus.arc.Arc;
import io.quarkus.arc.properties.IfBuildProperty;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

@ApplicationScoped
@IfBuildProperty(name = "floecat.kv", stringValue = "dynamodb")
public class KvStoreProducer {
  static final int BOOTSTRAP_PRIORITY = 1;

  @Inject DynamoDbTablesBootstrap ddbBootstrap;

  @ConfigProperty(name = "floecat.kv.table")
  String table;

  @ConfigProperty(name = "floecat.kv.ttl-enabled", defaultValue = "true")
  boolean ttlEnabled;

  @ConfigProperty(name = "floecat.kv", defaultValue = "memory")
  String kvMode;

  @ConfigProperty(name = "floecat.kv.auto-create", defaultValue = "false")
  boolean autoCreate;

  void onStart(@Observes @Priority(BOOTSTRAP_PRIORITY) StartupEvent event) {
    if (!"dynamodb".equalsIgnoreCase(kvMode) || !autoCreate) {
      return;
    }
    ddbBootstrap.ensureTableExists(table, ttlEnabled);
    DynamoDbBootstrapReadiness.markReady();
  }

  @Produces
  @ApplicationScoped
  @KvTable("floecat")
  KvStore coreKvStore(
      DynamoDbAsyncClient ddb, @ConfigProperty(name = "floecat.kv.table") String table) {
    var manager = Arc.container().select(DynamoDbAsyncClientManager.class);
    if (manager.isResolvable()) {
      DynamoDbAsyncClientManager clientManager = manager.get();
      return new DynamoDbKvStore(clientManager::current, table, clientManager::refreshAfterFailure);
    }
    return new DynamoDbKvStore(ddb, table);
  }
}
