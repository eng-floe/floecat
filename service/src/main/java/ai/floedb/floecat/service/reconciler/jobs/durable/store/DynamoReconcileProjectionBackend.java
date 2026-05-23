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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import ai.floedb.floecat.common.rpc.Pointer;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "dynamodb")
public class DynamoReconcileProjectionBackend implements ReconcileProjectionBackend {
  @Inject DynamoDbClient dynamoDb;

  @ConfigProperty(name = "floecat.kv.table", defaultValue = "floecat_pointers")
  String table = "floecat_pointers";

  @Inject
  public DynamoReconcileProjectionBackend() {}

  public void bind(DynamoDbClient dynamoDb, String table) {
    this.dynamoDb = dynamoDb;
    this.table = table;
  }

  @Override
  public Optional<Pointer> loadPointer(String key) {
    return DynamoPointerBackendSupport.loadPointer(dynamoDb, table, key);
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextPageToken) {
    return DynamoPointerBackendSupport.listPointersByPrefix(
        dynamoDb, table, prefix, limit, pageToken, nextPageToken);
  }

  @Override
  public boolean compareAndSetBatch(ProjectionWriteBatch batch) {
    return DynamoPointerBackendSupport.compareAndSetBatch(
        dynamoDb, table, ProjectionWriteBatchSupport.toCasOps(batch));
  }
}
