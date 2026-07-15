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

import ai.floedb.floecat.storage.aws.DynamoDbClientManager;
import jakarta.enterprise.inject.Instance;
import java.util.function.Function;
import java.util.function.Supplier;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

final class RefreshingDynamoCaller {
  private volatile Binding binding;

  void bind(Supplier<DynamoDbClient> dynamoDbSupplier) {
    if (dynamoDbSupplier == null) {
      throw new IllegalArgumentException("dynamoDbSupplier must not be null");
    }
    binding = new Binding(null, dynamoDbSupplier);
  }

  void bind(DynamoDbClientManager manager) {
    if (manager == null) {
      throw new IllegalArgumentException("manager must not be null");
    }
    binding = new Binding(manager, null);
  }

  <T> T call(
      Instance<DynamoDbClientManager> dynamoDbClientManager,
      Function<DynamoDbClient, T> operation) {
    Binding currentBinding = binding(dynamoDbClientManager);
    if (currentBinding.manager() != null) {
      return currentBinding.manager().call(operation);
    }
    return operation.apply(currentBinding.dynamoDbSupplier().get());
  }

  void callVoid(
      Instance<DynamoDbClientManager> dynamoDbClientManager,
      java.util.function.Consumer<DynamoDbClient> operation) {
    call(
        dynamoDbClientManager,
        client -> {
          operation.accept(client);
          return null;
        });
  }

  private Binding binding(Instance<DynamoDbClientManager> dynamoDbClientManager) {
    Binding currentBinding = binding;
    if (currentBinding != null) {
      return currentBinding;
    }
    if (dynamoDbClientManager == null || !dynamoDbClientManager.isResolvable()) {
      throw new IllegalStateException("No DynamoDB client manager available");
    }
    DynamoDbClientManager manager = dynamoDbClientManager.get();
    Binding nextBinding = new Binding(manager, null);
    binding = nextBinding;
    return nextBinding;
  }

  private record Binding(
      DynamoDbClientManager manager, Supplier<DynamoDbClient> dynamoDbSupplier) {}
}
