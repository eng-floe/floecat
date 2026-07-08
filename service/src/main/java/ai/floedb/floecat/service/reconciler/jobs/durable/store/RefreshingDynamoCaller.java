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

import ai.floedb.floecat.storage.aws.ClosedAwsClientDetector;
import ai.floedb.floecat.storage.aws.DynamoDbClientManager;
import jakarta.enterprise.inject.Instance;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

final class RefreshingDynamoCaller {
  private static final Logger LOG = Logger.getLogger(RefreshingDynamoCaller.class);

  private volatile Binding binding;

  void bind(
      Supplier<DynamoDbClient> dynamoDbSupplier,
      BiConsumer<DynamoDbClient, Throwable> clientFailureHandler) {
    if (dynamoDbSupplier == null) {
      throw new IllegalArgumentException("dynamoDbSupplier must not be null");
    }
    binding =
        new Binding(
            dynamoDbSupplier,
            clientFailureHandler == null ? (client, failure) -> {} : clientFailureHandler,
            clientFailureHandler != null);
  }

  <T> T call(
      Instance<DynamoDbClientManager> dynamoDbClientManager,
      Function<DynamoDbClient, T> operation) {
    Binding currentBinding = binding(dynamoDbClientManager);
    for (int attempt = 0; ; attempt++) {
      DynamoDbClient client = currentBinding.dynamoDbSupplier().get();
      try {
        return operation.apply(client);
      } catch (RuntimeException e) {
        if (currentBinding.supportsRefresh()
            && ClosedAwsClientDetector.isConnectionPoolShutdown(e)) {
          LOG.warnf(
              e, "DynamoDB connection pool shutdown in %s; refreshing client", callerContext());
          currentBinding.clientFailureHandler().accept(client, e);
          if (attempt == 0) {
            currentBinding = binding(dynamoDbClientManager);
            continue;
          }
        }
        throw e;
      }
    }
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
    Binding nextBinding = new Binding(manager::current, manager::refreshAfterFailure, true);
    binding = nextBinding;
    return nextBinding;
  }

  private record Binding(
      Supplier<DynamoDbClient> dynamoDbSupplier,
      BiConsumer<DynamoDbClient, Throwable> clientFailureHandler,
      boolean supportsRefresh) {}

  private static String callerContext() {
    String wrapperClassName = RefreshingDynamoCaller.class.getName();
    for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
      String className = frame.getClassName();
      if (className.equals(Thread.class.getName()) || className.equals(wrapperClassName)) {
        continue;
      }
      if (className.startsWith(wrapperClassName + "$")) {
        continue;
      }
      return className + "." + frame.getMethodName();
    }
    return "unknown";
  }
}
