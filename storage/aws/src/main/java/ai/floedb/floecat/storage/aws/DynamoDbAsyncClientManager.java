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

package ai.floedb.floecat.storage.aws;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

@ApplicationScoped
public class DynamoDbAsyncClientManager {

  private static final Logger LOG = Logger.getLogger(DynamoDbAsyncClientManager.class);

  @Inject AwsClients awsClients;

  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<DynamoDbAsyncClient> current = new AtomicReference<>();

  public DynamoDbAsyncClient current() {
    requireOpen();
    DynamoDbAsyncClient existing = current.get();
    if (existing != null) {
      return existing;
    }

    DynamoDbAsyncClient next = awsClients.newDynamoDbAsyncClient();
    if (closed.get()) {
      next.close();
      requireOpen();
    }
    if (current.compareAndSet(null, next)) {
      if (closed.get() && current.compareAndSet(next, null)) {
        next.close();
        requireOpen();
      }
      return next;
    }

    next.close();
    return requireOpen(current.get());
  }

  public void refreshAfterFailure(DynamoDbAsyncClient failedClient, Throwable failure) {
    if (!ClosedAwsClientDetector.isConnectionPoolShutdown(failure)) {
      return;
    }
    if (closed.get()) {
      return;
    }
    if (failedClient == null || current.get() != failedClient) {
      return;
    }

    DynamoDbAsyncClient next = awsClients.newDynamoDbAsyncClient();
    if (closed.get()) {
      next.close();
      return;
    }
    if (current.compareAndSet(failedClient, next)) {
      closeQuietly(failedClient);
      LOG.warn("Refreshed DynamoDB async client after closed connection pool");
    } else {
      next.close();
    }
  }

  @PreDestroy
  void close() {
    if (closed.compareAndSet(false, true)) {
      closeQuietly(current.getAndSet(null));
    }
  }

  private void requireOpen() {
    if (closed.get()) {
      throw new IllegalStateException("dynamodb async client manager is shut down");
    }
  }

  private DynamoDbAsyncClient requireOpen(DynamoDbAsyncClient client) {
    requireOpen();
    if (client == null) {
      throw new IllegalStateException("dynamodb async client manager is shut down");
    }
    return client;
  }

  private static void closeQuietly(DynamoDbAsyncClient client) {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (RuntimeException ignored) {
    }
  }
}
