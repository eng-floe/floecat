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
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@ApplicationScoped
public class DynamoDbClientManager {

  private static final Logger LOG = Logger.getLogger(DynamoDbClientManager.class);

  @Inject AwsClients awsClients;

  private final AtomicReference<DynamoDbClient> current = new AtomicReference<>();

  public DynamoDbClient current() {
    DynamoDbClient existing = current.get();
    if (existing != null) {
      return existing;
    }

    DynamoDbClient next = awsClients.newDynamoDbClient();
    if (current.compareAndSet(null, next)) {
      return next;
    }

    next.close();
    return current.get();
  }

  public void refreshAfterFailure(DynamoDbClient failedClient, Throwable failure) {
    if (!ClosedAwsClientDetector.isConnectionPoolShutdown(failure)) {
      return;
    }
    if (failedClient == null || current.get() != failedClient) {
      return;
    }

    DynamoDbClient next = awsClients.newDynamoDbClient();
    if (current.compareAndSet(failedClient, next)) {
      closeQuietly(failedClient);
      LOG.warn("Refreshed DynamoDB client after closed connection pool");
    } else {
      next.close();
    }
  }

  @PreDestroy
  void close() {
    closeQuietly(current.getAndSet(null));
  }

  private static void closeQuietly(DynamoDbClient client) {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (RuntimeException ignored) {
    }
  }
}
