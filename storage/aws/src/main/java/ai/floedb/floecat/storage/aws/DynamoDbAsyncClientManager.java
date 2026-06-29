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
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

@ApplicationScoped
public class DynamoDbAsyncClientManager {

  private static final Logger LOG = Logger.getLogger(DynamoDbAsyncClientManager.class);

  @Inject AwsClients awsClients;

  private final AtomicReference<DynamoDbAsyncClient> current = new AtomicReference<>();

  public DynamoDbAsyncClient current() {
    DynamoDbAsyncClient existing = current.get();
    if (existing != null) {
      return existing;
    }

    DynamoDbAsyncClient next = awsClients.newDynamoDbAsyncClient();
    if (current.compareAndSet(null, next)) {
      return next;
    }

    next.close();
    return current.get();
  }

  public void refreshAfterFailure(Throwable failure) {
    if (!ClosedAwsClientDetector.isConnectionPoolShutdown(failure)) {
      return;
    }

    DynamoDbAsyncClient previous = current.get();
    DynamoDbAsyncClient next = awsClients.newDynamoDbAsyncClient();
    if (current.compareAndSet(previous, next)) {
      closeQuietly(previous);
      LOG.warn("Refreshed DynamoDB async client after closed connection pool");
    } else {
      next.close();
    }
  }

  @PreDestroy
  void close() {
    closeQuietly(current.getAndSet(null));
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
