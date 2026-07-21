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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.storage.aws.DynamoDbClientManager;
import jakarta.enterprise.inject.Instance;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

class RefreshingDynamoCallerTest {

  @Test
  void call_refreshes_manager_client_and_retries_once() {
    RefreshingDynamoCaller caller = new RefreshingDynamoCaller();
    DynamoDbClient refreshedClient = mock(DynamoDbClient.class);

    DynamoDbClientManager manager = mock(DynamoDbClientManager.class);
    when(manager.call(any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Function<DynamoDbClient, Integer> operation = invocation.getArgument(0);
              return operation.apply(refreshedClient);
            });
    @SuppressWarnings("unchecked")
    Instance<DynamoDbClientManager> managerInstance = mock(Instance.class);
    when(managerInstance.isResolvable()).thenReturn(true);
    when(managerInstance.get()).thenReturn(manager);

    int result =
        caller.call(
            managerInstance,
            client -> {
              return 7;
            });

    assertEquals(7, result);
    verify(manager).call(any());
  }

  @Test
  void call_without_refresh_handler_rethrows_closed_pool_without_retry() {
    RefreshingDynamoCaller caller = new RefreshingDynamoCaller();
    DynamoDbClient staleClient = mock(DynamoDbClient.class);
    RuntimeException closedPool = new RuntimeException("Connection pool shut down");
    caller.bind(() -> staleClient);

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () ->
                caller.call(
                    null,
                    client -> {
                      throw closedPool;
                    }));

    assertSame(closedPool, thrown);
  }
}
