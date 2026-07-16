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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.aws.RefreshingAwsClient;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

class DynamoDbAsyncClientManagerTest {

  @Test
  void callAsync_replaces_client_on_connection_pool_shutdown_completion() {
    FakeAwsClients clients = new FakeAwsClients();
    DynamoDbAsyncClientManager manager = new DynamoDbAsyncClientManager();
    manager.awsClients = clients;

    CompletionException thrown =
        assertThrows(
            CompletionException.class,
            () ->
                manager
                    .callAsync(
                        client ->
                            CompletableFuture.failedFuture(
                                new IllegalStateException("Connection pool shut down")))
                    .join());
    assertTrue(thrown.getCause() instanceof IllegalStateException);
    DynamoDbAsyncClient first = clients.handles.get(0).client;

    DynamoDbAsyncClient second = manager.current();
    assertNotSame(first, second);
    assertEquals(2, clients.handles.size());
    assertTrue(clients.handles.get(0).closed);
    assertTrue(clients.handles.get(0).resourceClosed);
    assertFalse(clients.handles.get(1).closed);
    assertFalse(clients.handles.get(1).resourceClosed);
  }

  @Test
  void callAsync_ignores_unrelated_failure() {
    FakeAwsClients clients = new FakeAwsClients();
    DynamoDbAsyncClientManager manager = new DynamoDbAsyncClientManager();
    manager.awsClients = clients;

    CompletionException thrown =
        assertThrows(
            CompletionException.class,
            () ->
                manager
                    .callAsync(
                        client ->
                            CompletableFuture.failedFuture(new IllegalStateException("throttled")))
                    .join());
    assertTrue(thrown.getCause() instanceof IllegalStateException);
    DynamoDbAsyncClient first = clients.handles.get(0).client;

    assertSame(first, manager.current());
    assertEquals(1, clients.handles.size());
    assertFalse(clients.handles.get(0).closed);
    assertFalse(clients.handles.get(0).resourceClosed);
  }

  @Test
  void current_after_close_throws_without_recreating_client() {
    FakeAwsClients clients = new FakeAwsClients();
    DynamoDbAsyncClientManager manager = new DynamoDbAsyncClientManager();
    manager.awsClients = clients;

    DynamoDbAsyncClient first = manager.current();
    manager.close();

    IllegalStateException thrown = assertThrows(IllegalStateException.class, manager::current);

    assertEquals("DynamoDB async client is shut down", thrown.getMessage());
    assertEquals(1, clients.handles.size());
    assertTrue(clients.handles.get(0).closed);
    assertTrue(clients.handles.get(0).resourceClosed);
    assertSame(first, clients.handles.get(0).client);
  }

  private static final class FakeAwsClients extends AwsClients {
    private final List<ClientHandle> handles = new ArrayList<>();

    @Override
    public RefreshingAwsClient.ClientResource<DynamoDbAsyncClient>
        newDynamoDbAsyncClientResource() {
      ClientHandle handle = new ClientHandle();
      handles.add(handle);
      return RefreshingAwsClient.clientResource(
          handle.client, (AutoCloseable) () -> handle.resourceClosed = true);
    }
  }

  private static final class ClientHandle implements InvocationHandler {
    private final DynamoDbAsyncClient client =
        (DynamoDbAsyncClient)
            Proxy.newProxyInstance(
                DynamoDbAsyncClient.class.getClassLoader(),
                new Class<?>[] {DynamoDbAsyncClient.class},
                this);

    private boolean closed;
    private boolean resourceClosed;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      return switch (method.getName()) {
        case "close" -> {
          closed = true;
          yield null;
        }
        case "toString" -> "fake-dynamodb-client";
        case "hashCode" -> System.identityHashCode(proxy);
        case "equals" -> proxy == args[0];
        default -> null;
      };
    }
  }
}
