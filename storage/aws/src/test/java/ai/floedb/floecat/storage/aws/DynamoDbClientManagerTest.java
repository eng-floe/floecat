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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

class DynamoDbClientManagerTest {

  @Test
  void refreshAfterFailure_replaces_client_on_connection_pool_shutdown() {
    FakeAwsClients clients = new FakeAwsClients();
    DynamoDbClientManager manager = new DynamoDbClientManager();
    manager.awsClients = clients;

    DynamoDbClient first = manager.current();

    manager.refreshAfterFailure(first, new IllegalStateException("Connection pool shut down"));

    DynamoDbClient second = manager.current();
    assertNotSame(first, second);
    assertEquals(2, clients.handles.size());
    assertTrue(clients.handles.get(0).closed);
    assertFalse(clients.handles.get(1).closed);
  }

  @Test
  void refreshAfterFailure_ignores_unrelated_failure() {
    FakeAwsClients clients = new FakeAwsClients();
    DynamoDbClientManager manager = new DynamoDbClientManager();
    manager.awsClients = clients;

    DynamoDbClient first = manager.current();

    manager.refreshAfterFailure(first, new IllegalStateException("throttled"));

    assertSame(first, manager.current());
    assertEquals(1, clients.handles.size());
    assertFalse(clients.handles.get(0).closed);
  }

  @Test
  void refreshAfterFailure_ignores_stale_failed_client() {
    FakeAwsClients clients = new FakeAwsClients();
    DynamoDbClientManager manager = new DynamoDbClientManager();
    manager.awsClients = clients;

    DynamoDbClient first = manager.current();
    manager.refreshAfterFailure(first, new IllegalStateException("Connection pool shut down"));
    DynamoDbClient second = manager.current();

    manager.refreshAfterFailure(first, new IllegalStateException("Connection pool shut down"));

    assertSame(second, manager.current());
    assertEquals(2, clients.handles.size());
    assertTrue(clients.handles.get(0).closed);
    assertFalse(clients.handles.get(1).closed);
  }

  @Test
  void current_after_close_throws_without_recreating_client() {
    FakeAwsClients clients = new FakeAwsClients();
    DynamoDbClientManager manager = new DynamoDbClientManager();
    manager.awsClients = clients;

    DynamoDbClient first = manager.current();
    manager.close();

    IllegalStateException thrown = assertThrows(IllegalStateException.class, manager::current);

    assertEquals("dynamodb client manager is shut down", thrown.getMessage());
    assertEquals(1, clients.handles.size());
    assertTrue(clients.handles.get(0).closed);
    assertSame(first, clients.handles.get(0).client);
  }

  private static final class FakeAwsClients extends AwsClients {
    private final List<ClientHandle> handles = new ArrayList<>();

    @Override
    public DynamoDbClient newDynamoDbClient() {
      ClientHandle handle = new ClientHandle();
      handles.add(handle);
      return handle.client;
    }
  }

  private static final class ClientHandle implements InvocationHandler {
    private final DynamoDbClient client =
        (DynamoDbClient)
            Proxy.newProxyInstance(
                DynamoDbClient.class.getClassLoader(), new Class<?>[] {DynamoDbClient.class}, this);

    private boolean closed;

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
