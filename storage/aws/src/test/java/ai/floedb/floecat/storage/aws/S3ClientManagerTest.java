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
import software.amazon.awssdk.services.s3.S3Client;

class S3ClientManagerTest {

  @Test
  void refreshAfterFailure_replaces_client_on_connection_pool_shutdown() {
    FakeAwsClients clients = new FakeAwsClients();
    S3ClientManager manager = new S3ClientManager();
    manager.awsClients = clients;

    S3Client first = manager.current();

    manager.refreshAfterFailure(first, new IllegalStateException("Connection pool shut down"));

    S3Client second = manager.current();
    assertNotSame(first, second);
    assertEquals(2, clients.handles.size());
    assertTrue(clients.handles.get(0).closed);
    assertFalse(clients.handles.get(1).closed);
  }

  @Test
  void refreshAfterFailure_ignores_stale_failed_client() {
    FakeAwsClients clients = new FakeAwsClients();
    S3ClientManager manager = new S3ClientManager();
    manager.awsClients = clients;

    S3Client first = manager.current();
    manager.refreshAfterFailure(first, new IllegalStateException("Connection pool shut down"));
    S3Client second = manager.current();

    manager.refreshAfterFailure(first, new IllegalStateException("Connection pool shut down"));

    assertSame(second, manager.current());
    assertEquals(2, clients.handles.size());
    assertTrue(clients.handles.get(0).closed);
    assertFalse(clients.handles.get(1).closed);
  }

  @Test
  void current_after_close_throws_without_recreating_client() {
    FakeAwsClients clients = new FakeAwsClients();
    S3ClientManager manager = new S3ClientManager();
    manager.awsClients = clients;

    S3Client first = manager.current();
    manager.close();

    IllegalStateException thrown = assertThrows(IllegalStateException.class, manager::current);

    assertEquals("s3 client manager is shut down", thrown.getMessage());
    assertEquals(1, clients.handles.size());
    assertTrue(clients.handles.get(0).closed);
    assertSame(first, clients.handles.get(0).client);
  }

  private static final class FakeAwsClients extends AwsClients {
    private final List<ClientHandle> handles = new ArrayList<>();

    @Override
    public S3Client newS3Client() {
      ClientHandle handle = new ClientHandle();
      handles.add(handle);
      return handle.client;
    }
  }

  private static final class ClientHandle implements InvocationHandler {
    private final S3Client client =
        (S3Client)
            Proxy.newProxyInstance(
                S3Client.class.getClassLoader(), new Class<?>[] {S3Client.class}, this);

    private boolean closed;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      return switch (method.getName()) {
        case "close" -> {
          closed = true;
          yield null;
        }
        case "serviceName" -> "test";
        case "toString" -> "fake-s3-client";
        case "hashCode" -> System.identityHashCode(proxy);
        case "equals" -> proxy == args[0];
        default -> null;
      };
    }
  }
}
