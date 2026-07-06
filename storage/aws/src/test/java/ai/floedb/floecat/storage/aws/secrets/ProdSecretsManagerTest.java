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

package ai.floedb.floecat.storage.aws.secrets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.storage.aws.AwsClients;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.sts.StsClient;

class ProdSecretsManagerTest {

  @Test
  void get_refreshes_direct_secrets_client_after_closed_pool() {
    ClientHandle staleClient =
        ClientHandle.secretsFailure(new RuntimeException("Connection pool shut down"));
    ClientHandle refreshedClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("alpha".getBytes()));
    ClientHandle stsClient = ClientHandle.sts();

    FakeAwsClients awsClients = new FakeAwsClients();
    awsClients.refreshedSecretsClients.addLast(refreshedClient);

    ProdSecretsManager manager =
        new ProdSecretsManager(awsClients, staleClient.secretsClient, stsClient.stsClient);

    Optional<byte[]> result = manager.get("acct", "connectors", "secret");

    assertArrayEquals("alpha".getBytes(), result.orElseThrow());
    assertTrue(staleClient.closed);
    assertEquals(1, awsClients.secretsRefreshes);
    assertEquals(0, awsClients.stsRefreshes);
  }

  @Test
  void get_refreshes_sts_and_per_account_client_after_closed_pool() {
    ClientHandle bootstrapClient = ClientHandle.secretsValue("unused");
    ClientHandle staleStsClient = ClientHandle.sts();
    ClientHandle refreshedStsClient = ClientHandle.sts();
    ClientHandle stalePerAccountClient =
        ClientHandle.secretsFailure(new RuntimeException("Connection pool shut down"));
    ClientHandle refreshedPerAccountClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("beta".getBytes()));

    FakeAwsClients awsClients = new FakeAwsClients();
    awsClients.refreshedStsClients.addLast(refreshedStsClient);
    awsClients.perAccountSecretsClients.addLast(stalePerAccountClient);
    awsClients.perAccountSecretsClients.addLast(refreshedPerAccountClient);

    ProdSecretsManager manager =
        new ProdSecretsManager(awsClients, bootstrapClient.secretsClient, staleStsClient.stsClient);
    manager.roleArn = Optional.of("arn:aws:iam::123456789012:role/test");

    Optional<byte[]> result = manager.get("acct", "connectors", "secret");

    assertArrayEquals("beta".getBytes(), result.orElseThrow());
    assertTrue(stalePerAccountClient.closed);
    assertTrue(staleStsClient.closed);
    assertEquals(2, awsClients.perAccountClientBuilds);
    assertEquals(1, awsClients.stsRefreshes);
  }

  @Test
  void stale_failed_per_account_client_does_not_evict_newer_client() throws Exception {
    ClientHandle bootstrapClient = ClientHandle.secretsValue("unused");
    ClientHandle staleStsClient = ClientHandle.sts();
    ClientHandle refreshedStsClient = ClientHandle.sts();
    ClientHandle stalePerAccountClient = ClientHandle.secretsFailure(new RuntimeException("Connection pool shut down"));
    ClientHandle newerPerAccountClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("gamma".getBytes()));

    FakeAwsClients awsClients = new FakeAwsClients();
    awsClients.refreshedStsClients.addLast(refreshedStsClient);
    awsClients.perAccountSecretsClients.addLast(stalePerAccountClient);
    awsClients.perAccountSecretsClients.addLast(newerPerAccountClient);

    ProdSecretsManager manager =
        new ProdSecretsManager(awsClients, bootstrapClient.secretsClient, staleStsClient.stsClient);
    manager.roleArn = Optional.of("arn:aws:iam::123456789012:role/test");

    manager.get("acct", "connectors", "secret");

    invokeRefresh(manager, "acct", stalePerAccountClient.secretsClient);

    assertTrue(stalePerAccountClient.closed);
    assertTrue(!newerPerAccountClient.closed);
    assertEquals(1, awsClients.stsRefreshes);
    assertSameMappedClient(manager, "acct", newerPerAccountClient.secretsClient);
  }

  private static final class FakeAwsClients extends AwsClients {
    private final Deque<ClientHandle> refreshedSecretsClients = new ArrayDeque<>();
    private final Deque<ClientHandle> refreshedStsClients = new ArrayDeque<>();
    private final Deque<ClientHandle> perAccountSecretsClients = new ArrayDeque<>();
    private int secretsRefreshes;
    private int stsRefreshes;
    private int perAccountClientBuilds;

    @Override
    public SecretsManagerClient secretsManagerClient() {
      secretsRefreshes++;
      return refreshedSecretsClients.removeFirst().secretsClient;
    }

    @Override
    public StsClient stsClient() {
      stsRefreshes++;
      return refreshedStsClients.removeFirst().stsClient;
    }

    @Override
    public SecretsManagerClient secretsManagerClient(AwsCredentialsProvider credentialsProvider) {
      perAccountClientBuilds++;
      return perAccountSecretsClients.removeFirst().secretsClient;
    }

    @Override
    public void ensureCredentialsAvailable() {}
  }

  @SuppressWarnings("unchecked")
  private static void assertSameMappedClient(
      ProdSecretsManager manager, String accountId, SecretsManagerClient expected) throws Exception {
    Field field = ProdSecretsManager.class.getDeclaredField("perAccountClients");
    field.setAccessible(true);
    Map<String, SecretsManagerClient> clients =
        (Map<String, SecretsManagerClient>) field.get(manager);
    assertTrue(clients.get(accountId) == expected);
  }

  private static void invokeRefresh(
      ProdSecretsManager manager, String accountId, SecretsManagerClient failedClient)
      throws Exception {
    Method refresh =
        ProdSecretsManager.class.getDeclaredMethod(
            "refreshAfterClosedPool", String.class, SecretsManagerClient.class);
    refresh.setAccessible(true);
    refresh.invoke(manager, accountId, failedClient);
  }

  private static final class ClientHandle implements InvocationHandler {
    private final RuntimeException failure;
    private final String secretString;
    private boolean closed;
    private final SecretsManagerClient secretsClient;
    private final StsClient stsClient;

    private ClientHandle(RuntimeException failure, String secretString) {
      this.failure = failure;
      this.secretString = secretString;
      this.secretsClient =
          (SecretsManagerClient)
              Proxy.newProxyInstance(
                  SecretsManagerClient.class.getClassLoader(),
                  new Class<?>[] {SecretsManagerClient.class},
                  this);
      this.stsClient =
          (StsClient)
              Proxy.newProxyInstance(
                  StsClient.class.getClassLoader(), new Class<?>[] {StsClient.class}, this);
    }

    static ClientHandle secretsFailure(RuntimeException failure) {
      return new ClientHandle(failure, null);
    }

    static ClientHandle secretsValue(String secretString) {
      return new ClientHandle(null, secretString);
    }

    static ClientHandle sts() {
      return new ClientHandle(null, null);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      return switch (method.getName()) {
        case "getSecretValue" -> {
          if (failure != null) {
            throw failure;
          }
          yield GetSecretValueResponse.builder().secretString(secretString).build();
        }
        case "close" -> {
          closed = true;
          yield null;
        }
        case "serviceName" -> "test";
        case "toString" -> "client-handle";
        case "hashCode" -> System.identityHashCode(proxy);
        case "equals" -> proxy == args[0];
        default -> null;
      };
    }
  }
}
