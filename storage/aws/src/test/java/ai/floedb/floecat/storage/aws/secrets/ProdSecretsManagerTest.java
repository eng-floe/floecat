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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.floedb.floecat.storage.aws.AwsClients;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
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
  void get_refreshes_only_failed_per_account_client_after_secrets_closed_pool() {
    ClientHandle bootstrapClient = ClientHandle.secretsValue("unused");
    ClientHandle staleStsClient = ClientHandle.sts();
    ClientHandle stalePerAccountClient =
        ClientHandle.secretsFailure(new RuntimeException("Connection pool shut down"));
    ClientHandle refreshedPerAccountClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("beta".getBytes()));

    FakeAwsClients awsClients = new FakeAwsClients();
    awsClients.perAccountSecretsClients.addLast(stalePerAccountClient);
    awsClients.perAccountSecretsClients.addLast(refreshedPerAccountClient);

    ProdSecretsManager manager =
        new ProdSecretsManager(awsClients, bootstrapClient.secretsClient, staleStsClient.stsClient);
    manager.roleArn = Optional.of("arn:aws:iam::123456789012:role/test");

    Optional<byte[]> result = manager.get("acct", "connectors", "secret");

    assertArrayEquals("beta".getBytes(), result.orElseThrow());
    assertTrue(stalePerAccountClient.closed);
    assertFalse(staleStsClient.closed);
    assertEquals(2, awsClients.perAccountClientBuilds);
    assertEquals(0, awsClients.stsRefreshes);
  }

  @Test
  void per_account_secrets_pool_refresh_does_not_close_other_account_clients() {
    ClientHandle bootstrapClient = ClientHandle.secretsValue("unused");
    ClientHandle staleStsClient = ClientHandle.sts();
    ClientHandle otherAccountClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("other".getBytes()));
    ClientHandle stalePerAccountClient =
        ClientHandle.secretsFailure(new RuntimeException("Connection pool shut down"));
    ClientHandle refreshedPerAccountClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("beta".getBytes()));

    FakeAwsClients awsClients = new FakeAwsClients();
    awsClients.perAccountSecretsClients.addLast(otherAccountClient);
    awsClients.perAccountSecretsClients.addLast(stalePerAccountClient);
    awsClients.perAccountSecretsClients.addLast(refreshedPerAccountClient);

    ProdSecretsManager manager =
        new ProdSecretsManager(awsClients, bootstrapClient.secretsClient, staleStsClient.stsClient);
    manager.roleArn = Optional.of("arn:aws:iam::123456789012:role/test");

    manager.get("acct-other", "connectors", "secret");

    Optional<byte[]> result = manager.get("acct", "connectors", "secret");

    assertArrayEquals("beta".getBytes(), result.orElseThrow());
    assertTrue(stalePerAccountClient.closed);
    assertFalse(otherAccountClient.closed);
    assertFalse(staleStsClient.closed);
    assertEquals(3, awsClients.perAccountClientBuilds);
    assertEquals(0, awsClients.stsRefreshes);
  }

  @Test
  void credential_pool_refresh_replaces_sts_and_closes_clients_bound_to_previous_sts() {
    ClientHandle bootstrapClient = ClientHandle.secretsValue("unused");
    ClientHandle staleStsClient = ClientHandle.sts();
    ClientHandle refreshedStsClient = ClientHandle.sts();
    ClientHandle otherAccountClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("other".getBytes()));
    RuntimeException credentialFailure =
        SdkClientException.create(
            "Unable to load credentials from STS",
            new RuntimeException("Connection pool shut down"));
    ClientHandle stalePerAccountClient = ClientHandle.secretsFailure(credentialFailure);
    ClientHandle refreshedPerAccountClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("beta".getBytes()));

    FakeAwsClients awsClients = new FakeAwsClients();
    awsClients.refreshedStsClients.addLast(refreshedStsClient);
    awsClients.perAccountSecretsClients.addLast(otherAccountClient);
    awsClients.perAccountSecretsClients.addLast(stalePerAccountClient);
    awsClients.perAccountSecretsClients.addLast(refreshedPerAccountClient);

    ProdSecretsManager manager =
        new ProdSecretsManager(awsClients, bootstrapClient.secretsClient, staleStsClient.stsClient);
    manager.roleArn = Optional.of("arn:aws:iam::123456789012:role/test");

    manager.get("acct-other", "connectors", "secret");

    Optional<byte[]> result = manager.get("acct", "connectors", "secret");

    assertArrayEquals("beta".getBytes(), result.orElseThrow());
    assertTrue(stalePerAccountClient.closed);
    assertTrue(otherAccountClient.closed);
    assertTrue(staleStsClient.closed);
    assertEquals(3, awsClients.perAccountClientBuilds);
    assertEquals(1, awsClients.stsRefreshes);
  }

  @Test
  void concurrent_credential_pool_failures_invalidate_sts_once_and_rebuild_both_accounts()
      throws Exception {
    ClientHandle bootstrapClient = ClientHandle.secretsValue("unused");
    ClientHandle staleStsClient = ClientHandle.sts();
    ClientHandle refreshedStsClient = ClientHandle.sts();
    ClientHandle accountAClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("warm-a".getBytes()));
    ClientHandle accountBClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("warm-b".getBytes()));
    ClientHandle refreshedAccountAClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("fresh-a".getBytes()));
    ClientHandle refreshedAccountBClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("fresh-b".getBytes()));
    RuntimeException credentialFailure =
        SdkClientException.create(
            "Unable to load credentials from STS",
            new RuntimeException("Connection pool shut down"));
    CountDownLatch bothFailuresReady = new CountDownLatch(2);
    CountDownLatch releaseFailures = new CountDownLatch(1);

    FakeAwsClients awsClients = new FakeAwsClients();
    awsClients.refreshedStsClients.addLast(refreshedStsClient);
    awsClients.perAccountSecretsClients.addLast(accountAClient);
    awsClients.perAccountSecretsClients.addLast(accountBClient);
    awsClients.perAccountSecretsClients.addLast(refreshedAccountAClient);
    awsClients.perAccountSecretsClients.addLast(refreshedAccountBClient);

    ProdSecretsManager manager =
        new ProdSecretsManager(awsClients, bootstrapClient.secretsClient, staleStsClient.stsClient);
    manager.roleArn = Optional.of("arn:aws:iam::123456789012:role/test");

    assertArrayEquals(
        "warm-a".getBytes(), manager.get("acct-a", "connectors", "secret").orElseThrow());
    assertArrayEquals(
        "warm-b".getBytes(), manager.get("acct-b", "connectors", "secret").orElseThrow());

    accountAClient.failWith(credentialFailure, bothFailuresReady, releaseFailures);
    accountBClient.failWith(credentialFailure, bothFailuresReady, releaseFailures);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Optional<byte[]>> accountA =
          executor.submit(() -> manager.get("acct-a", "connectors", "secret"));
      Future<Optional<byte[]>> accountB =
          executor.submit(() -> manager.get("acct-b", "connectors", "secret"));
      assertTrue(bothFailuresReady.await(5, TimeUnit.SECONDS));
      releaseFailures.countDown();

      Set<String> refreshedPayloads =
          Set.of(
              new String(accountA.get(5, TimeUnit.SECONDS).orElseThrow(), StandardCharsets.UTF_8),
              new String(accountB.get(5, TimeUnit.SECONDS).orElseThrow(), StandardCharsets.UTF_8));
      assertEquals(Set.of("fresh-a", "fresh-b"), refreshedPayloads);
    } finally {
      executor.shutdownNow();
    }

    assertTrue(staleStsClient.closed);
    assertTrue(accountAClient.closed);
    assertTrue(accountBClient.closed);
    assertFalse(refreshedStsClient.closed);
    assertFalse(refreshedAccountAClient.closed);
    assertFalse(refreshedAccountBClient.closed);
    assertEquals(1, awsClients.stsRefreshes);
    assertEquals(4, awsClients.perAccountClientBuilds);
  }

  @Test
  void stale_failed_per_account_client_does_not_evict_newer_client() throws Exception {
    ClientHandle bootstrapClient = ClientHandle.secretsValue("unused");
    ClientHandle staleStsClient = ClientHandle.sts();
    ClientHandle stalePerAccountClient =
        ClientHandle.secretsFailure(new RuntimeException("Connection pool shut down"));
    ClientHandle newerPerAccountClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("gamma".getBytes()));

    FakeAwsClients awsClients = new FakeAwsClients();
    awsClients.perAccountSecretsClients.addLast(stalePerAccountClient);
    awsClients.perAccountSecretsClients.addLast(newerPerAccountClient);

    ProdSecretsManager manager =
        new ProdSecretsManager(awsClients, bootstrapClient.secretsClient, staleStsClient.stsClient);
    manager.roleArn = Optional.of("arn:aws:iam::123456789012:role/test");

    manager.get("acct", "connectors", "secret");

    invokeRefresh(
        manager,
        "acct",
        stalePerAccountClient.secretsClient,
        new RuntimeException("Connection pool shut down"));

    assertTrue(stalePerAccountClient.closed);
    assertFalse(newerPerAccountClient.closed);
    assertEquals(0, awsClients.stsRefreshes);
    assertSameMappedClient(manager, "acct", newerPerAccountClient.secretsClient);
  }

  @Test
  void direct_client_after_shutdown_throws_clear_state_error() {
    ClientHandle directClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("unused".getBytes()));
    ClientHandle stsClient = ClientHandle.sts();
    ProdSecretsManager manager =
        new ProdSecretsManager(
            new FakeAwsClients(), directClient.secretsClient, stsClient.stsClient);

    manager.shutdown();

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> manager.get("acct", "connectors", "secret"));

    assertEquals("secrets manager is shut down", thrown.getMessage());
  }

  @Test
  void role_client_after_shutdown_throws_clear_state_error() {
    ClientHandle directClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("unused".getBytes()));
    ClientHandle stsClient = ClientHandle.sts();
    ProdSecretsManager manager =
        new ProdSecretsManager(
            new FakeAwsClients(), directClient.secretsClient, stsClient.stsClient);
    manager.roleArn = Optional.of("arn:aws:iam::123456789012:role/test");

    manager.shutdown();

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> manager.get("acct", "connectors", "secret"));

    assertEquals("secrets manager is shut down", thrown.getMessage());
  }

  private static final class FakeAwsClients extends AwsClients {
    private final Deque<ClientHandle> refreshedSecretsClients = new ArrayDeque<>();
    private final Deque<ClientHandle> refreshedStsClients = new ArrayDeque<>();
    private final Deque<ClientHandle> perAccountSecretsClients = new ArrayDeque<>();
    private int secretsRefreshes;
    private int stsRefreshes;
    private int perAccountClientBuilds;

    @Override
    public synchronized SecretsManagerClient secretsManagerClient() {
      secretsRefreshes++;
      return removeFirst(refreshedSecretsClients, "refreshed Secrets Manager client").secretsClient;
    }

    @Override
    public synchronized StsClient stsClient() {
      stsRefreshes++;
      return removeFirst(refreshedStsClients, "refreshed STS client").stsClient;
    }

    @Override
    public synchronized SecretsManagerClient secretsManagerClient(
        AwsCredentialsProvider credentialsProvider) {
      perAccountClientBuilds++;
      return removeFirst(perAccountSecretsClients, "per-account Secrets Manager client")
          .secretsClient;
    }

    @Override
    public void ensureCredentialsAvailable() {}

    private static ClientHandle removeFirst(Deque<ClientHandle> handles, String description) {
      if (handles.isEmpty()) {
        fail("unexpected request for " + description);
      }
      return handles.removeFirst();
    }
  }

  @SuppressWarnings("unchecked")
  private static void assertSameMappedClient(
      ProdSecretsManager manager, String accountId, SecretsManagerClient expected)
      throws Exception {
    Field field = ProdSecretsManager.class.getDeclaredField("perAccountClients");
    field.setAccessible(true);
    Map<String, SecretsManagerClient> clients =
        (Map<String, SecretsManagerClient>) field.get(manager);
    assertTrue(clients.get(accountId) == expected);
  }

  private static void invokeRefresh(
      ProdSecretsManager manager,
      String accountId,
      SecretsManagerClient failedClient,
      Throwable failure)
      throws Exception {
    Method refresh =
        ProdSecretsManager.class.getDeclaredMethod(
            "refreshAfterClosedPool", String.class, SecretsManagerClient.class, Throwable.class);
    refresh.setAccessible(true);
    refresh.invoke(manager, accountId, failedClient, failure);
  }

  private static final class ClientHandle implements InvocationHandler {
    private volatile RuntimeException failure;
    private volatile CountDownLatch failureReady;
    private volatile CountDownLatch releaseFailure;
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

    void failWith(
        RuntimeException failure, CountDownLatch failureReady, CountDownLatch releaseFailure) {
      this.failure = failure;
      this.failureReady = failureReady;
      this.releaseFailure = releaseFailure;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      return switch (method.getName()) {
        case "getSecretValue" -> {
          if (failure != null) {
            awaitFailureRelease();
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

    private void awaitFailureRelease() {
      CountDownLatch ready = failureReady;
      CountDownLatch release = releaseFailure;
      if (ready == null || release == null) {
        return;
      }
      ready.countDown();
      try {
        if (!release.await(5, TimeUnit.SECONDS)) {
          throw new AssertionError("timed out waiting to release scripted failure");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted while waiting to release scripted failure", e);
      }
    }
  }
}
