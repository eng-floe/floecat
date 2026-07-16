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

import ai.floedb.floecat.aws.RefreshingAwsClient;
import ai.floedb.floecat.storage.aws.AwsClients;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.Base64;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
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
    assertFalse(refreshedClient.provider.closed);
    manager.shutdown();
    assertTrue(refreshedClient.provider.closed);
    assertEquals(1, awsClients.secretsRefreshes);
    assertEquals(0, awsClients.stsRefreshes);
  }

  @Test
  void shared_secrets_provider_closes_when_refreshed_client_is_replaced() {
    ClientHandle staleClient =
        ClientHandle.secretsFailure(new RuntimeException("Connection pool shut down"));
    ClientHandle firstRefreshedClient =
        ClientHandle.secretsFailure(new RuntimeException("Connection pool shut down"));
    ClientHandle secondRefreshedClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("alpha".getBytes()));
    ClientHandle stsClient = ClientHandle.sts();

    FakeAwsClients awsClients = new FakeAwsClients();
    awsClients.refreshedSecretsClients.addLast(firstRefreshedClient);
    awsClients.refreshedSecretsClients.addLast(secondRefreshedClient);

    ProdSecretsManager manager =
        new ProdSecretsManager(awsClients, staleClient.secretsClient, stsClient.stsClient);

    assertThrows(RuntimeException.class, () -> manager.get("acct", "connectors", "secret"));
    assertFalse(firstRefreshedClient.provider.closed);

    Optional<byte[]> result = manager.get("acct", "connectors", "secret");

    assertArrayEquals("alpha".getBytes(), result.orElseThrow());
    assertTrue(firstRefreshedClient.provider.closed);
    assertFalse(secondRefreshedClient.provider.closed);
    manager.shutdown();
    assertTrue(secondRefreshedClient.provider.closed);
    assertEquals(2, awsClients.secretsRefreshes);
  }

  @Test
  void injected_shared_secrets_provider_closes_when_initial_client_is_replaced() {
    ClientHandle initialClient =
        ClientHandle.secretsFailure(new RuntimeException("Connection pool shut down"));
    ClientHandle refreshedClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("alpha".getBytes()));

    FakeAwsClients awsClients = new FakeAwsClients();
    awsClients.refreshedSecretsClients.addLast(initialClient);
    awsClients.refreshedSecretsClients.addLast(refreshedClient);

    ProdSecretsManager manager = new ProdSecretsManager(awsClients);

    Optional<byte[]> result = manager.get("acct", "connectors", "secret");

    assertArrayEquals("alpha".getBytes(), result.orElseThrow());
    assertTrue(initialClient.closed);
    assertTrue(initialClient.provider.closed);
    assertFalse(refreshedClient.provider.closed);
    manager.shutdown();
    assertTrue(refreshedClient.provider.closed);
    assertEquals(2, awsClients.secretsRefreshes);
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
  void
      concurrent_credential_pool_failures_invalidate_sts_once_and_keep_surviving_client_bound_to_open_sts()
          throws Exception {
    ClientHandle bootstrapClient = ClientHandle.secretsValue("unused");
    ClientHandle staleStsClient = ClientHandle.sts();
    ClientHandle refreshedStsClient = ClientHandle.sts();
    ClientHandle otherAccountClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("other".getBytes()));
    RuntimeException credentialFailure =
        SdkClientException.create(
            "Unable to load credentials from STS",
            new RuntimeException("Connection pool shut down"));
    CountDownLatch staleAttemptsReady = new CountDownLatch(2);
    CountDownLatch releaseStaleAttempts = new CountDownLatch(1);
    ClientHandle stalePerAccountClient =
        ClientHandle.secretsFailure(
            credentialFailure,
            () -> {
              staleAttemptsReady.countDown();
              try {
                releaseStaleAttempts.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            });
    ClientHandle refreshedPerAccountClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("beta".getBytes()));

    FakeAwsClients awsClients = new FakeAwsClients();
    awsClients.activeStsClient = staleStsClient;
    awsClients.refreshedStsClients.addLast(refreshedStsClient);
    awsClients.perAccountSecretsClients.addLast(otherAccountClient);
    awsClients.perAccountSecretsClients.addLast(stalePerAccountClient);
    awsClients.perAccountSecretsClients.addLast(refreshedPerAccountClient);

    ProdSecretsManager manager =
        new ProdSecretsManager(awsClients, bootstrapClient.secretsClient, staleStsClient.stsClient);
    manager.roleArn = Optional.of("arn:aws:iam::123456789012:role/test");
    manager.get("acct-other", "connectors", "secret");

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Optional<byte[]>> first =
          executor.submit(() -> manager.get("acct", "connectors", "secret"));
      Future<Optional<byte[]>> second =
          executor.submit(() -> manager.get("acct", "connectors", "secret"));
      assertTrue(staleAttemptsReady.await(5, TimeUnit.SECONDS));
      releaseStaleAttempts.countDown();

      assertArrayEquals("beta".getBytes(), first.get(5, TimeUnit.SECONDS).orElseThrow());
      assertArrayEquals("beta".getBytes(), second.get(5, TimeUnit.SECONDS).orElseThrow());
    } finally {
      executor.shutdownNow();
    }

    assertTrue(stalePerAccountClient.closed);
    assertEquals(1, stalePerAccountClient.closeCount);
    assertTrue(otherAccountClient.closed);
    assertEquals(1, otherAccountClient.closeCount);
    assertTrue(staleStsClient.closed);
    assertEquals(1, staleStsClient.closeCount);
    assertFalse(refreshedStsClient.closed);
    assertTrue(refreshedPerAccountClient.boundStsClient == refreshedStsClient);
    assertEquals(1, awsClients.stsRefreshes);
    assertEquals(3, awsClients.perAccountClientBuilds);
  }

  @Test
  void concurrent_credential_pool_failures_for_different_accounts_retry_through_one_refreshed_sts()
      throws Exception {
    ClientHandle bootstrapClient = ClientHandle.secretsValue("unused");
    ClientHandle staleStsClient = ClientHandle.sts();
    ClientHandle refreshedStsClient = ClientHandle.sts();
    ClientHandle otherAccountClient =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("other".getBytes()));
    RuntimeException credentialFailure =
        SdkClientException.create(
            "Unable to load credentials from STS",
            new RuntimeException("Connection pool shut down"));
    CountDownLatch staleAttemptsReady = new CountDownLatch(2);
    CountDownLatch releaseStaleAttempts = new CountDownLatch(1);
    Runnable beforeFailure =
        () -> {
          staleAttemptsReady.countDown();
          try {
            releaseStaleAttempts.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        };
    ClientHandle staleAccountA = ClientHandle.secretsFailure(credentialFailure, beforeFailure);
    ClientHandle staleAccountB = ClientHandle.secretsFailure(credentialFailure, beforeFailure);
    ClientHandle refreshedAccountA =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("alpha".getBytes()));
    ClientHandle refreshedAccountB =
        ClientHandle.secretsValue(Base64.getEncoder().encodeToString("bravo".getBytes()));

    FakeAwsClients awsClients = new FakeAwsClients();
    awsClients.activeStsClient = staleStsClient;
    awsClients.refreshedStsClients.addLast(refreshedStsClient);
    awsClients.perAccountSecretsClients.addLast(otherAccountClient);
    awsClients.perAccountSecretsClients.addLast(staleAccountA);
    awsClients.perAccountSecretsClients.addLast(staleAccountB);
    awsClients.perAccountSecretsClients.addLast(refreshedAccountA);
    awsClients.perAccountSecretsClients.addLast(refreshedAccountB);

    ProdSecretsManager manager =
        new ProdSecretsManager(awsClients, bootstrapClient.secretsClient, staleStsClient.stsClient);
    manager.roleArn = Optional.of("arn:aws:iam::123456789012:role/test");
    manager.get("acct-other", "connectors", "secret");

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Optional<byte[]>> first =
          executor.submit(() -> manager.get("acct-a", "connectors", "secret"));
      Future<Optional<byte[]>> second =
          executor.submit(() -> manager.get("acct-b", "connectors", "secret"));
      assertTrue(staleAttemptsReady.await(5, TimeUnit.SECONDS));
      releaseStaleAttempts.countDown();

      assertTrue(first.get(5, TimeUnit.SECONDS).isPresent());
      assertTrue(second.get(5, TimeUnit.SECONDS).isPresent());
    } finally {
      executor.shutdownNow();
    }

    assertTrue(staleAccountA.closed);
    assertEquals(1, staleAccountA.closeCount);
    assertTrue(staleAccountB.closed);
    assertEquals(1, staleAccountB.closeCount);
    assertTrue(otherAccountClient.closed);
    assertEquals(1, otherAccountClient.closeCount);
    assertTrue(staleStsClient.closed);
    assertEquals(1, staleStsClient.closeCount);
    assertFalse(refreshedStsClient.closed);
    assertFalse(refreshedStsClient.provider.closed);
    assertTrue(refreshedAccountA.boundStsClient == refreshedStsClient);
    assertTrue(refreshedAccountB.boundStsClient == refreshedStsClient);
    assertEquals(1, awsClients.stsRefreshes);
    assertEquals(5, awsClients.perAccountClientBuilds);
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

    assertEquals("Secrets Manager client is shut down", thrown.getMessage());
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

    assertEquals("STS client is shut down", thrown.getMessage());
  }

  private static final class FakeAwsClients extends AwsClients {
    private final Deque<ClientHandle> refreshedSecretsClients = new ArrayDeque<>();
    private final Deque<ClientHandle> refreshedStsClients = new ArrayDeque<>();
    private final Deque<ClientHandle> perAccountSecretsClients = new ArrayDeque<>();
    private ClientHandle activeStsClient;
    private int secretsRefreshes;
    private int stsRefreshes;
    private int perAccountClientBuilds;

    @Override
    public RefreshingAwsClient.ClientResource<SecretsManagerClient>
        newSecretsManagerClientResource() {
      secretsRefreshes++;
      ClientHandle handle = refreshedSecretsClients.removeFirst();
      return RefreshingAwsClient.clientResource(handle.secretsClient, handle.provider);
    }

    @Override
    public RefreshingAwsClient.ClientResource<StsClient> newStsClientResource() {
      stsRefreshes++;
      activeStsClient = refreshedStsClients.removeFirst();
      return RefreshingAwsClient.clientResource(
          activeStsClient.stsClient, activeStsClient.provider);
    }

    @Override
    public SecretsManagerClient secretsManagerClient(AwsCredentialsProvider credentialsProvider) {
      perAccountClientBuilds++;
      ClientHandle handle = perAccountSecretsClients.removeFirst();
      handle.boundStsClient = activeStsClient;
      return handle.secretsClient;
    }

    @Override
    public void ensureCredentialsAvailable() {}
  }

  private static final class ClientHandle implements InvocationHandler {
    private final RuntimeException failure;
    private final Runnable beforeFailure;
    private final String secretString;
    private boolean closed;
    private int closeCount;
    private ClientHandle boundStsClient;
    private final ProviderHandle provider = new ProviderHandle();
    private final SecretsManagerClient secretsClient;
    private final StsClient stsClient;

    private ClientHandle(RuntimeException failure, Runnable beforeFailure, String secretString) {
      this.failure = failure;
      this.beforeFailure = beforeFailure;
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
      return secretsFailure(failure, null);
    }

    static ClientHandle secretsFailure(RuntimeException failure, Runnable beforeFailure) {
      return new ClientHandle(failure, beforeFailure, null);
    }

    static ClientHandle secretsValue(String secretString) {
      return new ClientHandle(null, null, secretString);
    }

    static ClientHandle sts() {
      return new ClientHandle(null, null, null);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      return switch (method.getName()) {
        case "getSecretValue" -> {
          if (failure != null) {
            if (beforeFailure != null) {
              beforeFailure.run();
            }
            throw failure;
          }
          yield GetSecretValueResponse.builder().secretString(secretString).build();
        }
        case "close" -> {
          closed = true;
          closeCount++;
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

  private static final class ProviderHandle implements AwsCredentialsProvider, AutoCloseable {
    private boolean closed;

    @Override
    public AwsCredentials resolveCredentials() {
      return null;
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
