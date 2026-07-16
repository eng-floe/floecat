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

package ai.floedb.floecat.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RefreshingAwsClientTest {

  @Test
  void callRefreshesAndRetriesCheckedClosedPoolIoException() throws Exception {
    List<TestClient> created = new ArrayList<>();
    AtomicInteger attempts = new AtomicInteger();

    try (RefreshingAwsClient<TestClient> client =
        new RefreshingAwsClient<>(
            () -> {
              TestClient next = new TestClient(created.size() + 1);
              created.add(next);
              return next;
            })) {
      String result =
          client.call(
              current -> {
                if (attempts.getAndIncrement() == 0) {
                  throw new IOException("Connection pool shut down");
                }
                return "client-" + current.id;
              });

      assertThat(result).isEqualTo("client-2");
    }

    assertThat(attempts).hasValue(2);
    assertThat(created).hasSize(2);
    assertThat(created.get(0).closed).isTrue();
    assertThat(created.get(1).closed).isTrue();
  }

  @Test
  void callUncheckedRefreshesRuntimeClosedPoolFailureOnlyOnce() {
    List<TestClient> created = new ArrayList<>();
    AtomicInteger attempts = new AtomicInteger();

    try (RefreshingAwsClient<TestClient> client =
        new RefreshingAwsClient<>(
            () -> {
              TestClient next = new TestClient(created.size() + 1);
              created.add(next);
              return next;
            })) {
      assertThatThrownBy(
              () ->
                  client.callUnchecked(
                      current -> {
                        attempts.incrementAndGet();
                        throw new IllegalStateException("Connection pool shut down");
                      }))
          .isInstanceOf(IllegalStateException.class);
    }

    assertThat(attempts).hasValue(2);
    assertThat(created).hasSize(2);
  }

  @Test
  void callUncheckedDoesNotRetryUnrelatedFailure() {
    AtomicInteger attempts = new AtomicInteger();

    try (RefreshingAwsClient<TestClient> client =
        new RefreshingAwsClient<>(() -> new TestClient(1))) {
      assertThatThrownBy(
              () ->
                  client.callUnchecked(
                      current -> {
                        attempts.incrementAndGet();
                        throw new IllegalStateException("throttled");
                      }))
          .isInstanceOf(IllegalStateException.class);
    }

    assertThat(attempts).hasValue(1);
  }

  @Test
  void detectorChecksNestedCauses() {
    Throwable failure =
        new RuntimeException("outer", new IllegalStateException("Connection pool shut down"));

    assertThat(ClosedAwsClientDetector.isConnectionPoolShutdown(failure)).isTrue();
  }

  @Test
  void callClosesAssociatedResourceWhenRefreshingClient() throws Exception {
    List<TestClient> clients = new ArrayList<>();
    List<TestResource> resources = new ArrayList<>();
    AtomicInteger attempts = new AtomicInteger();

    try (RefreshingAwsClient<TestClient> client =
        RefreshingAwsClient.withResourceFactory(
            () -> {
              TestClient nextClient = new TestClient(clients.size() + 1);
              TestResource nextResource = new TestResource();
              clients.add(nextClient);
              resources.add(nextResource);
              return RefreshingAwsClient.clientResource(nextClient, nextResource);
            })) {
      String result =
          client.call(
              current -> {
                if (attempts.getAndIncrement() == 0) {
                  throw new IOException("Connection pool shut down");
                }
                return "client-" + current.id;
              });

      assertThat(result).isEqualTo("client-2");
      assertThat(resources.get(0).closed).isTrue();
      assertThat(resources.get(1).closed).isFalse();
    }

    assertThat(clients).hasSize(2);
    assertThat(resources).hasSize(2);
    assertThat(clients.get(0).closed).isTrue();
    assertThat(clients.get(1).closed).isTrue();
    assertThat(resources.get(0).closed).isTrue();
    assertThat(resources.get(1).closed).isTrue();
  }

  @Test
  void callAsyncRetriesExceptionalCompletion() {
    List<TestClient> created = new ArrayList<>();
    AtomicInteger attempts = new AtomicInteger();

    try (RefreshingAwsClient<TestClient> client =
        new RefreshingAwsClient<>(
            () -> {
              TestClient next = new TestClient(created.size() + 1);
              created.add(next);
              return next;
            })) {
      String result =
          client
              .callAsync(
                  current -> {
                    if (attempts.getAndIncrement() == 0) {
                      return CompletableFuture.failedFuture(
                          new IllegalStateException("Connection pool shut down"));
                    }
                    return CompletableFuture.completedFuture("client-" + current.id);
                  })
              .join();

      assertThat(result).isEqualTo("client-2");
    }

    assertThat(attempts).hasValue(2);
    assertThat(created).hasSize(2);
  }

  @Test
  void callAsyncCancellationCancelsActiveStage() {
    CompletableFuture<String> pending = new CompletableFuture<>();

    try (RefreshingAwsClient<TestClient> client =
        new RefreshingAwsClient<>(() -> new TestClient(1))) {
      CompletableFuture<String> result = client.callAsync(_ -> pending);

      result.cancel(true);
    }

    assertThat(pending).isCancelled();
  }

  @Test
  void callAsyncCancellationCancelsRetryStage() {
    CompletableFuture<String> retry = new CompletableFuture<>();
    AtomicInteger attempts = new AtomicInteger();

    try (RefreshingAwsClient<TestClient> client =
        new RefreshingAwsClient<>(() -> new TestClient(1))) {
      CompletableFuture<String> result =
          client.callAsync(
              _ -> {
                if (attempts.getAndIncrement() == 0) {
                  return CompletableFuture.failedFuture(
                      new IllegalStateException("Connection pool shut down"));
                }
                return retry;
              });

      result.cancel(true);
    }

    assertThat(retry).isCancelled();
  }

  @Test
  void currentResourceCreationIsSingleFlight() throws Exception {
    AtomicInteger builds = new AtomicInteger();
    CountDownLatch firstBuildStarted = new CountDownLatch(1);
    CountDownLatch releaseFirstBuild = new CountDownLatch(1);

    try (RefreshingAwsClient<TestClient> client =
        RefreshingAwsClient.withResourceFactory(
            () -> {
              int id = builds.incrementAndGet();
              if (id == 1) {
                firstBuildStarted.countDown();
                try {
                  releaseFirstBuild.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new RuntimeException(e);
                }
              }
              return RefreshingAwsClient.clientResource(new TestClient(id));
            })) {
      ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        Future<TestClient> first = executor.submit(client::current);
        assertThat(firstBuildStarted.await(5, TimeUnit.SECONDS)).isTrue();
        Future<TestClient> second = executor.submit(client::current);

        releaseFirstBuild.countDown();

        TestClient firstClient = first.get(5, TimeUnit.SECONDS);
        TestClient secondClient = second.get(5, TimeUnit.SECONDS);
        assertThat(secondClient).isSameAs(firstClient);
        assertThat(builds).hasValue(1);
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  void closePreventsReopen() {
    RefreshingAwsClient<TestClient> client = new RefreshingAwsClient<>(() -> new TestClient(1));
    client.current();
    client.close();

    assertThatThrownBy(client::current)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("AWS client is shut down");
  }

  @Test
  void asyncNonClosedPoolFailureIsNotRetried() {
    AtomicInteger attempts = new AtomicInteger();

    try (RefreshingAwsClient<TestClient> client =
        new RefreshingAwsClient<>(() -> new TestClient(1))) {
      assertThatThrownBy(
              () ->
                  client
                      .callAsync(
                          current -> {
                            attempts.incrementAndGet();
                            return CompletableFuture.failedFuture(
                                new IllegalStateException("throttled"));
                          })
                      .join())
          .isInstanceOf(CompletionException.class);
    }

    assertThat(attempts).hasValue(1);
  }

  private static final class TestClient implements AutoCloseable {
    private final int id;
    private boolean closed;

    private TestClient(int id) {
      this.id = id;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private static final class TestResource implements AutoCloseable {
    private boolean closed;

    @Override
    public void close() {
      closed = true;
    }
  }
}
