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
package ai.floedb.floecat.service.concurrent;

import static ai.floedb.floecat.service.testsupport.ConcurrentTestSupport.await;
import static ai.floedb.floecat.service.testsupport.ConcurrentTestSupport.awaitUninterruptibly;
import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.context.PropagatedContext;
import io.grpc.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** Verifies unary and streaming RPC cancellation reaches metadata admission waits. */
class MetadataRpcCancellationTest {

  @Test
  void cancelledUniAbandonsQueuedMetadataRead() throws Exception {
    try (SaturatedAdmission admission = SaturatedAdmission.start()) {
      AtomicBoolean queuedBackendStarted = new AtomicBoolean();
      Cancellable subscription =
          new TestService()
              .runRead(admission.reader(), () -> queuedBackendStarted.getAndSet(true))
              .subscribe()
              .with(ignored -> {}, ignored -> {});

      assertCancellationAbandonsRead(admission.runner(), subscription, queuedBackendStarted);
    }
  }

  @Test
  void cancelledGrpcStreamAbandonsQueuedMetadataRead() throws Exception {
    try (SaturatedAdmission admission = SaturatedAdmission.start()) {
      AtomicBoolean queuedBackendStarted = new AtomicBoolean();
      CompletableFuture<Throwable> failure = new CompletableFuture<>();
      try (Context.CancellableContext grpcContext = Context.current().withCancellation()) {
        grpcContext.call(
            () ->
                new TestService()
                    .streamRead(admission.reader(), () -> queuedBackendStarted.getAndSet(true))
                    .subscribe()
                    .with(ignored -> {}, failure::complete));

        await(() -> admission.runner().admissionWaiters() == 1);
        grpcContext.cancel(null);
        await(() -> admission.runner().admissionWaiters() == 0);
        assertThat(failure.get(2, TimeUnit.SECONDS))
            .isInstanceOf(java.util.concurrent.CancellationException.class);
        assertThat(queuedBackendStarted).isFalse();
      }
    }
  }

  private static void assertCancellationAbandonsRead(
      MetadataIoRunner runner, Cancellable subscription, AtomicBoolean backendStarted)
      throws InterruptedException {
    await(() -> runner.admissionWaiters() == 1);
    subscription.cancel();
    await(() -> runner.admissionWaiters() == 0);
    assertThat(backendStarted).isFalse();
  }

  /** Exposes the production RPC execution boundary to the admission regression. */
  private static final class TestService extends BaseServiceImpl {
    <T> Uni<T> runRead(MetadataResourceReader reader, Supplier<T> backend) {
      return run(() -> reader.read(backend));
    }

    <T> Multi<T> streamRead(MetadataResourceReader reader, Supplier<T> backend) {
      return runStream(
          (ignored, cancelled) ->
              Multi.createFrom()
                  .item(
                      () -> {
                        try (var cancellationScope =
                            PropagatedContext.bindCancellation(cancelled)) {
                          return reader.read(backend);
                        }
                      }));
    }
  }

  /** One occupied permit and its admitted reader, released when the test scope closes. */
  private record SaturatedAdmission(
      MetadataIoRunner runner,
      MetadataResourceReader reader,
      CountDownLatch releaseHolder,
      CompletableFuture<Void> holder)
      implements AutoCloseable {

    static SaturatedAdmission start() throws InterruptedException {
      MetadataIoRunner runner = new MetadataIoRunner(1);
      MetadataResourceReader reader = new MetadataResourceReader(runner);
      CountDownLatch holderEntered = new CountDownLatch(1);
      CountDownLatch releaseHolder = new CountDownLatch(1);
      CompletableFuture<Void> holder =
          CompletableFuture.runAsync(
              () ->
                  reader.read(
                      () -> {
                        holderEntered.countDown();
                        awaitUninterruptibly(releaseHolder);
                        return null;
                      }));
      assertThat(holderEntered.await(2, TimeUnit.SECONDS)).isTrue();
      return new SaturatedAdmission(runner, reader, releaseHolder, holder);
    }

    @Override
    public void close() throws InterruptedException {
      releaseHolder.countDown();
      holder.join();
      await(() -> runner.permitsInUse() == 0);
    }
  }
}
