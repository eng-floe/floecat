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

package ai.floedb.floecat.service.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.service.account.LifecycleControl;
import ai.floedb.floecat.service.account.LifecycleDrain;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BaseServiceImplTest {
  private static final String CORRELATION_ID = "corr-test";

  @Test
  void toStatusPreservesOriginalCanonicalCode() {
    TestServiceImpl service = new TestServiceImpl();
    StatusRuntimeException original = io.grpc.Status.NOT_FOUND.asRuntimeException();
    StatusRuntimeException repacked = service.repack(original, CORRELATION_ID);

    Status statusProto = StatusProto.fromThrowable(repacked);
    assertEquals(io.grpc.Status.Code.NOT_FOUND.value(), statusProto.getCode());
    Error err =
        statusProto.getDetailsList().stream()
            .filter(any -> any.is(Error.class))
            .findFirst()
            .map(
                any -> {
                  try {
                    return any.unpack(Error.class);
                  } catch (InvalidProtocolBufferException e) {
                    throw new AssertionError("failed to unpack Error detail", e);
                  }
                })
            .orElseThrow();
    assertEquals(CORRELATION_ID, err.getCorrelationId());
    assertEquals(ErrorCode.MC_NOT_FOUND, err.getCode());
  }

  @Test
  void runExecutesSupplierOffEventLoop() throws Exception {
    TestServiceImpl service = new TestServiceImpl();
    Vertx vertx = Vertx.vertx();
    try {
      CompletableFuture<Boolean> bodyRanOnEventLoop = new CompletableFuture<>();
      vertx.runOnContext(
          ignored ->
              service
                  .bodyRunsOnEventLoop()
                  .subscribe()
                  .with(bodyRanOnEventLoop::complete, bodyRanOnEventLoop::completeExceptionally));

      assertFalse(bodyRanOnEventLoop.get(5, TimeUnit.SECONDS));
    } finally {
      vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void runWithRetryDoesNotRetryAnActiveIdempotencyClaim() {
    TestServiceImpl service = new TestServiceImpl();
    AtomicInteger attempts = new AtomicInteger();

    Throwable failure =
        org.junit.jupiter.api.Assertions.assertThrows(
            IdempotencyInProgressException.class,
            () ->
                service
                    .withRetry(
                        () -> {
                          attempts.incrementAndGet();
                          throw new IdempotencyInProgressException("pending");
                        })
                    .await()
                    .indefinitely());

    assertEquals("pending", failure.getMessage());
    assertEquals(1, attempts.get());
  }

  @Test
  void runWithRetryRetriesTypedTransientConflicts() {
    TestServiceImpl service = new TestServiceImpl();
    AtomicInteger attempts = new AtomicInteger();

    String result =
        service
            .withRetry(
                () -> {
                  if (attempts.getAndIncrement() == 0) {
                    throw new BaseResourceRepository.AbortRetryableException("pointer conflict");
                  }
                  return "ok";
                })
            .await()
            .indefinitely();

    assertEquals("ok", result);
    assertEquals(2, attempts.get());
  }

  @Test
  void runWithRetryDoesNotRetryAnAbortedStatus() {
    TestServiceImpl service = new TestServiceImpl();
    AtomicInteger attempts = new AtomicInteger();

    org.junit.jupiter.api.Assertions.assertThrows(
        StatusRuntimeException.class,
        () ->
            service
                .withRetry(
                    () -> {
                      attempts.incrementAndGet();
                      throw io.grpc.Status.ABORTED.asRuntimeException();
                    })
                .await()
                .indefinitely());

    assertEquals(1, attempts.get());
  }

  @Test
  void rpcAdmissionStartsAtSubscriptionAndClosesAtTermination() {
    TestDrain drain = new TestDrain();
    TestServiceImpl service = new TestServiceImpl(drain);

    Uni<String> operation = service.admitted(() -> "ok");
    assertEquals(0, drain.active.get());
    assertEquals("ok", operation.await().indefinitely());
    assertEquals(0, drain.active.get());
  }

  @Test
  void drainingAdmissionIsMappedToUnavailableInsideTheUni() {
    TestDrain drain = new TestDrain();
    drain.draining.set(true);
    TestServiceImpl service = new TestServiceImpl(drain);

    Uni<String> operation = service.mappedAdmission(() -> "never", CORRELATION_ID);
    StatusRuntimeException failure =
        assertThrows(StatusRuntimeException.class, () -> operation.await().indefinitely());

    assertEquals(io.grpc.Status.Code.UNAVAILABLE, failure.getStatus().getCode());
    assertEquals(0, drain.active.get());
  }

  @Test
  void drainingAdmissionIsMappedToUnavailableForStreams() {
    TestDrain drain = new TestDrain();
    drain.draining.set(true);
    TestServiceImpl service = new TestServiceImpl(drain);

    StatusRuntimeException streamFailure =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.admittedStream().collect().asList().await().indefinitely());
    StatusRuntimeException emitterFailure =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.admittedEmitterStream().collect().asList().await().indefinitely());

    assertEquals(io.grpc.Status.Code.UNAVAILABLE, streamFailure.getStatus().getCode());
    assertEquals(io.grpc.Status.Code.UNAVAILABLE, emitterFailure.getStatus().getCode());
    assertEquals(0, drain.active.get());
  }

  @Test
  void inProgressMapsToAbortedAtTheRpcBoundary() {
    TestServiceImpl service = new TestServiceImpl();

    StatusRuntimeException mapped =
        service.map(new IdempotencyInProgressException("pending"), CORRELATION_ID);

    assertEquals(io.grpc.Status.Code.ABORTED, mapped.getStatus().getCode());
  }

  private static final class TestServiceImpl extends BaseServiceImpl {
    private TestServiceImpl() {}

    private TestServiceImpl(LifecycleDrain drain) {
      lifecycleDrain = drain;
    }

    StatusRuntimeException repack(StatusRuntimeException ex, String corrId) {
      return toStatus(ex, corrId);
    }

    StatusRuntimeException map(Throwable failure, String corrId) {
      return toStatus(failure, corrId);
    }

    Uni<Boolean> bodyRunsOnEventLoop() {
      return run(Context::isOnEventLoopThread);
    }

    <T> Uni<T> withRetry(java.util.function.Supplier<T> supplier) {
      return runWithRetry(supplier);
    }

    <T> Uni<T> admitted(java.util.function.Supplier<T> supplier) {
      return run(supplier);
    }

    <T> Uni<T> mappedAdmission(java.util.function.Supplier<T> supplier, String corrId) {
      return mapFailures(run(supplier), corrId);
    }

    Multi<String> admittedStream() {
      return runStream(callCtx -> Multi.createFrom().item("ok"));
    }

    Multi<String> admittedEmitterStream() {
      return runStreamEmitter((callCtx, emitter) -> emitter.complete());
    }
  }

  private static final class TestDrain implements LifecycleDrain {
    private final AtomicBoolean draining = new AtomicBoolean();
    private final AtomicInteger active = new AtomicInteger();

    @Override
    public Permit admitRpc() {
      if (draining.get()) {
        throw new DrainingException();
      }
      active.incrementAndGet();
      return () -> active.decrementAndGet();
    }

    @Override
    public LifecycleControl.Status beginProcessDrain() {
      draining.set(true);
      return status();
    }

    @Override
    public LifecycleControl.Status status() {
      return new LifecycleControl.Status(
          "test", "test", draining.get(), java.util.List.of(), active.get());
    }
  }
}
