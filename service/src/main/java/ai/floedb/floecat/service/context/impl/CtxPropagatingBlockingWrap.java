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

package ai.floedb.floecat.service.context.impl;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.quarkus.arc.Arc;
import io.quarkus.arc.InjectableContext;
import io.quarkus.arc.ManagedContext;
import io.vertx.core.Vertx;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.jboss.logging.Logger;

/**
 * Worker-thread {@link ServerInterceptor} wrap that preserves both the gRPC {@link Context} and the
 * CDI request context across the worker hop.
 *
 * <p>The pattern mirrors {@code
 * io.quarkus.grpc.runtime.supports.blocking.BlockingServerInterceptor} + {@code
 * BlockingExecutionHandler}: the chain build is dispatched via {@code executeBlocking}, and each
 * subsequent listener event is re-dispatched via {@code executeBlocking} with the gRPC {@link
 * Context} captured at scheduling time and re-attached on the worker. Unlike Quarkus's class (which
 * only activates for {@code @Blocking}-annotated methods), this wrap applies to every method.
 *
 * <p>Replaces the deprecated {@code io.vertx.grpc.BlockingServerInterceptor.wrap}, which does
 * <i>not</i> propagate {@code io.grpc.Context} across its buffered-event replay. Combined with
 * Quarkus's {@code GrpcDuplicatedContextGrpcInterceptor} hopping via {@code runOnContext} on
 * Vert.x-context mismatch, the deprecated wrap drops the principal/correlation context on a subset
 * of inbound calls.
 *
 * <p>Package-private so the test suite can instantiate it directly without going through the full
 * Quarkus interceptor chain.
 */
final class CtxPropagatingBlockingWrap implements ServerInterceptor {
  private static final Logger LOG = Logger.getLogger(CtxPropagatingBlockingWrap.class);

  private final Vertx vertx;
  private final ServerInterceptor inner;

  CtxPropagatingBlockingWrap(Vertx vertx, ServerInterceptor inner) {
    this.vertx = vertx;
    this.inner = inner;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    // Capture the active CDI request context AND its state on the caller thread (event loop).
    // The ManagedContext is a global accessor; the ContextState is what's thread-local — we
    // re-activate the captured state inside the worker.
    final ManagedContext requestContext = requestContextOrNull();
    final InjectableContext.ContextState state =
        requestContext == null ? null : safeGetState(requestContext);

    ReplayListener<ReqT> replay = new ReplayListener<>(vertx, requestContext, state);
    vertx
        .<ServerCall.Listener<ReqT>>executeBlocking(
            () -> {
              boolean activated = false;
              if (requestContext != null && state != null) {
                try {
                  requestContext.activate(state);
                  activated = true;
                } catch (RuntimeException e) {
                  LOG.debug("Could not re-activate CDI request context on worker", e);
                }
              }
              try {
                return inner.interceptCall(call, headers, next);
              } finally {
                if (activated) {
                  try {
                    requestContext.deactivate();
                  } catch (RuntimeException ignored) {
                    // ignore
                  }
                }
              }
            },
            false)
        .onComplete(
            ar -> {
              if (ar.succeeded()) {
                replay.setDelegate(ar.result());
              } else {
                Metadata md = Status.trailersFromThrowable(ar.cause());
                if (md == null) {
                  md = new Metadata();
                }
                call.close(Status.fromThrowable(ar.cause()), md);
              }
            });
    return replay;
  }

  private static ManagedContext requestContextOrNull() {
    try {
      return Arc.container() == null ? null : Arc.container().requestContext();
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static InjectableContext.ContextState safeGetState(ManagedContext rc) {
    try {
      return rc.isActive() ? rc.getState() : null;
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Buffers inbound events until the delegate listener (built on a worker) is available, then
   * re-dispatches each event to a worker with the gRPC and CDI request contexts re-attached. Events
   * are processed in order: one worker task at a time, chained via {@code onComplete}.
   */
  private static final class ReplayListener<ReqT> extends ServerCall.Listener<ReqT> {
    private final Vertx vertx;
    private final ManagedContext requestContext;
    private final InjectableContext.ContextState requestContextState;
    private volatile ServerCall.Listener<ReqT> delegate;
    private final Queue<Consumer<ServerCall.Listener<ReqT>>> incomingEvents =
        new ConcurrentLinkedQueue<>();
    private final Object drainLock = new Object();
    private boolean isConsumingFromIncomingEvents;

    ReplayListener(
        Vertx vertx,
        ManagedContext requestContext,
        InjectableContext.ContextState requestContextState) {
      this.vertx = vertx;
      this.requestContext = requestContext;
      this.requestContextState = requestContextState;
    }

    void setDelegate(ServerCall.Listener<ReqT> delegate) {
      synchronized (drainLock) {
        this.delegate = delegate;
      }
      startDrainIfPossible();
    }

    private void scheduleOrEnqueue(Consumer<ServerCall.Listener<ReqT>> consumer) {
      // Capture the gRPC Context at event-arrival time (not at execute time) so it survives the
      // buffer/replay path even when the worker that drains the queue has a different (or no)
      // io.grpc.Context attached. The wrapper re-attaches `arrivalCtx` around the actual
      // consumer call on the worker.
      final Context arrivalCtx = Context.current();
      Consumer<ServerCall.Listener<ReqT>> withCtx =
          listener -> {
            Context prev = arrivalCtx.attach();
            try {
              consumer.accept(listener);
            } finally {
              arrivalCtx.detach(prev);
            }
          };
      synchronized (drainLock) {
        incomingEvents.add(withCtx);
      }
      startDrainIfPossible();
    }

    private void startDrainIfPossible() {
      Consumer<ServerCall.Listener<ReqT>> next;
      synchronized (drainLock) {
        if (delegate == null || isConsumingFromIncomingEvents) {
          return;
        }
        next = incomingEvents.poll();
        if (next == null) {
          return;
        }
        isConsumingFromIncomingEvents = true;
      }
      executeBlockingWithContext(next);
    }

    /**
     * Dispatch one event to a worker. The gRPC {@link Context} was already captured in {@link
     * #scheduleOrEnqueue} so the consumer is responsible for attaching/detaching it; here we only
     * re-activate the CDI request context state and chain the next queued event via {@code
     * onComplete}.
     */
    private void executeBlockingWithContext(Consumer<ServerCall.Listener<ReqT>> consumer) {
      final ServerCall.Listener<ReqT> target = delegate;
      vertx
          .<Void>executeBlocking(
              () -> {
                boolean activated = false;
                if (requestContext != null && requestContextState != null) {
                  try {
                    requestContext.activate(requestContextState);
                    activated = true;
                  } catch (RuntimeException ignored) {
                    // ignore
                  }
                }
                try {
                  consumer.accept(target);
                } finally {
                  if (activated) {
                    try {
                      requestContext.deactivate();
                    } catch (RuntimeException ignored) {
                      // ignore
                    }
                  }
                }
                return null;
              },
              false)
          .onComplete(
              p -> {
                Consumer<ServerCall.Listener<ReqT>> nextEvent;
                synchronized (drainLock) {
                  nextEvent = incomingEvents.poll();
                  if (nextEvent == null) {
                    isConsumingFromIncomingEvents = false;
                    return;
                  }
                }
                executeBlockingWithContext(nextEvent);
              });
    }

    @Override
    public void onMessage(ReqT message) {
      scheduleOrEnqueue(t -> t.onMessage(message));
    }

    @Override
    public void onHalfClose() {
      scheduleOrEnqueue(ServerCall.Listener::onHalfClose);
    }

    @Override
    public void onCancel() {
      scheduleOrEnqueue(ServerCall.Listener::onCancel);
    }

    @Override
    public void onComplete() {
      scheduleOrEnqueue(ServerCall.Listener::onComplete);
    }

    @Override
    public void onReady() {
      scheduleOrEnqueue(ServerCall.Listener::onReady);
    }
  }
}
