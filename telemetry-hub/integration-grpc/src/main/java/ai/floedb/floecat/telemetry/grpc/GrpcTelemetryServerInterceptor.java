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
package ai.floedb.floecat.telemetry.grpc;

import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import ai.floedb.floecat.telemetry.StoreOperationSummary;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.RpcMetrics;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** gRPC server interceptor that routes RPC metrics through the telemetry hub. */
public final class GrpcTelemetryServerInterceptor implements ServerInterceptor {
  public interface AccountResolver {
    String resolve(ServerCall<?, ?> call, Metadata headers);
  }

  private static final AccountResolver DEFAULT_ACCOUNT_RESOLVER = (call, headers) -> null;

  private final Observability observability;
  private final String component;
  private final AccountResolver accountResolver;
  private final ConcurrentMap<String, RpcMetrics> metricsByOperation = new ConcurrentHashMap<>();

  public GrpcTelemetryServerInterceptor(Observability observability, String component) {
    this(observability, component, DEFAULT_ACCOUNT_RESOLVER);
  }

  public GrpcTelemetryServerInterceptor(
      Observability observability, String component, AccountResolver accountResolver) {
    this.observability = Objects.requireNonNull(observability, "observability");
    this.component = Objects.requireNonNull(component, "component");
    this.accountResolver = Objects.requireNonNull(accountResolver, "accountResolver");
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    String operation = simplifyOp(call.getMethodDescriptor());
    StoreOperationSummary.reset();
    RpcMetrics rpcMetrics =
        metricsByOperation.computeIfAbsent(
            operation, op -> new RpcMetrics(observability, component, op));
    Span span = Span.current();
    if (span.getSpanContext().isValid()) {
      span.setAttribute("floecat.component", component);
      span.setAttribute("floecat.operation", operation);
    }
    String account = nullToDash(accountResolver.resolve(call, headers));
    long startedNanos = System.nanoTime();
    PhaseDiagnostics diagnostics =
        observability.diagnostics(component, operation, Tag.of(TagKey.ACCOUNT, account));
    Context storeSummaryContext =
        StoreOperationSummary.start(Context.current(), diagnostics != PhaseDiagnostics.NOOP);
    ObservationScope scope = rpcMetrics.observe(Tag.of(TagKey.ACCOUNT, account));
    ScopeAwareServerCall<ReqT, RespT> scopedCall =
        new ScopeAwareServerCall<>(
            call, scope, rpcMetrics, account, span, diagnostics, storeSummaryContext, startedNanos);
    try {
      ServerCall.Listener<ReqT> listener;
      try (Scope ignored = storeSummaryContext.makeCurrent()) {
        listener = next.startCall(scopedCall, headers);
      }
      return new ScopeAwareServerListener<>(listener, scopedCall);
    } catch (RuntimeException | Error e) {
      scopedCall.finish(Status.UNKNOWN.withCause(e), new Metadata());
      throw e;
    }
  }

  public static String simplifyOp(MethodDescriptor<?, ?> descriptor) {
    String full = Objects.requireNonNull(descriptor, "descriptor").getFullMethodName();
    if (full == null || full.isBlank()) {
      return "-";
    }
    int slash = full.indexOf('/');
    if (slash <= 0) {
      return full;
    }
    String svc = full.substring(0, slash);
    int lastDot = svc.lastIndexOf('.');
    if (lastDot >= 0) {
      svc = svc.substring(lastDot + 1);
    }
    String method = full.substring(slash + 1);
    return svc + "." + method;
  }

  private static String nullToDash(String value) {
    if (value == null || value.isBlank()) {
      return "-";
    }
    return value;
  }

  private final class ScopeAwareServerCall<ReqT, RespT>
      extends SimpleForwardingServerCall<ReqT, RespT> {
    private final ObservationScope scope;
    private final RpcMetrics rpcMetrics;
    private final String account;
    private final Span serverSpan;
    private final PhaseDiagnostics diagnostics;
    private final Context storeSummaryContext;
    private final long startedNanos;
    private final AtomicLong requestMessages = new AtomicLong();
    private final AtomicLong responseMessages = new AtomicLong();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicBoolean completed = new AtomicBoolean();
    private final AtomicBoolean finished = new AtomicBoolean();

    private ScopeAwareServerCall(
        ServerCall<ReqT, RespT> delegate,
        ObservationScope scope,
        RpcMetrics rpcMetrics,
        String account,
        Span serverSpan,
        PhaseDiagnostics diagnostics,
        Context storeSummaryContext,
        long startedNanos) {
      super(delegate);
      this.scope = Objects.requireNonNull(scope, "scope");
      this.rpcMetrics = Objects.requireNonNull(rpcMetrics, "rpcMetrics");
      this.account = account;
      this.serverSpan = serverSpan;
      this.diagnostics = Objects.requireNonNull(diagnostics, "diagnostics");
      this.storeSummaryContext = Objects.requireNonNull(storeSummaryContext, "storeSummaryContext");
      this.startedNanos = startedNanos;
    }

    @Override
    public void sendMessage(RespT message) {
      responseMessages.incrementAndGet();
      super.sendMessage(message);
    }

    @Override
    public void close(Status status, Metadata trailers) {
      Objects.requireNonNull(status, "status");
      finish(status, trailers);
      super.close(status, trailers);
    }

    private void requestMessage() {
      requestMessages.incrementAndGet();
    }

    private void cancelled() {
      cancelled.set(true);
      finish(Status.CANCELLED, new Metadata());
    }

    private void completed() {
      completed.set(true);
    }

    private void finish(Status status, Metadata trailers) {
      Objects.requireNonNull(status, "status");
      if (!finished.compareAndSet(false, true)) {
        return;
      }
      try (Scope ignored = storeSummaryContext.makeCurrent()) {
        String code = status.getCode().name();
        if (serverSpan != null && serverSpan.getSpanContext().isValid()) {
          serverSpan.setAttribute("floecat.rpc.status", code);
        }
        scope.status(code);
        if (status.isOk()) {
          scope.success();
        } else {
          scope.error(status.asRuntimeException(safeTrailers(trailers)));
        }
        diagnostics.put("status", code);
        diagnostics.put("success", status.isOk());
        diagnostics.put("cancelled", cancelled.get());
        diagnostics.put("completed", completed.get() || status.isOk());
        diagnostics.put("request_messages", requestMessages.get());
        diagnostics.put("response_messages", responseMessages.get());
        diagnostics.nanos("duration", System.nanoTime() - startedNanos);
        StoreOperationSummary.addTo(diagnostics);
        diagnostics.emit("floecat.rpc.summary");
      } finally {
        try {
          scope.close();
        } finally {
          rpcMetrics.recordRequest(account, status.getCode().name());
        }
      }
    }
  }

  private final class ScopeAwareServerListener<ReqT, RespT>
      extends SimpleForwardingServerCallListener<ReqT> {
    private final ScopeAwareServerCall<ReqT, RespT> call;

    private ScopeAwareServerListener(
        ServerCall.Listener<ReqT> delegate, ScopeAwareServerCall<ReqT, RespT> call) {
      super(delegate);
      this.call = Objects.requireNonNull(call, "call");
    }

    @Override
    public void onMessage(ReqT message) {
      try (Scope ignored = call.storeSummaryContext.makeCurrent()) {
        call.requestMessage();
        super.onMessage(message);
      }
    }

    @Override
    public void onHalfClose() {
      try (Scope ignored = call.storeSummaryContext.makeCurrent()) {
        super.onHalfClose();
      }
    }

    @Override
    public void onCancel() {
      try (Scope ignored = call.storeSummaryContext.makeCurrent()) {
        call.cancelled();
        super.onCancel();
      }
    }

    @Override
    public void onComplete() {
      try (Scope ignored = call.storeSummaryContext.makeCurrent()) {
        call.completed();
        super.onComplete();
      }
    }

    @Override
    public void onReady() {
      try (Scope ignored = call.storeSummaryContext.makeCurrent()) {
        super.onReady();
      }
    }
  }

  private static Metadata safeTrailers(Metadata trailers) {
    return trailers == null ? new Metadata() : trailers;
  }
}
