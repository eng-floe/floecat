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
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.RpcMetrics;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
    RpcMetrics rpcMetrics =
        metricsByOperation.computeIfAbsent(
            operation, op -> new RpcMetrics(observability, component, op));
    String account = nullToDash(accountResolver.resolve(call, headers));
    ObservationScope scope = rpcMetrics.observe(Tag.of(TagKey.ACCOUNT, account));
    ServerCall<ReqT, RespT> scopedCall =
        new ScopeAwareServerCall<>(call, scope, rpcMetrics, account);
    return next.startCall(scopedCall, headers);
  }

  private static String simplifyOp(MethodDescriptor<?, ?> descriptor) {
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

    private ScopeAwareServerCall(
        ServerCall<ReqT, RespT> delegate,
        ObservationScope scope,
        RpcMetrics rpcMetrics,
        String account) {
      super(delegate);
      this.scope = Objects.requireNonNull(scope, "scope");
      this.rpcMetrics = Objects.requireNonNull(rpcMetrics, "rpcMetrics");
      this.account = account;
    }

    @Override
    public void close(Status status, Metadata trailers) {
      Objects.requireNonNull(status, "status");
      try {
        String code = status.getCode().name();
        scope.status(code);
        if (status.isOk()) {
          scope.success();
        } else {
          scope.error(status.asRuntimeException(safeTrailers(trailers)));
        }
      } finally {
        try {
          scope.close();
        } finally {
          rpcMetrics.recordRequest(account, status.getCode().name());
          super.close(status, trailers);
        }
      }
    }
  }

  private static Metadata safeTrailers(Metadata trailers) {
    return trailers == null ? new Metadata() : trailers;
  }
}
