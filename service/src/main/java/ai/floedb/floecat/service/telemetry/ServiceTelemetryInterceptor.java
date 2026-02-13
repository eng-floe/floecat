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

package ai.floedb.floecat.service.telemetry;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.grpc.GrpcTelemetryServerInterceptor;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.opentelemetry.api.trace.Span;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Objects;
import org.jboss.logging.MDC;

/** Service-specific gRPC interceptor that publishes RPC metrics through the telemetry hub. */
@ApplicationScoped
@GlobalInterceptor
@Priority(2)
public final class ServiceTelemetryInterceptor implements ServerInterceptor {
  private final GrpcTelemetryServerInterceptor delegate;

  @Inject
  public ServiceTelemetryInterceptor(Observability observability) {
    this.delegate =
        new GrpcTelemetryServerInterceptor(
            Objects.requireNonNull(observability, "observability"),
            "service",
            (__call, __headers) -> {
              PrincipalContext pc = InboundContextInterceptor.PC_KEY.get();
              return pc == null ? null : pc.getAccountId();
            });
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    String operation = GrpcTelemetryServerInterceptor.simplifyOp(call.getMethodDescriptor());
    Span span = Span.current();
    if (span.getSpanContext().isValid()) {
      String queryId = InboundContextInterceptor.QUERY_KEY.get();
      if (queryId != null && !queryId.isBlank()) {
        span.setAttribute("query_id", queryId);
      }
      String correlationId = InboundContextInterceptor.CORR_KEY.get();
      if (correlationId != null && !correlationId.isBlank()) {
        span.setAttribute("correlation_id", correlationId);
      }
      PrincipalContext principalContext = InboundContextInterceptor.PC_KEY.get();
      if (principalContext != null) {
        span.setAttribute("floecat_account_id", principalContext.getAccountId());
        span.setAttribute("floecat_subject", principalContext.getSubject());
      }
      String engineVersion = InboundContextInterceptor.ENGINE_VERSION_KEY.get();
      if (engineVersion != null && !engineVersion.isBlank()) {
        span.setAttribute("floecat_engine_version", engineVersion);
      }
      String engineKind = InboundContextInterceptor.ENGINE_KIND_KEY.get();
      if (engineKind != null && !engineKind.isBlank()) {
        span.setAttribute("floecat_engine_kind", engineKind);
      }
    }
    MDC.put("floecat_component", "service");
    MDC.put("floecat_operation", operation);
    ServerCall<ReqT, RespT> wrapped =
        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
          @Override
          public void close(Status status, Metadata trailers) {
            try {
              MDC.remove("floecat_component");
              MDC.remove("floecat_operation");
            } finally {
              super.close(status, trailers);
            }
          }
        };
    try {
      return delegate.interceptCall(wrapped, headers, next);
    } catch (RuntimeException e) {
      MDC.remove("floecat_component");
      MDC.remove("floecat_operation");
      throw e;
    }
  }
}
