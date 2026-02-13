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
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Objects;

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
    return delegate.interceptCall(call, headers, next);
  }
}
