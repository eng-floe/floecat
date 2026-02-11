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
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.RpcMetrics;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
@GlobalInterceptor
@Priority(2)
public class MeteringInterceptor implements ServerInterceptor {

  @Inject Observability observability;

  private final Map<String, RpcMetrics> metricsByOperation = new ConcurrentHashMap<>();

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    final String operation = simplifyOp(call.getMethodDescriptor().getFullMethodName());
    final RpcMetrics rpcMetrics =
        metricsByOperation.computeIfAbsent(
            operation, op -> new RpcMetrics(observability, "service", op));

    ServerCall<ReqT, RespT> forwarding =
        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
          final RpcMetrics.ObservationScopeHolder scope;

          {
            final String account = resolveAccount();
            scope = rpcMetrics.observe(Tag.of(TagKey.ACCOUNT, account));
          }

          @Override
          public void close(Status status, Metadata trailers) {
            final String account = resolveAccount();
            final String statusStr = status.getCode().name();

            scope.status(statusStr);
            if (status.isOk()) {
              scope.success();
            } else {
              scope.error(status.asRuntimeException());
            }
            scope.close();

            rpcMetrics.recordRequest(account, statusStr);
            super.close(status, trailers);
          }

          private String resolveAccount() {
            final PrincipalContext pc = InboundContextInterceptor.PC_KEY.get();
            return nullToDash(pc == null ? null : pc.getAccountId());
          }
        };

    return next.startCall(forwarding, headers);
  }

  private static String simplifyOp(String fullMethod) {
    int slash = fullMethod.indexOf('/');
    if (slash <= 0) {
      return fullMethod;
    }

    String svc = fullMethod.substring(0, slash);
    int lastDot = svc.lastIndexOf('.');
    if (lastDot >= 0) {
      svc = svc.substring(lastDot + 1);
    }

    String method = fullMethod.substring(slash + 1);
    return svc + "." + method;
  }

  private static String nullToDash(String s) {
    return (s == null || s.isBlank()) ? "-" : s;
  }
}
