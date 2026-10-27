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

import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.grpc.GrpcTelemetryServerInterceptor;
import ai.floedb.floecat.telemetry.helpers.RpcMetrics;
import com.google.protobuf.MessageLite;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.spi.Prioritized;
import jakarta.inject.Inject;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Records serialized protobuf payload bytes for Floecat's inbound and outbound gRPC traffic onto
 * the {@code floecat.core.rpc.bytes} counter, tagged by operation and {@code direction}. These
 * totals are RPC telemetry, not JVM profiles, so they live on the RPC metric alongside request
 * counts and latency — replacing the former pprof "network profile" upload pipeline and its fixed
 * window.
 */
@ApplicationScoped
@GlobalInterceptor
public final class RpcByteMeteringInterceptor implements ServerInterceptor, Prioritized {
  private static final String COMPONENT = "service";
  private static final String INBOUND = "inbound";
  private static final String OUTBOUND = "outbound";

  private final Observability observability;
  // Operation names are shared with GrpcTelemetryServerInterceptor via simplifyOp, so this cache
  // keys off the same canonical operation and reuses the existing RPC_ACTIVE gauge registration.
  private final ConcurrentMap<String, RpcMetrics> metricsByOperation = new ConcurrentHashMap<>();

  @Inject
  public RpcByteMeteringInterceptor(Observability observability) {
    this.observability = Objects.requireNonNull(observability, "observability");
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    String operation = GrpcTelemetryServerInterceptor.simplifyOp(call.getMethodDescriptor());
    RpcMetrics metrics =
        metricsByOperation.computeIfAbsent(
            operation, op -> new RpcMetrics(observability, COMPONENT, op));
    ServerCall<ReqT, RespT> measuredCall =
        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
          @Override
          public void sendMessage(RespT message) {
            metrics.recordBytes(OUTBOUND, serializedSize(message));
            super.sendMessage(message);
          }
        };
    ServerCall.Listener<ReqT> listener = next.startCall(measuredCall, headers);
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
      @Override
      public void onMessage(ReqT message) {
        metrics.recordBytes(INBOUND, serializedSize(message));
        super.onMessage(message);
      }
    };
  }

  private static long serializedSize(Object message) {
    return message instanceof MessageLite protobuf ? protobuf.getSerializedSize() : 0;
  }

  /**
   * Below 0 so it runs inside the tracing window. Byte accounting is order-independent (the
   * measured message is the same object regardless of interceptor order), so the exact value is
   * immaterial.
   */
  @Override
  public int getPriority() {
    return -250;
  }
}
