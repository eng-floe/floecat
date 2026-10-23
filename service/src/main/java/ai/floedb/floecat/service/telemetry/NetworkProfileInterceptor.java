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

/** Records serialized protobuf payload sizes for Floecat's inbound and outbound gRPC traffic. */
@ApplicationScoped
@GlobalInterceptor
public final class NetworkProfileInterceptor implements ServerInterceptor, Prioritized {
  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    ServerCall<ReqT, RespT> measuredCall =
        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
          @Override
          public void sendMessage(RespT message) {
            NetworkProfileExporter.recordOutbound(serializedSize(message));
            super.sendMessage(message);
          }
        };
    ServerCall.Listener<ReqT> listener = next.startCall(measuredCall, headers);
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
      @Override
      public void onMessage(ReqT message) {
        NetworkProfileExporter.recordInbound(serializedSize(message));
        super.onMessage(message);
      }
    };
  }

  private static long serializedSize(Object message) {
    return message instanceof MessageLite protobuf ? protobuf.getSerializedSize() : 0;
  }

  @Override
  public int getPriority() {
    return -250;
  }
}
