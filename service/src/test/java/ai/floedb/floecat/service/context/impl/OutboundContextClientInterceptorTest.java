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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Empty;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.stub.MetadataUtils;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class OutboundContextClientInterceptorTest {

  private static final MethodDescriptor<Empty, Empty> METHOD =
      MethodDescriptor.<Empty, Empty>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(MethodDescriptor.generateFullMethodName("test", "Method"))
          .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(Empty.getDefaultInstance()))
          .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(Empty.getDefaultInstance()))
          .build();

  /** Ensures the client interceptor copies x-engine-version onto outbound calls. */
  @Test
  void injectsEngineVersionHeader() {
    var interceptor = new OutboundContextClientInterceptor();
    var captured = new AtomicReference<Metadata>();

    Channel baseChannel =
        new Channel() {
          @Override
          public String authority() {
            return "test";
          }

          @Override
          public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
              MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
            return new ClientCall<ReqT, RespT>() {
              @Override
              public void start(Listener<RespT> responseListener, Metadata headers) {
                captured.set(headers);
                responseListener.onClose(Status.OK, new Metadata());
              }

              @Override
              public void request(int numMessages) {}

              @Override
              public void cancel(String message, Throwable cause) {}

              @Override
              public void halfClose() {}

              @Override
              public void sendMessage(ReqT message) {}
            };
          }
        };

    var ctx =
        io.grpc.Context.current().withValue(InboundContextInterceptor.ENGINE_VERSION_KEY, "16.0");

    Channel intercepted =
        ClientInterceptors.intercept(
            baseChannel, MetadataUtils.newAttachHeadersInterceptor(new Metadata()), interceptor);

    ctx.run(
        () -> {
          ClientCall<Empty, Empty> call = intercepted.newCall(METHOD, CallOptions.DEFAULT);
          call.start(new ClientCall.Listener<>() {}, new Metadata());
        });

    assertThat(captured.get()).isNotNull();
    Metadata.Key<String> key =
        Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);
    assertThat(captured.get().get(key)).isEqualTo("16.0");
  }
}
