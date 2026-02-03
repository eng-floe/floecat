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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class OutboundContextClientInterceptorTest {

  private static final Metadata.Key<String> ENGINE_KIND_KEY =
      Metadata.Key.of("x-engine-kind", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ENGINE_VERSION_KEY =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);

  @Test
  void doesNotEmitEmptyEngineHeadersWhenContextMissing() {
    OutboundContextClientInterceptor interceptor =
        new OutboundContextClientInterceptor(Optional.empty(), Optional.empty());
    AtomicReference<Metadata> captured = new AtomicReference<>();

    MethodDescriptor<String, String> method =
        MethodDescriptor.<String, String>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("test/empty")
            .setRequestMarshaller(new StringMarshaller())
            .setResponseMarshaller(new StringMarshaller())
            .build();

    ClientCall<String, String> call =
        interceptor.interceptCall(method, CallOptions.DEFAULT, new TestChannel(captured));
    call.start(new ClientCall.Listener<>() {}, new Metadata());

    Metadata headers = captured.get();
    assertThat(headers.get(ENGINE_KIND_KEY)).isNull();
    assertThat(headers.get(ENGINE_VERSION_KEY)).isNull();
  }

  private static final class TestChannel extends Channel {
    private final AtomicReference<Metadata> captured;

    private TestChannel(AtomicReference<Metadata> captured) {
      this.captured = captured;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
      return new ClientCall<>() {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          captured.set(headers);
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

    @Override
    public String authority() {
      return "test";
    }
  }

  private static final class StringMarshaller implements MethodDescriptor.Marshaller<String> {
    @Override
    public InputStream stream(String value) {
      return new ByteArrayInputStream(
          value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0]);
    }

    @Override
    public String parse(InputStream stream) {
      try (stream) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        stream.transferTo(buffer);
        return buffer.toString(StandardCharsets.UTF_8);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
