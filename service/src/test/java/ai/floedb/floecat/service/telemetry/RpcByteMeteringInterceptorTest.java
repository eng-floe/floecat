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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TestObservability;
import com.google.protobuf.StringValue;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RpcByteMeteringInterceptorTest {

  @Test
  void countsInboundAndOutboundBytesOncePerMessage() {
    TestObservability observability = new TestObservability();
    RpcByteMeteringInterceptor interceptor = new RpcByteMeteringInterceptor(observability);

    StringValue request = StringValue.of("request-payload");
    StringValue response = StringValue.of("response");

    TestServerCall call = new TestServerCall("ai.floedb.Service/Method");
    AtomicReference<ServerCall<StringValue, StringValue>> measured = new AtomicReference<>();
    ServerCall.Listener<StringValue> listener =
        interceptor.interceptCall(
            call,
            new Metadata(),
            (c, headers) -> {
              measured.set(c);
              return new ServerCall.Listener<>() {};
            });

    listener.onMessage(request);
    measured.get().sendMessage(response);

    // Each message is counted exactly once: one inbound emission, one outbound emission.
    List<List<Tag>> history = observability.counterTagHistory(Telemetry.Metrics.RPC_BYTES);
    assertThat(history).hasSize(2);
    assertThat(observability.counterValue(Telemetry.Metrics.RPC_BYTES))
        .isEqualTo((double) (request.getSerializedSize() + response.getSerializedSize()));

    assertThat(history.get(0))
        .contains(
            Tag.of(TagKey.COMPONENT, "service"),
            Tag.of(TagKey.OPERATION, "Service.Method"),
            Tag.of(TagKey.DIRECTION, "inbound"));
    assertThat(history.get(1))
        .contains(
            Tag.of(TagKey.COMPONENT, "service"),
            Tag.of(TagKey.OPERATION, "Service.Method"),
            Tag.of(TagKey.DIRECTION, "outbound"));
  }

  private static final class TestServerCall extends ServerCall<StringValue, StringValue> {
    private final MethodDescriptor<StringValue, StringValue> descriptor;

    private TestServerCall(String fullMethodName) {
      this.descriptor =
          MethodDescriptor.<StringValue, StringValue>newBuilder()
              .setType(MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(fullMethodName)
              .setRequestMarshaller(new StringValueMarshaller())
              .setResponseMarshaller(new StringValueMarshaller())
              .build();
    }

    @Override
    public void request(int numMessages) {}

    @Override
    public void sendHeaders(Metadata headers) {}

    @Override
    public void sendMessage(StringValue message) {}

    @Override
    public void close(Status status, Metadata trailers) {}

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public Attributes getAttributes() {
      return Attributes.EMPTY;
    }

    @Override
    public String getAuthority() {
      return "test";
    }

    @Override
    public MethodDescriptor<StringValue, StringValue> getMethodDescriptor() {
      return descriptor;
    }
  }

  private static final class StringValueMarshaller
      implements MethodDescriptor.Marshaller<StringValue> {
    @Override
    public InputStream stream(StringValue value) {
      return new ByteArrayInputStream(value.toByteArray());
    }

    @Override
    public StringValue parse(InputStream stream) {
      return StringValue.getDefaultInstance();
    }
  }
}
