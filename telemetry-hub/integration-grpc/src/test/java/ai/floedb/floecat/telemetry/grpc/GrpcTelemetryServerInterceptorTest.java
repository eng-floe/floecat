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

import ai.floedb.floecat.telemetry.StoreOperationSummary;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TestObservability;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.Status;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class GrpcTelemetryServerInterceptorTest {
  @Test
  void recordsErrorMetrics() {
    TestObservability observability = new TestObservability();
    GrpcTelemetryServerInterceptor interceptor =
        new GrpcTelemetryServerInterceptor(observability, "svc");
    TestServerCall call = new TestServerCall("ai.floedb.Service/Method");
    AtomicReference<ServerCall<Void, Void>> activeCall = new AtomicReference<>();
    interceptor.interceptCall(
        call,
        new Metadata(),
        (c, headers) -> {
          activeCall.set(c);
          return new ServerCall.Listener<>() {};
        });
    activeCall.get().close(Status.INTERNAL, new Metadata());

    Assertions.assertThat(observability.counterValue(Telemetry.Metrics.RPC_REQUESTS)).isEqualTo(1);
    List<Tag> requestTags = observability.counterTagHistory(Telemetry.Metrics.RPC_REQUESTS).get(0);
    Assertions.assertThat(requestTags)
        .contains(
            Tag.of(TagKey.ACCOUNT, "-"), Tag.of(TagKey.STATUS, Status.INTERNAL.getCode().name()));
    Assertions.assertThat(requestTags)
        .contains(Tag.of(TagKey.COMPONENT, "svc"), Tag.of(TagKey.OPERATION, "Service.Method"));
    List<TestObservability.TestObservationScope> scopes = observability.scopes().get("RPC");
    Assertions.assertThat(scopes).hasSize(1);
    Assertions.assertThat(scopes.get(0).isSuccess()).isFalse();
    Assertions.assertThat(scopes.get(0).error()).isNotNull();
    Assertions.assertThat(scopes.get(0).tags())
        .contains(
            Tag.of(TagKey.COMPONENT, "svc"),
            Tag.of(TagKey.OPERATION, "Service.Method"),
            Tag.of(TagKey.ACCOUNT, "-"));
  }

  @Test
  void recordsSuccessMetrics() {
    TestObservability observability = new TestObservability();
    GrpcTelemetryServerInterceptor interceptor =
        new GrpcTelemetryServerInterceptor(observability, "svc");
    TestServerCall call = new TestServerCall("ai.floedb.Service/Method");
    AtomicReference<ServerCall<Void, Void>> activeCall = new AtomicReference<>();
    interceptor.interceptCall(
        call,
        new Metadata(),
        (c, headers) -> {
          activeCall.set(c);
          return new ServerCall.Listener<>() {};
        });
    activeCall.get().close(Status.OK, new Metadata());

    Assertions.assertThat(observability.counterValue(Telemetry.Metrics.RPC_REQUESTS)).isEqualTo(1);
    List<TestObservability.TestObservationScope> scopes = observability.scopes().get("RPC");
    Assertions.assertThat(scopes.get(0).isSuccess()).isTrue();
    Assertions.assertThat(observability.diagnosticEvents()).hasSize(1);
    TestObservability.TestDiagnosticEvent event = observability.diagnosticEvents().get(0);
    Assertions.assertThat(event.eventName()).isEqualTo("floecat.rpc.summary");
    Assertions.assertThat(event.fields())
        .containsEntry("success", true)
        .containsEntry("completed", true);
  }

  @Test
  void keepsGenericSummaryForDomainSummaryRpc() {
    TestObservability observability = new TestObservability();
    GrpcTelemetryServerInterceptor interceptor =
        new GrpcTelemetryServerInterceptor(observability, "svc");
    TestServerCall call =
        new TestServerCall("ai.floedb.floecat.query.UserObjectsService/GetUserObjects");
    AtomicReference<ServerCall<Void, Void>> activeCall = new AtomicReference<>();
    interceptor.interceptCall(
        call,
        new Metadata(),
        (c, headers) -> {
          activeCall.set(c);
          return new ServerCall.Listener<>() {};
        });
    activeCall.get().close(Status.OK, new Metadata());

    Assertions.assertThat(observability.counterValue(Telemetry.Metrics.RPC_REQUESTS)).isEqualTo(1);
    Assertions.assertThat(observability.diagnosticEvents()).hasSize(1);
    Assertions.assertThat(observability.diagnosticEvents().get(0).eventName())
        .isEqualTo("floecat.rpc.summary");
  }

  @Test
  void onCancelEmitsCancelledSummaryOnce() {
    TestObservability observability = new TestObservability();
    GrpcTelemetryServerInterceptor interceptor =
        new GrpcTelemetryServerInterceptor(observability, "svc");
    TestServerCall call = new TestServerCall("ai.floedb.Service/Method");
    ServerCall.Listener<Void> listener =
        interceptor.interceptCall(
            call,
            new Metadata(),
            (c, headers) ->
                new ServerCall.Listener<>() {
                  @Override
                  public void onCancel() {}
                });

    listener.onCancel();

    Assertions.assertThat(observability.diagnosticEvents()).hasSize(1);
    Assertions.assertThat(observability.diagnosticEvents().get(0).fields())
        .containsEntry("status", "CANCELLED")
        .containsEntry("success", false)
        .containsEntry("cancelled", true);
    Assertions.assertThat(observability.counterValue(Telemetry.Metrics.RPC_REQUESTS)).isEqualTo(1);
  }

  @Test
  void rpcSummaryIncludesGenericStoreAggregate() {
    TestObservability observability = new TestObservability();
    GrpcTelemetryServerInterceptor interceptor =
        new GrpcTelemetryServerInterceptor(observability, "svc");
    TestServerCall call = new TestServerCall("ai.floedb.Service/Method");
    AtomicReference<ServerCall<Void, Void>> activeCall = new AtomicReference<>();
    interceptor.interceptCall(
        call,
        new Metadata(),
        (c, headers) -> {
          activeCall.set(c);
          StoreOperationSummary.record(
              "repository", "table.get_by_key", Duration.ofMillis(2), true);
          StoreOperationSummary.put("current_snapshot_source", "property");
          return new ServerCall.Listener<>() {};
        });
    activeCall.get().close(Status.OK, new Metadata());

    Assertions.assertThat(observability.diagnosticEvents()).hasSize(1);
    Assertions.assertThat(observability.diagnosticEvents().get(0).fields())
        .containsEntry("store_operations", 1L)
        .containsEntry("store_errors", 0L)
        .containsEntry("repo_gets", 1L)
        .containsEntry("repo_lists", 0L)
        .containsEntry("current_snapshot_source", "property")
        .containsEntry("repo_table_get_by_key_count", 1L);
  }

  @Test
  void rpcSummaryIncludesStoreAggregateRecordedOnPropagatedWorkerThread() {
    TestObservability observability = new TestObservability();
    GrpcTelemetryServerInterceptor interceptor =
        new GrpcTelemetryServerInterceptor(observability, "svc");
    TestServerCall call = new TestServerCall("ai.floedb.Service/Method");
    AtomicReference<ServerCall<Void, Void>> activeCall = new AtomicReference<>();
    AtomicReference<Throwable> workerFailure = new AtomicReference<>();
    interceptor.interceptCall(
        call,
        new Metadata(),
        (c, headers) -> {
          activeCall.set(c);
          Context captured = Context.current();
          Thread worker =
              new Thread(
                  () -> {
                    try (Scope ignored = captured.makeCurrent()) {
                      StoreOperationSummary.record(
                          "repository", "table.get_by_key", Duration.ofMillis(2), true);
                      StoreOperationSummary.put("current_snapshot_source", "property");
                    } catch (Throwable t) {
                      workerFailure.set(t);
                    }
                  });
          worker.start();
          try {
            worker.join();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
          }
          return new ServerCall.Listener<>() {};
        });
    if (workerFailure.get() != null) {
      throw new AssertionError(workerFailure.get());
    }
    activeCall.get().close(Status.OK, new Metadata());

    Assertions.assertThat(observability.diagnosticEvents()).hasSize(1);
    Assertions.assertThat(observability.diagnosticEvents().get(0).fields())
        .containsEntry("store_operations", 1L)
        .containsEntry("repo_gets", 1L)
        .containsEntry("current_snapshot_source", "property")
        .containsEntry("repo_table_get_by_key_count", 1L);
  }

  private static final class TestServerCall extends ServerCall<Void, Void> {
    private final MethodDescriptor<Void, Void> descriptor;

    private TestServerCall(String fullMethodName) {
      this.descriptor =
          MethodDescriptor.<Void, Void>newBuilder()
              .setType(MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(fullMethodName)
              .setRequestMarshaller(new VoidMarshaller())
              .setResponseMarshaller(new VoidMarshaller())
              .build();
    }

    @Override
    public void request(int numMessages) {}

    @Override
    public void sendHeaders(Metadata headers) {}

    @Override
    public void sendMessage(Void message) {}

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
    public MethodDescriptor<Void, Void> getMethodDescriptor() {
      return descriptor;
    }
  }

  private static final class VoidMarshaller implements MethodDescriptor.Marshaller<Void> {
    private static final byte[] EMPTY = new byte[0];

    @Override
    public InputStream stream(Void value) {
      return new ByteArrayInputStream(EMPTY);
    }

    @Override
    public Void parse(InputStream stream) {
      return null;
    }
  }
}
