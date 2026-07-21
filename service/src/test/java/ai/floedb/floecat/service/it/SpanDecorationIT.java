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

package ai.floedb.floecat.service.it;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.GetUserObjectsRequest;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.query.rpc.UserObjectsServiceGrpc;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Build-time guard for the interceptor-ordering premise the span decorations rest on.
 *
 * <p>Quarkus's {@code GrpcTracingServerInterceptor} carries no {@code Prioritized} priority and
 * sorts to 0; floecat's {@code SpanCaptureInterceptor} and {@code ServiceTelemetryInterceptor} sit
 * at negative priorities so their chain builds run INSIDE its {@code makeCurrent()} window — the
 * only place the server span is ever current. Unit tests cannot validate that: the production
 * {@code QuarkusContextStorage} and the real interceptor sort only exist in a booted Quarkus. This
 * IT boots one, issues one real RPC, and asserts the decorations that ONLY land when the ordering
 * holds:
 *
 * <ul>
 *   <li>{@code correlation_id} — written by {@code SpanCaptureInterceptor} inside the window;
 *   <li>{@code floecat.component} / {@code floecat.rpc.status} — written by the telemetry
 *       delegate's in-window span capture and its close-time finish;
 *   <li>the {@code floecat.rpc.summary} event — emitted only when the diagnostics factory saw a
 *       valid recording span at interceptCall.
 * </ul>
 *
 * <p>If a Quarkus/OTel bump ever changes the tracing interceptor's effective priority, this test
 * fails at build time instead of the decorations silently no-op'ing in production (where the only
 * signal would be {@code SpanCaptureInterceptor}'s WARN).
 */
@QuarkusTest
@TestProfile(SpanDecorationIT.InMemoryTracingProfile.class)
class SpanDecorationIT {

  /** Re-enables span export (test defaults disable it) and drains batches fast. */
  public static class InMemoryTracingProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "quarkus.otel.traces.exporter", "cdi",
          "quarkus.otel.exporter.otlp.enabled", "false",
          "quarkus.otel.bsp.schedule.delay", "100ms");
    }
  }

  /**
   * CDI-published in-memory exporter, picked up because the profile sets the traces exporter to
   * {@code cdi}. Under every other test profile the exporter is {@code none}, so this bean is inert
   * there. Static so the test reads finished spans without injecting SDK types.
   */
  @ApplicationScoped
  public static class InMemoryExporterProducer {
    static final InMemorySpanExporter EXPORTER = InMemorySpanExporter.create();

    @Produces
    @Singleton
    SpanExporter exporter() {
      return EXPORTER;
    }
  }

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub lifecycle;

  @GrpcClient("floecat")
  Channel channel;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @Test
  void serverSpanCarriesFloecatDecorations() {
    InMemoryExporterProducer.EXPORTER.reset();

    catalog.listCatalogs(ListCatalogsRequest.newBuilder().build());

    SpanData span =
        awaitServerSpan("ListCatalogs")
            .orElseThrow(
                () ->
                    new AssertionError(
                        "no SERVER span for ListCatalogs was exported — tracing itself is broken"
                            + " in this profile"));

    // SpanCaptureInterceptor, inside the tracing window: identity attributes on the REAL span.
    assertThat(span.getAttributes().get(AttributeKey.stringKey("correlation_id")))
        .as("correlation_id — SpanCaptureInterceptor must capture inside the tracing window")
        .isNotBlank();

    // The telemetry delegate's in-window capture: component tag at interceptCall, status at
    // finish. Both read the span reference captured at interceptCall time.
    assertThat(span.getAttributes().get(AttributeKey.stringKey("floecat.component")))
        .as("floecat.component — ServiceTelemetryInterceptor must run inside the tracing window")
        .isEqualTo("service");
    assertThat(span.getAttributes().get(AttributeKey.stringKey("floecat.rpc.status")))
        .as("floecat.rpc.status — the finish() path must hold the captured span")
        .isEqualTo("OK");

    // The diagnostics factory returns NOOP unless a valid recording span was current at
    // interceptCall; the summary event only exists when it was not NOOP.
    assertThat(span.getEvents())
        .as("floecat.rpc.summary event — PhaseDiagnostics must not be NOOP")
        .anyMatch(event -> "floecat.rpc.summary".equals(event.getName()));
  }

  @Test
  void streamingServerSpanCarriesBodySetDecorations() {
    // Closes the loop the unary test cannot: the unary decorations all land inside the tracing
    // window, so they would pass even if the duplicated-context carrier were broken. This drives a
    // STREAMING RPC (getUserObjects) whose handler body runs on a hopped worker thread OUTSIDE the
    // window; the floecat.get_user_objects.* attributes are written on Span.current() captured
    // inside that body. They only reach the real RPC span if storeSpanOnDuplicatedContext ->
    // otelContextForBody grafted the captured span onto the body's context — the mechanism this
    // whole change exists to enable. An unresolved candidate is enough: the body's summary sets
    // the outcome attribute regardless of whether the table resolves.
    resetter.wipeAll();
    seeder.seedData();
    InMemoryExporterProducer.EXPORTER.reset();

    var cat = TestSupport.createCatalog(catalog, "span_it_cat", "");
    var begin =
        lifecycle.beginQuery(
            BeginQueryRequest.newBuilder().setDefaultCatalogId(cat.getResourceId()).build());
    var candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(
                QueryInput.newBuilder()
                    .setName(TestSupport.fq("span_it_cat", List.of("no_such_ns"), "no_such_table"))
                    .build())
            .build();

    collectUserObjectBundle(
        GetUserObjectsRequest.newBuilder()
            .setQueryId(begin.getQuery().getQueryId())
            .addTables(candidate)
            .build());

    SpanData span =
        awaitServerSpan("GetUserObjects")
            .orElseThrow(
                () -> new AssertionError("no SERVER span for GetUserObjects was exported"));

    // Written on Span.current() inside the streaming body — present only if the carrier grafted
    // the captured span onto the hopped body's OTel context.
    assertThat(span.getAttributes().get(AttributeKey.stringKey("floecat.get_user_objects.outcome")))
        .as(
            "floecat.get_user_objects.outcome — the carrier-to-body graft must make the RPC span"
                + " current inside the streaming handler body")
        .isNotBlank();
  }

  /** Drives the streaming getUserObjects RPC and waits for the stream to terminate. */
  private void collectUserObjectBundle(GetUserObjectsRequest request) {
    UserObjectsServiceGrpc.UserObjectsServiceStub async =
        UserObjectsServiceGrpc.newStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
    CompletableFuture<List<UserObjectsBundleChunk>> future = new CompletableFuture<>();
    List<UserObjectsBundleChunk> chunks = Collections.synchronizedList(new ArrayList<>());
    async.getUserObjects(
        request,
        new StreamObserver<>() {
          @Override
          public void onNext(UserObjectsBundleChunk chunk) {
            chunks.add(chunk);
          }

          @Override
          public void onError(Throwable t) {
            // A not-found candidate may terminate the stream in error; the span is still emitted
            // with the body-set outcome, which is what this test asserts.
            future.complete(new ArrayList<>(chunks));
          }

          @Override
          public void onCompleted() {
            future.complete(new ArrayList<>(chunks));
          }
        });
    future.orTimeout(10, TimeUnit.SECONDS).join();
  }

  /** Polls the exporter for the finished SERVER span of the given method (batch delay ~100ms). */
  private static Optional<SpanData> awaitServerSpan(String methodPart) {
    Instant deadline = Instant.now().plus(Duration.ofSeconds(15));
    while (Instant.now().isBefore(deadline)) {
      Optional<SpanData> match =
          InMemoryExporterProducer.EXPORTER.getFinishedSpanItems().stream()
              .filter(span -> span.getKind() == io.opentelemetry.api.trace.SpanKind.SERVER)
              .filter(span -> span.getName().contains(methodPart))
              .findFirst();
      if (match.isPresent()) {
        return match;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return Optional.empty();
      }
    }
    return Optional.empty();
  }
}
