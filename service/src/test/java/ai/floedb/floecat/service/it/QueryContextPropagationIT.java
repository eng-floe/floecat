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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.CatalogSpec;
import ai.floedb.floecat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.GetUserObjectsRequest;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.RelationResolution;
import ai.floedb.floecat.query.rpc.ResolutionStatus;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.query.rpc.UserObjectsBundleChunk;
import ai.floedb.floecat.query.rpc.UserObjectsServiceGrpc;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.it.profiles.OidcSessionHeaderProfile;
import ai.floedb.floecat.service.it.util.TestKeyPair;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import io.grpc.Channel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.jwt.build.Jwt;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Stress reproducer for the intermittent loss of the inbound call context (principal, correlation
 * id, engine context) between the gRPC interceptor and service bodies running on Mutiny workers.
 *
 * <p>The context loss has two observable forms, both exercised here:
 *
 * <ul>
 *   <li><b>Loud form</b> — the principal is lost, {@code authz.require(pc, "catalog.read")} throws
 *       {@code PERMISSION_DENIED} (historical manifestation, before the principal gained its
 *       duplicated-context mirror).
 *   <li><b>Silent form</b> — the principal survives via its mirror but the engine context and
 *       correlation id (grpc-context-keys only) are lost. An engine-gated system object such as
 *       {@code sys.const} then resolves as {@code RESOLUTION_STATUS_NOT_FOUND} with a completed
 *       stream (wrong answer, no error), and error payloads built in the service body carry an
 *       empty correlation id. See eng-floe/floecat#361.
 * </ul>
 *
 * <p>Scenarios:
 *
 * <ul>
 *   <li>{@code beginQuery} on its own — the {@link
 *       ai.floedb.floecat.service.common.BaseServiceImpl#run BaseServiceImpl.run} path with a
 *       {@code Uni}.
 *   <li>{@code beginQuery + getUserObjects} per attempt — the {@code Multi.createFrom().emitter(
 *       ...).runSubscriptionOn(...)} path, with the in-body {@code grpcCtx.run(...)} that the
 *       handler uses for streaming. Each attempt resolves an engine-gated system table and fails on
 *       any non-FOUND resolution (silent-form detector).
 *   <li>{@code getUserObjects} against an unknown query id — the returned {@code NOT_FOUND} error
 *       payload must echo the request's correlation id, which the service body read from the
 *       fragile context channel (silent-form detector for the correlation channel).
 * </ul>
 *
 * <p>Tunable via {@code -Dfloecat.test.query-ctx.threads} / {@code .iterations}. Defaults are sized
 * to saturate the default Vert.x worker pool without making the test miserable to run on a laptop.
 */
@QuarkusTest
@TestProfile(OidcSessionHeaderProfile.class)
class QueryContextPropagationIT {

  private static final Metadata.Key<String> SESSION_HEADER =
      Metadata.Key.of("x-floe-session", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORRELATION_HEADER =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ENGINE_KIND_HEADER =
      Metadata.Key.of("x-engine-kind", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ENGINE_VERSION_HEADER =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);

  /**
   * Engine kind whose system catalog snapshot contains {@code sys.const} (provided by {@link
   * ai.floedb.floecat.service.catalog.it.TestCatalogExtension}). Resolution of that name is
   * engine-gated: it succeeds only while the request's engine context is visible to the resolving
   * thread, which is exactly the property under test. With a lost engine context the resolver falls
   * back to the default snapshot, which does not contain the table.
   */
  private static final String ENGINE_KIND = "test-engine";

  private static final int DEFAULT_THREADS = 16;
  private static final int DEFAULT_ITERATIONS_PER_THREAD = 25;
  private static final long TIMEOUT_SECONDS = 180;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;

  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub queries;

  // UserObjectsService is server-streaming; the Quarkus @GrpcClient injection of the blocking stub
  // for streaming RPCs is awkward, so build it from the raw Channel per call.
  @GrpcClient("floecat")
  Channel channel;

  @Inject AccountRepository accountRepository;
  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  private String accountId;
  private String jwt;
  private ResourceId catalogId;

  @BeforeEach
  void resetStores() throws Exception {
    resetter.wipeAll();
    seeder.seedData();
    accountId =
        accountRepository
            .getByName(TestSupport.DEFAULT_SEED_ACCOUNT)
            .orElseThrow()
            .getResourceId()
            .getId();
    jwt = initAccountJwt();

    // Pre-create a single catalog for all the BeginQuery calls to reference. Uses the same JWT
    // path so the setup itself exercises the same chain we're testing — if context propagation is
    // broken in setup, the test will fail loudly here rather than masking the problem.
    Metadata setupMetadata = metadataFor("ctx-prop-setup");
    var stub = catalogs.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(setupMetadata));
    catalogId =
        stub.createCatalog(
                CreateCatalogRequest.newBuilder()
                    .setSpec(
                        CatalogSpec.newBuilder().setDisplayName("ctxprop_setup_catalog").build())
                    .build())
            .getCatalog()
            .getResourceId();
  }

  @Test
  void concurrentBeginQueryDoesNotLosePrincipalContext() throws Exception {
    runStress(
        "beginQuery",
        (threadIdx, iteration) -> {
          String correlationId = "ctx-prop-bq-t" + threadIdx + "-i" + iteration;
          Metadata md = metadataFor(correlationId);
          var stub = queries.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
          stub.beginQuery(BeginQueryRequest.newBuilder().setDefaultCatalogId(catalogId).build());
        });
  }

  @Test
  void concurrentGetUserObjectsDoesNotLoseCallContext() throws Exception {
    // Silent-form detector: sys.const only resolves while the engine context declared in the
    // request headers is visible to the resolving thread. A NOT_FOUND here means the engine
    // context was lost mid-call even though the stream completed OK.
    TableReferenceCandidate systemTableCandidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(
                QueryInput.newBuilder()
                    .setName(NameRef.newBuilder().addPath("sys").setName("const"))
                    .build())
            .build();

    AtomicInteger silentNotFound = new AtomicInteger();
    AtomicReference<String> firstSilentSample = new AtomicReference<>();

    runStress(
        "getUserObjects",
        (threadIdx, iteration) -> {
          String correlationId = "ctx-prop-guo-t" + threadIdx + "-i" + iteration;
          Metadata md = metadataFor(correlationId);

          var queryStub = queries.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
          var begin =
              queryStub.beginQuery(
                  BeginQueryRequest.newBuilder().setDefaultCatalogId(catalogId).build());
          String queryId = begin.getQuery().getQueryId();

          var userObjectsStub =
              UserObjectsServiceGrpc.newBlockingStub(channel)
                  .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));

          Iterator<UserObjectsBundleChunk> it =
              userObjectsStub.getUserObjects(
                  GetUserObjectsRequest.newBuilder()
                      .setQueryId(queryId)
                      .addTables(systemTableCandidate)
                      .build());

          // Drain the stream so any PERMISSION_DENIED emitted by the server surfaces here, and
          // inspect every resolution: any non-FOUND outcome for the engine-gated system table is
          // the silent context-loss form.
          while (it.hasNext()) {
            UserObjectsBundleChunk chunk = it.next();
            if (!chunk.hasResolutions()) {
              continue;
            }
            for (RelationResolution resolution : chunk.getResolutions().getItemsList()) {
              if (resolution.getStatus() != ResolutionStatus.RESOLUTION_STATUS_FOUND) {
                silentNotFound.incrementAndGet();
                firstSilentSample.compareAndSet(
                    null,
                    "t="
                        + threadIdx
                        + " i="
                        + iteration
                        + " status="
                        + resolution.getStatus()
                        + " failure="
                        + resolution.getFailure().getMessage());
              }
            }
          }
        });

    assertEquals(
        0,
        silentNotFound.get(),
        () ->
            "Silent context-propagation failure: sys.const resolved as non-FOUND "
                + silentNotFound.get()
                + " time(s) despite x-engine-kind="
                + ENGINE_KIND
                + " on every call — the engine context was lost between the inbound interceptor"
                + " and engine-gated system-graph resolution. First sample: "
                + firstSilentSample.get());
  }

  @Test
  void getUserObjectsErrorCarriesRequestCorrelationId() throws Exception {
    // The NOT_FOUND error for an unknown query id is built inside the service body from the
    // correlation id read off the fragile context channel. If that channel is lost, the error
    // payload carries an empty correlation id — the same loss that produces empty
    // `correlation_id=` ok-lines in production logs.
    AtomicInteger mismatches = new AtomicInteger();
    AtomicReference<String> firstMismatchSample = new AtomicReference<>();

    runStress(
        "getUserObjectsCorrelation",
        (threadIdx, iteration) -> {
          String correlationId =
              "ctx-prop-corr-t" + threadIdx + "-i" + iteration + "-" + UUID.randomUUID();
          Metadata md = new Metadata();
          md.put(SESSION_HEADER, jwt);
          md.put(CORRELATION_HEADER, correlationId);
          md.put(ENGINE_KIND_HEADER, ENGINE_KIND);
          md.put(ENGINE_VERSION_HEADER, "");

          var userObjectsStub =
              UserObjectsServiceGrpc.newBlockingStub(channel)
                  .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));

          Iterator<UserObjectsBundleChunk> it =
              userObjectsStub.getUserObjects(
                  GetUserObjectsRequest.newBuilder()
                      .setQueryId("unknown-" + UUID.randomUUID())
                      .build());
          try {
            while (it.hasNext()) {
              it.next();
            }
            fail("expected NOT_FOUND for unknown query id");
          } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
              throw e;
            }
            ai.floedb.floecat.common.rpc.Error error;
            try {
              error = TestSupport.unpackMcError(e);
            } catch (Exception unpackFailure) {
              throw new IllegalStateException("could not unpack error details", unpackFailure);
            }
            String echoed = error == null ? null : error.getCorrelationId();
            if (!correlationId.equals(echoed)) {
              mismatches.incrementAndGet();
              firstMismatchSample.compareAndSet(
                  null,
                  "t="
                      + threadIdx
                      + " i="
                      + iteration
                      + " sent="
                      + correlationId
                      + " got="
                      + echoed);
            }
          }
        });

    assertEquals(
        0,
        mismatches.get(),
        () ->
            "Silent context-propagation failure: "
                + mismatches.get()
                + " NOT_FOUND error payload(s) did not echo the request correlation id — the"
                + " correlation id was lost between the inbound interceptor and the service body."
                + " First sample: "
                + firstMismatchSample.get());
  }

  // ---------- stress harness ----------

  @FunctionalInterface
  private interface Attempt {
    void run(int threadIdx, int iteration);
  }

  private void runStress(String label, Attempt attempt) throws Exception {
    int threads = Integer.getInteger("floecat.test.query-ctx.threads", DEFAULT_THREADS);
    int iterations =
        Integer.getInteger("floecat.test.query-ctx.iterations", DEFAULT_ITERATIONS_PER_THREAD);
    int total = threads * iterations;

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger successes = new AtomicInteger();
    ConcurrentMap<Status.Code, AtomicInteger> failuresByCode = new ConcurrentHashMap<>();
    ConcurrentMap<Status.Code, String> firstFailureSample = new ConcurrentHashMap<>();

    for (int t = 0; t < threads; t++) {
      final int threadIdx = t;
      pool.submit(
          () -> {
            try {
              start.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
            for (int i = 0; i < iterations; i++) {
              try {
                attempt.run(threadIdx, i);
                successes.incrementAndGet();
              } catch (StatusRuntimeException e) {
                Status.Code code = e.getStatus().getCode();
                failuresByCode.computeIfAbsent(code, k -> new AtomicInteger()).incrementAndGet();
                firstFailureSample.putIfAbsent(
                    code, "t=" + threadIdx + " i=" + i + " desc=" + e.getStatus().getDescription());
              } catch (Throwable t2) {
                failuresByCode
                    .computeIfAbsent(Status.Code.UNKNOWN, k -> new AtomicInteger())
                    .incrementAndGet();
                firstFailureSample.putIfAbsent(
                    Status.Code.UNKNOWN,
                    "t="
                        + threadIdx
                        + " i="
                        + i
                        + " ex="
                        + t2.getClass().getName()
                        + ": "
                        + t2.getMessage());
              }
            }
          });
    }

    start.countDown();
    pool.shutdown();
    boolean done = pool.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (!done) {
      pool.shutdownNow();
      fail(label + " stress did not complete within " + TIMEOUT_SECONDS + "s");
    }

    // Both codes mark a lost call context: PERMISSION_DENIED was the historical form (empty
    // principal reaching authz), UNAUTHENTICATED is the loud form after the Authorizer learned to
    // distinguish a missing identity from a missing grant.
    int contextLosses =
        failuresByCode.getOrDefault(Status.Code.PERMISSION_DENIED, new AtomicInteger()).get()
            + failuresByCode.getOrDefault(Status.Code.UNAUTHENTICATED, new AtomicInteger()).get();

    StringBuilder summary = new StringBuilder();
    summary
        .append(label)
        .append(": total=")
        .append(total)
        .append(" successes=")
        .append(successes.get());
    failuresByCode.forEach(
        (code, count) -> {
          summary
              .append(' ')
              .append(code)
              .append('=')
              .append(count.get())
              .append('(')
              .append(firstFailureSample.get(code))
              .append(')');
        });
    System.out.println(summary);

    if (contextLosses > 0) {
      fail(
          "Context propagation failure: "
              + contextLosses
              + "/"
              + total
              + " "
              + label
              + " calls returned PERMISSION_DENIED or UNAUTHENTICATED — the principal context was"
              + " lost between the inbound interceptor and the service body running on a Mutiny"
              + " worker. "
              + summary);
    }

    assertTrue(
        successes.get() > 0, "no " + label + " calls succeeded; test setup broken: " + summary);
  }

  private Metadata metadataFor(String correlationId) {
    Metadata md = new Metadata();
    md.put(SESSION_HEADER, jwt);
    md.put(CORRELATION_HEADER, correlationId + "-" + UUID.randomUUID());
    // Production planners always declare their engine; sending it on every call keeps the stress
    // path representative and arms the engine-gated resolution assertions.
    md.put(ENGINE_KIND_HEADER, ENGINE_KIND);
    md.put(ENGINE_VERSION_HEADER, "");
    return md;
  }

  private String initAccountJwt() throws Exception {
    var now = Instant.now();
    return Jwt.claims()
        .issuer("https://floecat.test")
        .subject("ctx-prop-it")
        .claim("account_id", accountId)
        .claim("roles", List.of("init-account"))
        .issuedAt(now)
        .expiresAt(now.plusSeconds(7L * 365 * 24 * 3600))
        .audience("floecat-client")
        .sign(TestKeyPair.privateKey());
  }
}
