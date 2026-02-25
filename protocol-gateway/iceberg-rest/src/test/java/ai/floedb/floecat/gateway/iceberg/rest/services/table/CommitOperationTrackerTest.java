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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CommitOperationTrackerTest {
  private CommitOperationTracker tracker;

  @BeforeEach
  void setUp() {
    tracker = new CommitOperationTracker();
    tracker.mapper = new ObjectMapper();
  }

  @Test
  void executeReplaysSuccessfulResponseForSamePayload() {
    AtomicInteger calls = new AtomicInteger();
    CommitOperationTracker.OperationKey key =
        new CommitOperationTracker.OperationKey("acct", "table:db.orders", "op-1");

    Response first =
        tracker.execute(
            key,
            Map.of("k", "v"),
            () -> {
              calls.incrementAndGet();
              return Response.noContent().build();
            });
    Response second =
        tracker.execute(
            key,
            Map.of("k", "v"),
            () -> {
              calls.incrementAndGet();
              return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
            });

    assertEquals(1, calls.get());
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), first.getStatus());
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), second.getStatus());
  }

  @Test
  void executeReturnsConflictForPayloadMismatch() {
    CommitOperationTracker.OperationKey key =
        new CommitOperationTracker.OperationKey("acct", "txn:foo", "tx-1");

    Response first = tracker.execute(key, Map.of("a", 1), () -> Response.noContent().build());
    Response second = tracker.execute(key, Map.of("a", 2), () -> Response.noContent().build());

    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), first.getStatus());
    assertEquals(Response.Status.CONFLICT.getStatusCode(), second.getStatus());
  }

  @Test
  void executeRetriesOnServerFailureAndReplaysOnClientFailure() {
    AtomicInteger retryableCalls = new AtomicInteger();
    CommitOperationTracker.OperationKey retryableKey =
        new CommitOperationTracker.OperationKey("acct", "table:db.orders", "op-retry");

    Response firstServerFailure =
        tracker.execute(
            retryableKey,
            Map.of("k", "v"),
            () -> {
              retryableCalls.incrementAndGet();
              return Response.status(Status.INTERNAL_SERVER_ERROR)
                  .entity(Map.of("e", "boom"))
                  .build();
            });
    Response secondRetry =
        tracker.execute(
            retryableKey,
            Map.of("k", "v"),
            () -> {
              retryableCalls.incrementAndGet();
              return Response.noContent().build();
            });

    assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), firstServerFailure.getStatus());
    assertEquals(Status.NO_CONTENT.getStatusCode(), secondRetry.getStatus());
    assertEquals(2, retryableCalls.get());

    AtomicInteger terminalCalls = new AtomicInteger();
    CommitOperationTracker.OperationKey terminalKey =
        new CommitOperationTracker.OperationKey("acct", "table:db.orders", "op-terminal");
    Response firstClientFailure =
        tracker.execute(
            terminalKey,
            Map.of("k", "v"),
            () -> {
              terminalCalls.incrementAndGet();
              return Response.status(Status.CONFLICT).entity(Map.of("e", "conflict")).build();
            });
    Response replayedClientFailure =
        tracker.execute(
            terminalKey,
            Map.of("k", "v"),
            () -> {
              terminalCalls.incrementAndGet();
              return Response.noContent().build();
            });

    assertEquals(Status.CONFLICT.getStatusCode(), firstClientFailure.getStatus());
    assertEquals(Status.CONFLICT.getStatusCode(), replayedClientFailure.getStatus());
    assertEquals(1, terminalCalls.get());
  }

  @Test
  void executeLoadsPersistedRecordAcrossTrackerInstances() throws Exception {
    Path dir = Files.createTempDirectory("commit-op-tracker");
    Config cfg = mock(Config.class);
    when(cfg.getOptionalValue(
            eq("floecat.gateway.commit-op.in-progress-timeout-ms"), eq(Long.class)))
        .thenReturn(java.util.Optional.of(120_000L));
    when(cfg.getOptionalValue(eq("floecat.gateway.commit-op.store-dir"), eq(String.class)))
        .thenReturn(java.util.Optional.of(dir.toString()));

    CommitOperationTracker.OperationKey key =
        new CommitOperationTracker.OperationKey("acct", "table:db.orders", "op-persist");

    tracker.config = cfg;
    AtomicInteger firstCalls = new AtomicInteger();
    Response first =
        tracker.execute(
            key,
            Map.of("a", 1),
            () -> {
              firstCalls.incrementAndGet();
              return Response.noContent().build();
            });
    assertEquals(Status.NO_CONTENT.getStatusCode(), first.getStatus());
    assertEquals(1, firstCalls.get());

    CommitOperationTracker secondTracker = new CommitOperationTracker();
    secondTracker.mapper = new ObjectMapper();
    secondTracker.config = cfg;
    AtomicInteger secondCalls = new AtomicInteger();
    Response replay =
        secondTracker.execute(
            key,
            Map.of("a", 1),
            () -> {
              secondCalls.incrementAndGet();
              return Response.status(Status.INTERNAL_SERVER_ERROR).build();
            });
    assertEquals(Status.NO_CONTENT.getStatusCode(), replay.getStatus());
    assertEquals(0, secondCalls.get());
  }

  @Test
  void executeRetriesStaleInProgressRecord() throws Exception {
    Path dir = Files.createTempDirectory("commit-op-stale");
    Config cfg = mock(Config.class);
    when(cfg.getOptionalValue(
            eq("floecat.gateway.commit-op.in-progress-timeout-ms"), eq(Long.class)))
        .thenReturn(java.util.Optional.of(5_000L));
    when(cfg.getOptionalValue(eq("floecat.gateway.commit-op.store-dir"), eq(String.class)))
        .thenReturn(java.util.Optional.of(dir.toString()));
    tracker.config = cfg;

    CommitOperationTracker.OperationKey key =
        new CommitOperationTracker.OperationKey("acct", "table:db.orders", "op-stale");
    String digest = hashPayload("acct|table:db.orders|op-stale");
    long now = System.currentTimeMillis();
    String staleJson =
        "{"
            + "\"requestHash\":\""
            + hashPayload(Map.of("a", 1))
            + "\","
            + "\"steps\":[\"PLAN_OK\"],"
            + "\"state\":\"IN_PROGRESS\","
            + "\"status\":0,"
            + "\"entity\":null,"
            + "\"createdAtMs\":"
            + (now - 20_000L)
            + ","
            + "\"updatedAtMs\":"
            + (now - 20_000L)
            + "}";
    Files.writeString(dir.resolve(digest + ".json"), staleJson, StandardCharsets.UTF_8);

    AtomicInteger calls = new AtomicInteger();
    Response response =
        tracker.execute(
            key,
            Map.of("a", 1),
            () -> {
              calls.incrementAndGet();
              return Response.noContent().build();
            });

    assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
    assertEquals(1, calls.get());
    String updated = Files.readString(dir.resolve(digest + ".json"), StandardCharsets.UTF_8);
    assertTrue(updated.contains("\"state\":\"SUCCEEDED\""));
    assertTrue(updated.contains("RETRY_STALE_IN_PROGRESS"));
  }

  private String hashPayload(Object payload) throws Exception {
    byte[] bytes = tracker.mapper.writeValueAsBytes(payload);
    return java.util.Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
  }
}
