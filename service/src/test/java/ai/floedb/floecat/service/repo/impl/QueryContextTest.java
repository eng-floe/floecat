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

package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.util.TestSupport;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class QueryContextTest {
  private final Clock clock = Clock.systemUTC();

  private static PrincipalContext pc(ResourceId accountId, String subject, String queryId) {
    var b =
        PrincipalContext.newBuilder()
            .setAccountId(accountId.getId())
            .setSubject(subject == null ? "test-user" : subject);
    if (queryId != null) {
      b.setQueryId(queryId);
    }
    return b.build();
  }

  @Test
  void newActive_valid() {
    ResourceId accountId = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT);
    var pc = pc(accountId, "alice", "query-123");
    var ctx =
        QueryContext.newActive(
            "query-123",
            pc,
            null,
            null,
            null,
            null,
            1_000,
            1,
            ResourceId.newBuilder().setId("cat-it").build());

    assertEquals("query-123", ctx.getQueryId());
    assertEquals(pc, ctx.getPrincipal());
    assertEquals(QueryContext.State.ACTIVE, ctx.getState());
    assertTrue(ctx.getExpiresAtMs() > ctx.getCreatedAtMs());
    assertEquals(1, ctx.getVersion());
    assertTrue(ctx.remainingTtlMs(clock.millis()) > 0);
  }

  @Test
  void builder_rejectsExpiresBeforeCreated() {
    ResourceId accountId = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT);
    var pc = pc(accountId, "alice", "p1");
    Executable ex =
        () ->
            QueryContext.builder()
                .queryId("p1")
                .principal(pc)
                .createdAtMs(2000)
                .expiresAtMs(1000)
                .build();
    var err = assertThrows(IllegalArgumentException.class, ex);
    assertTrue(err.getMessage().contains("expiresAtMs must be >="));
  }

  @Test
  void extendLease_isMonotonic() {
    ResourceId accountId = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT);
    var pc = pc(accountId, "alice", "p1");
    var ctx =
        QueryContext.newActive(
            "p1",
            pc,
            null,
            null,
            null,
            null,
            200,
            1,
            ResourceId.newBuilder().setId("cat-it").build());
    long originalExp = ctx.getExpiresAtMs();

    var same = ctx.extendLease(originalExp - 50, 2);
    assertEquals(originalExp, same.getExpiresAtMs());
    assertEquals(ctx, same);

    var extended = ctx.extendLease(originalExp + 500, 3);
    assertTrue(extended.getExpiresAtMs() > originalExp);
    assertEquals(3, extended.getVersion());
  }

  @Test
  void end_commit_setsStateAndGrace() {
    ResourceId accountId = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT);
    var pc = pc(accountId, "alice", "p1");
    var ctx =
        QueryContext.newActive(
            "p1",
            pc,
            null,
            null,
            null,
            null,
            100,
            1,
            ResourceId.newBuilder().setId("cat-it").build());
    long targetGrace = clock.millis() + 500;
    var ended = ctx.end(true, targetGrace, 2);

    assertEquals(QueryContext.State.ENDED_COMMIT, ended.getState());
    assertTrue(ended.getExpiresAtMs() >= targetGrace);
    assertEquals(2, ended.getVersion());
  }

  @Test
  void asExpired_onlyIfActive() {
    ResourceId accountId = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT);
    var pc = pc(accountId, "alice", "p1");
    var ctx =
        QueryContext.newActive(
            "p1",
            pc,
            null,
            null,
            null,
            null,
            100,
            1,
            ResourceId.newBuilder().setId("cat-it").build());

    var expired = ctx.asExpired(2);
    assertEquals(QueryContext.State.EXPIRED, expired.getState());
    assertEquals(2, expired.getVersion());

    var again = expired.asExpired(3);
    assertSame(expired, again);
  }

  // Ensures QueryContext surfaces the pinned SnapshotPin for tables participating in the lease.
  @Test
  void requireSnapshotPinReturnsPin() {
    ResourceId accountId = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT);
    var pc = pc(accountId, "alice", "p-lookup");
    ResourceId tableId = TestSupport.rid(accountId.getId(), "tbl-lookup", ResourceKind.RK_TABLE);
    var snapshots =
        SnapshotSet.newBuilder()
            .addPins(SnapshotPin.newBuilder().setTableId(tableId).setSnapshotId(77).build())
            .build();
    var ctx =
        QueryContext.newActive(
            "p-lookup",
            pc,
            null,
            snapshots.toByteArray(),
            null,
            null,
            500,
            1,
            ResourceId.newBuilder().setId("cat-it").build());

    var pin = ctx.requireSnapshotPin(tableId, "corr-123");
    assertEquals(77, pin.getSnapshotId());
    assertTrue(pin.hasTableId());
  }

  // Ensures QueryContext rejects tables that were not pinned in the lease snapshot set.
  @Test
  void requireSnapshotPinMissingTable() {
    ResourceId accountId = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT);
    var pc = pc(accountId, "alice", "p-missing");
    ResourceId pinned = TestSupport.rid(accountId.getId(), "tbl-a", ResourceKind.RK_TABLE);
    ResourceId other = TestSupport.rid(accountId.getId(), "tbl-b", ResourceKind.RK_TABLE);

    var snapshots =
        SnapshotSet.newBuilder()
            .addPins(SnapshotPin.newBuilder().setTableId(pinned).setSnapshotId(900).build())
            .build();

    var ctx =
        QueryContext.newActive(
            "p-missing",
            pc,
            null,
            snapshots.toByteArray(),
            null,
            null,
            500,
            1,
            ResourceId.newBuilder().setId("cat-it").build());

    StatusRuntimeException err =
        assertThrows(
            StatusRuntimeException.class, () -> ctx.requireSnapshotPin(other, "corr-missing"));
    assertEquals(Status.Code.NOT_FOUND, err.getStatus().getCode());
  }
}
