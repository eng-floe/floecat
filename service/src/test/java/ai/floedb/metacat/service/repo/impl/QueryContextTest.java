package ai.floedb.metacat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.query.impl.QueryContext;
import ai.floedb.metacat.service.util.TestSupport;
import java.time.Clock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class QueryContextTest {
  private final Clock clock = Clock.systemUTC();

  private static PrincipalContext pc(ResourceId tenantId, String subject, String queryId) {
    var b =
        PrincipalContext.newBuilder()
            .setTenantId(tenantId.getId())
            .setSubject(subject == null ? "test-user" : subject);
    if (queryId != null) {
      b.setQueryId(queryId);
    }
    return b.build();
  }

  @Test
  void newActive_valid() {
    ResourceId tenantId = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT);
    var pc = pc(tenantId, "alice", "query-123");
    var ctx = QueryContext.newActive("query-123", pc, null, null, 1_000, 1);

    assertEquals("query-123", ctx.getQueryId());
    assertEquals(pc, ctx.getPrincipal());
    assertEquals(QueryContext.State.ACTIVE, ctx.getState());
    assertTrue(ctx.getExpiresAtMs() > ctx.getCreatedAtMs());
    assertEquals(1, ctx.getVersion());
    assertTrue(ctx.remainingTtlMs(clock.millis()) > 0);
  }

  @Test
  void builder_rejectsExpiresBeforeCreated() {
    ResourceId tenantId = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT);
    var pc = pc(tenantId, "alice", "p1");
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
    ResourceId tenantId = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT);
    var pc = pc(tenantId, "alice", "p1");
    var ctx = QueryContext.newActive("p1", pc, null, null, 200, 1);
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
    ResourceId tenantId = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT);
    var pc = pc(tenantId, "alice", "p1");
    var ctx = QueryContext.newActive("p1", pc, null, null, 100, 1);
    long targetGrace = clock.millis() + 500;
    var ended = ctx.end(true, targetGrace, 2);

    assertEquals(QueryContext.State.ENDED_COMMIT, ended.getState());
    assertTrue(ended.getExpiresAtMs() >= targetGrace);
    assertEquals(2, ended.getVersion());
  }

  @Test
  void asExpired_onlyIfActive() {
    ResourceId tenantId = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT);
    var pc = pc(tenantId, "alice", "p1");
    var ctx = QueryContext.newActive("p1", pc, null, null, 100, 1);

    var expired = ctx.asExpired(2);
    assertEquals(QueryContext.State.EXPIRED, expired.getState());
    assertEquals(2, expired.getVersion());

    var again = expired.asExpired(3);
    assertSame(expired, again);
  }
}
