package ai.floedb.metacat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.planning.impl.PlanContext;
import ai.floedb.metacat.service.util.TestSupport;
import java.time.Clock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class PlanContextTest {
  private final Clock clock = Clock.systemUTC();

  private static PrincipalContext pc(ResourceId tenantId, String subject, String planId) {
    var b =
        PrincipalContext.newBuilder()
            .setTenantId(tenantId)
            .setSubject(subject == null ? "test-user" : subject);
    if (planId != null) {
      b.setPlanId(planId);
    }
    return b.build();
  }

  @Test
  void newActive_valid() {
    ResourceId tenantId = TestSupport.createTenantId("t-001");
    var pc = pc(tenantId, "alice", "plan-123");
    var ctx = PlanContext.newActive("plan-123", pc, null, null, 1_000, 1);

    assertEquals("plan-123", ctx.getPlanId());
    assertEquals(pc, ctx.getPrincipal());
    assertEquals(PlanContext.State.ACTIVE, ctx.getState());
    assertTrue(ctx.getExpiresAtMs() > ctx.getCreatedAtMs());
    assertEquals(1, ctx.getVersion());
    assertTrue(ctx.remainingTtlMs(clock.millis()) > 0);
  }

  @Test
  void builder_rejectsExpiresBeforeCreated() {
    ResourceId tenantId = TestSupport.createTenantId("t-001");
    var pc = pc(tenantId, "alice", "p1");
    Executable ex =
        () ->
            PlanContext.builder()
                .planId("p1")
                .principal(pc)
                .createdAtMs(2000)
                .expiresAtMs(1000)
                .build();
    var err = assertThrows(IllegalArgumentException.class, ex);
    assertTrue(err.getMessage().contains("expiresAtMs must be >="));
  }

  @Test
  void extendLease_isMonotonic() {
    ResourceId tenantId = TestSupport.createTenantId("t-001");
    var pc = pc(tenantId, "alice", "p1");
    var ctx = PlanContext.newActive("p1", pc, null, null, 200, 1);
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
    ResourceId tenantId = TestSupport.createTenantId("t-001");
    var pc = pc(tenantId, "alice", "p1");
    var ctx = PlanContext.newActive("p1", pc, null, null, 100, 1);
    long targetGrace = clock.millis() + 500;
    var ended = ctx.end(true, targetGrace, 2);

    assertEquals(PlanContext.State.ENDED_COMMIT, ended.getState());
    assertTrue(ended.getExpiresAtMs() >= targetGrace);
    assertEquals(2, ended.getVersion());
  }

  @Test
  void asExpired_onlyIfActive() {
    ResourceId tenantId = TestSupport.createTenantId("t-001");
    var pc = pc(tenantId, "alice", "p1");
    var ctx = PlanContext.newActive("p1", pc, null, null, 100, 1);

    var expired = ctx.asExpired(2);
    assertEquals(PlanContext.State.EXPIRED, expired.getState());
    assertEquals(2, expired.getVersion());

    var again = expired.asExpired(3);
    assertSame(expired, again);
  }
}
