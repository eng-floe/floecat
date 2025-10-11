package ai.floedb.metacat.service.repo.impl;

import java.time.Clock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.service.planning.impl.PlanContext;

class PlanContextTest {

  private final Clock clock = Clock.systemUTC();

  private static PrincipalContext pc(String tenant, String subject, String planId) {
    var b = PrincipalContext.newBuilder()
      .setTenantId(tenant)
      .setSubject(subject == null ? "test-user" : subject);
    if (planId != null) b.setPlanId(planId);
    return b.build();
  }

  @Test
  void newActive_valid() {
    var pc = pc("t-001", "alice", "plan-123");
    var ctx = PlanContext.newActive(
      "plan-123", "t-001", pc, null, null,
      1_000, 1);

    assertEquals("plan-123", ctx.getPlanId());
    assertEquals("t-001", ctx.getTenantId());
    assertEquals(pc, ctx.getPrincipal());
    assertEquals(PlanContext.State.ACTIVE, ctx.getState());
    assertTrue(ctx.getExpiresAtMs() > ctx.getCreatedAtMs());
    assertEquals(1, ctx.getVersion());
    assertTrue(ctx.remainingTtlMs(clock.millis()) > 0);
  }

  @Test
  void newActive_rejectsTenantMismatch() {
    var pc = pc("t-002", "alice", "plan-123");
    Executable ex = () -> PlanContext.newActive("plan-123", "t-001", pc, null, null, 1000, 1);
    var err = assertThrows(IllegalArgumentException.class, ex);
    assertTrue(err.getMessage().contains("tenantId must match"));
  }

  @Test
  void builder_rejectsExpiresBeforeCreated() {
    var pc = pc("t-001", "alice", "p1");
    Executable ex = () -> PlanContext.builder()
      .planId("p1")
      .tenantId("t-001")
      .principal(pc)
      .createdAtMs(2000)
      .expiresAtMs(1000) // earlier than createdAtMs
      .build();
    var err = assertThrows(IllegalArgumentException.class, ex);
    assertTrue(err.getMessage().contains("expiresAtMs must be >="));
  }

  @Test
  void extendLease_isMonotonic() {
    var pc = pc("t-001", "alice", "p1");
    var ctx = PlanContext.newActive("p1", "t-001", pc, null, null, 200, 1);
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
    var pc = pc("t-001", "alice", "p1");
    var ctx = PlanContext.newActive("p1", "t-001", pc, null, null, 100, 1);
    long targetGrace = clock.millis() + 500;
    var ended = ctx.end(true, targetGrace, 2);

    assertEquals(PlanContext.State.ENDED_COMMIT, ended.getState());
    assertTrue(ended.getExpiresAtMs() >= targetGrace);
    assertEquals(2, ended.getVersion());
  }

  @Test
  void asExpired_onlyIfActive() {
    var pc = pc("t-001", "alice", "p1");
    var ctx = PlanContext.newActive("p1", "t-001", pc, null, null, 100, 1);

    var expired = ctx.asExpired(2);
    assertEquals(PlanContext.State.EXPIRED, expired.getState());
    assertEquals(2, expired.getVersion());

    var again = expired.asExpired(3);
    assertSame(expired, again);
  }
}
