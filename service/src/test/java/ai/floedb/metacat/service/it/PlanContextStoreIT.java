package ai.floedb.metacat.service.it;

import java.time.Clock;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.service.planning.impl.PlanContextStoreImpl;
import ai.floedb.metacat.service.planning.impl.PlanContext;

@QuarkusTest
class PlanContextStoreIT {

  public static class StoreTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "metacat.plan.default-ttl-ms", "100",   // small default TTL
          "metacat.plan.ended-grace-ms", "80",    // small grace TTL
          "metacat.plan.max-size", "1000"
      );
    }
  }

  @Inject
  PlanContextStoreImpl store;

  private final Clock clock = Clock.systemUTC();

  private static PrincipalContext pc(String tenant, String planId) {
    var b = PrincipalContext.newBuilder()
        .setTenantId(tenant)
        .setSubject("it-user");
    if (planId != null) b.setPlanId(planId);
    return b.build();
  }

  private static PlanContext newPlan(String planId, String tenant, long ttlMs) {
    return PlanContext.newActive(
        planId, tenant, pc(tenant, planId),
        /*expansion*/null, /*snapshotSet*/null,
        ttlMs, /*version*/1
    );
  }

  @Test
  void putGet_roundTrip_active() {
    String planId = "p-rt-1";
    var ctx = newPlan(planId, "t-001", 500);
    store.put(ctx);

    var got = store.get(planId).orElseThrow();
    assertEquals(PlanContext.State.ACTIVE, got.getState());
    assertEquals(planId, got.getPlanId());
    assertEquals("t-001", got.getTenantId());
    assertEquals(ctx.getPrincipal(), got.getPrincipal());
    assertTrue(got.getExpiresAtMs() >= ctx.getCreatedAtMs());
  }

  @Test
  void get_afterTtl_marksExpiredSoftly() throws Exception {
    String planId = "p-exp-1";
    // Very short ttl so we can observe expiry quickly
    var ctx = newPlan(planId, "t-001", 50);
    store.put(ctx);

    // Sleep beyond lease
    Thread.sleep(70);

    var got = store.get(planId).orElseThrow();
    // store.get() will mark ACTIVE→EXPIRED if past expiresAtMs
    assertEquals(PlanContext.State.EXPIRED, got.getState());
  }

  @Test
  void extendLease_monotonic_whenActive() {
    String planId = "p-extend-1";
    var ctx = newPlan(planId, "t-001", 100);
    store.put(ctx);

    var before = store.get(planId).orElseThrow();
    long oldExp = before.getExpiresAtMs();

    // Request earlier than current expiry => no change
    var same = store.extendLease(planId, oldExp - 50).orElseThrow();
    assertEquals(oldExp, same.getExpiresAtMs());

    // Request later => should extend
    long requested = oldExp + 250;
    var extended = store.extendLease(planId, requested).orElseThrow();
    assertTrue(extended.getExpiresAtMs() >= requested);
  }

  @Test
  void extendLease_noop_whenEnded() {
    String planId = "p-extend-ended";
    var ctx = newPlan(planId, "t-001", 100);
    store.put(ctx);

    store.end(planId, true).orElseThrow(); // commit

    var after = store.extendLease(planId, clock.millis() + 10_000).orElseThrow();
    assertEquals(PlanContext.State.ENDED_COMMIT, after.getState(), "state should remain ended");
  }

  @Test
  void end_setsStateAndGrace() {
    String planId = "p-end-1";
    var ctx = newPlan(planId, "t-001", 100);
    store.put(ctx);

    var ended = store.end(planId, /*commit*/true).orElseThrow();
    assertEquals(PlanContext.State.ENDED_COMMIT, ended.getState());

    // Grace TTL should push expiry into the future (relative to now)
    assertTrue(ended.getExpiresAtMs() >= clock.millis());
  }

  @Test
  void delete_and_size() {
    String planId = "p-del-1";
    var ctx = newPlan(planId, "t-001", 200);
    store.put(ctx);

    assertTrue(store.size() >= 1);

    assertTrue(store.delete(planId));
    assertTrue(store.get(planId).isEmpty());
  }
}