package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.planning.impl.PlanContext;
import ai.floedb.metacat.service.planning.impl.PlanContextStoreImpl;
import ai.floedb.metacat.service.util.TestDataResetter;
import ai.floedb.metacat.service.util.TestSupport;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class PlanContextStoreIT {
  public static class StoreTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "metacat.plan.default-ttl-ms", "100",
          "metacat.plan.ended-grace-ms", "80",
          "metacat.plan.max-size", "1000");
    }
  }

  @Inject PlanContextStoreImpl store;

  private final Clock clock = Clock.systemUTC();

  private static PrincipalContext pc(String planId) {
    ResourceId tenantId = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT);
    var b = PrincipalContext.newBuilder().setTenantId(tenantId.getId()).setSubject("it-user");
    if (planId != null) {
      b.setPlanId(planId);
    }
    return b.build();
  }

  private static PlanContext newPlan(String planId, long ttlMs) {
    return PlanContext.newActive(planId, pc(planId), null, null, ttlMs, 1);
  }

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void planPutGet() {
    String planId = "p-rt-1";

    var ctx = newPlan(planId, 500);
    store.put(ctx);

    var got = store.get(planId).orElseThrow();
    assertEquals(PlanContext.State.ACTIVE, got.getState());
    assertEquals(planId, got.getPlanId());
    assertEquals(ctx.getPrincipal(), got.getPrincipal());
    assertTrue(got.getExpiresAtMs() >= ctx.getCreatedAtMs());
  }

  @Test
  void planExpired() throws Exception {
    String planId = "p-exp-1";
    var ctx = newPlan(planId, 50);
    store.put(ctx);

    Thread.sleep(70);

    var got = store.get(planId).orElseThrow();
    assertEquals(PlanContext.State.EXPIRED, got.getState());
  }

  @Test
  void planExtendLease() {
    String planId = "p-extend-1";
    var ctx = newPlan(planId, 100);
    store.put(ctx);

    var before = store.get(planId).orElseThrow();
    long oldExp = before.getExpiresAtMs();

    var same = store.extendLease(planId, oldExp - 50).orElseThrow();
    assertEquals(oldExp, same.getExpiresAtMs());

    long requested = oldExp + 250;
    var extended = store.extendLease(planId, requested).orElseThrow();
    assertTrue(extended.getExpiresAtMs() >= requested);
  }

  @Test
  void planNoExtendIfExpired() {
    String planId = "p-extend-ended";
    var ctx = newPlan(planId, 100);
    store.put(ctx);

    store.end(planId, true).orElseThrow();

    var after = store.extendLease(planId, clock.millis() + 10_000).orElseThrow();
    assertEquals(PlanContext.State.ENDED_COMMIT, after.getState(), "state should remain ended");
  }

  @Test
  void planEndCommit() {
    String planId = "p-end-1";
    var ctx = newPlan(planId, 100);
    store.put(ctx);

    var ended = store.end(planId, true).orElseThrow();
    assertEquals(PlanContext.State.ENDED_COMMIT, ended.getState());

    assertTrue(ended.getExpiresAtMs() >= clock.millis());
  }

  @Test
  void planDelete() {
    String planId = "p-del-1";
    var ctx = newPlan(planId, 200);
    store.put(ctx);

    assertTrue(store.size() >= 1);

    assertTrue(store.delete(planId));
    assertTrue(store.get(planId).isEmpty());
  }
}
