package ai.floedb.metacat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.bootstrap.impl.SeedRunner;
import ai.floedb.metacat.service.query.impl.QueryContext;
import ai.floedb.metacat.service.query.impl.QueryContextStoreImpl;
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
class QueryContextStoreIT {
  public static class StoreTestProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "metacat.query.default-ttl-ms", "100",
          "metacat.query.ended-grace-ms", "80",
          "metacat.query.max-size", "1000");
    }
  }

  @Inject QueryContextStoreImpl store;

  private final Clock clock = Clock.systemUTC();

  private static PrincipalContext pc(String queryId) {
    ResourceId tenantId = TestSupport.createTenantId(TestSupport.DEFAULT_SEED_TENANT);
    var b = PrincipalContext.newBuilder().setTenantId(tenantId.getId()).setSubject("it-user");
    if (queryId != null) {
      b.setQueryId(queryId);
    }
    return b.build();
  }

  private static QueryContext newQuery(String queryId, long ttlMs) {
    return QueryContext.newActive(queryId, pc(queryId), null, null, null, null, ttlMs, 1);
  }

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void queryPutGet() {
    String queryId = "q-rt-1";

    var ctx = newQuery(queryId, 500);
    store.put(ctx);

    var got = store.get(queryId).orElseThrow();
    assertEquals(QueryContext.State.ACTIVE, got.getState());
    assertEquals(queryId, got.getQueryId());
    assertEquals(ctx.getPrincipal(), got.getPrincipal());
    assertTrue(got.getExpiresAtMs() >= ctx.getCreatedAtMs());
  }

  @Test
  void queryExpired() throws Exception {
    String queryId = "q-exp-1";
    var ctx = newQuery(queryId, 50);
    store.put(ctx);

    Thread.sleep(70);

    var got = store.get(queryId).orElseThrow();
    assertEquals(QueryContext.State.EXPIRED, got.getState());
  }

  @Test
  void queryExtendLease() {
    String queryId = "q-extend-1";
    var ctx = newQuery(queryId, 100);
    store.put(ctx);

    var before = store.get(queryId).orElseThrow();
    long oldExp = before.getExpiresAtMs();

    var same = store.extendLease(queryId, oldExp - 50).orElseThrow();
    assertEquals(oldExp, same.getExpiresAtMs());

    long requested = oldExp + 250;
    var extended = store.extendLease(queryId, requested).orElseThrow();
    assertTrue(extended.getExpiresAtMs() >= requested);
  }

  @Test
  void queryNoExtendIfExpired() {
    String queryId = "q-extend-ended";
    var ctx = newQuery(queryId, 100);
    store.put(ctx);

    store.end(queryId, true).orElseThrow();

    var after = store.extendLease(queryId, clock.millis() + 10_000).orElseThrow();
    assertEquals(QueryContext.State.ENDED_COMMIT, after.getState(), "state should remain ended");
  }

  @Test
  void queryEndCommit() {
    String queryId = "q-end-1";
    var ctx = newQuery(queryId, 100);
    store.put(ctx);

    var ended = store.end(queryId, true).orElseThrow();
    assertEquals(QueryContext.State.ENDED_COMMIT, ended.getState());

    assertTrue(ended.getExpiresAtMs() >= clock.millis());
  }

  @Test
  void queryDelete() {
    String queryId = "q-del-1";
    var ctx = newQuery(queryId, 200);
    store.put(ctx);

    assertTrue(store.size() >= 1);

    assertTrue(store.delete(queryId));
    assertTrue(store.get(queryId).isEmpty());
  }
}
