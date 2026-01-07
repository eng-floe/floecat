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

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.query.impl.QueryContextStoreImpl;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
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
          "floecat.query.default-ttl-ms", "100",
          "floecat.query.ended-grace-ms", "80",
          "floecat.query.max-size", "1000");
    }
  }

  @Inject QueryContextStoreImpl store;

  private final Clock clock = Clock.systemUTC();

  private static PrincipalContext pc(String queryId) {
    ResourceId accountId = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT);
    var b = PrincipalContext.newBuilder().setAccountId(accountId.getId()).setSubject("it-user");
    if (queryId != null) {
      b.setQueryId(queryId);
    }
    return b.build();
  }

  private static QueryContext newQuery(String queryId, long ttlMs) {
    return QueryContext.newActive(
        queryId,
        pc(queryId),
        null,
        null,
        null,
        null,
        ttlMs,
        1,
        ResourceId.newBuilder().setId("cat-it").build());
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
