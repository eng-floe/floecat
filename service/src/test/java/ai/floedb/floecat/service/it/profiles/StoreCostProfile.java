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

package ai.floedb.floecat.service.it.profiles;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.HashMap;
import java.util.Map;

/**
 * Substitutes counting stores for the in-memory ones, so a test can assert what a request costs.
 *
 * <p>The counting stores subclass the in-memory implementations this profile already uses, so
 * nothing about storage behaviour changes — only that calls are tallied. Selecting them through a
 * profile rather than annotating them globally keeps every other test on the plain stores: a
 * counter shared across tests would be meaningless.
 */
public class StoreCostProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(
        "quarkus.arc.selected-alternatives",
        "ai.floedb.floecat.service.testsupport.CountingPointerStore,"
            + "ai.floedb.floecat.service.testsupport.CountingBlobStore,"
            + "ai.floedb.floecat.service.testsupport.StoreCostMeter");

    // Every short TTL on a read path this measures is pinned past the length of any test here.
    // A TTL firing between warming a request and measuring it makes the cost record the clock
    // rather than the read path. Both of these default to two seconds, both sit on the warm
    // resolution, and pinning only one leaves the same symptom arriving from the other -- which is
    // what made these numbers look unmeasurable. The pointer cache is due to be replaced; the
    // pinning stays regardless, because a measurement must not depend on a timer either way.
    overrides.put("floecat.root.pointer-cache-ttl-seconds", "600");
    overrides.put("floecat.metadata.graph.meta-cache-ttl-seconds", "600");

    // The topology cache is bounded by ENTRIES, not time, and it is application-scoped: another
    // suite filling it evicts what this one relied on, so the same scan reads one pointer alone and
    // twenty after a neighbour has run. Sized past anything a test fixture creates, that eviction
    // cannot happen and the count stops depending on what ran before.
    overrides.put("floecat.topology.ns-cache-size", "100000");
    overrides.put("floecat.topology.rel-cache-size", "100000");

    // One switch, not a list of schedulers. A GC tick or reconcile poll landing inside the measured
    // window is recorded as part of the request -- which is how one request measured 17 KV reads on
    // one run and 19 on the next. Quarkus owns every @Scheduled bean here, so disabling its
    // scheduler quiesces all of them, including any added later; GcOnProfile and
    // ReconcileJobStoreControlPlaneProfile use the same switch.
    overrides.put("quarkus.scheduler.enabled", "false");

    // Not @Scheduled: the reconciler's own auto-drive loop.
    overrides.put("floecat.reconciler.auto.enabled", "false");

    return overrides;
  }
}
