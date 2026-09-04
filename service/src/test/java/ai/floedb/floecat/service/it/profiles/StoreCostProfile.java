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
 * Selects a detailed local observer, so a test can assert what a request costs.
 *
 * <p>The production and test observers sit behind the same store decorators. Selecting the recorder
 * through a profile changes only where observations go; the stores and their behaviour are exactly
 * those the running service uses. Every other test keeps the production telemetry observer.
 */
public class StoreCostProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(
        "quarkus.arc.selected-alternatives",
        "ai.floedb.floecat.service.testsupport.RecordingStoreReadObserver,"
            + "ai.floedb.floecat.service.testsupport.StoreCostMeter");

    // The two short TTLs this profile used to pin are gone with the caches they belonged to: the
    // pointer cache replaces both. Nothing here depends on a timer any more, which is the point --
    // the stability came from closing the measurement window, not from the pins.

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
