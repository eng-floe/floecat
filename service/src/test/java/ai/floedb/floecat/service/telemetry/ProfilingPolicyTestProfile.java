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

package ai.floedb.floecat.service.telemetry;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class ProfilingPolicyTestProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.ofEntries(
        Map.entry("quarkus.profile", "telemetry-prof"),
        Map.entry("floecat.profiling.enabled", "true"),
        Map.entry("floecat.profiling.endpoints-enabled", "true"),
        Map.entry("floecat.profiling.capture-duration", "100MS"),
        Map.entry("floecat.profiling.max-capture-bytes", "1048576"),
        Map.entry("floecat.profiling.rate-limit", "10"),
        Map.entry("floecat.profiling.rate-window", "5S"),
        Map.entry("floecat.profiling.policy.enabled", "true"),
        Map.entry("floecat.profiling.policy.gc.enabled", "true"),
        Map.entry("floecat.profiling.policy.gc.threshold-bytes", "1048576"),
        Map.entry("floecat.profiling.policy.gc.gc-name", "G1 Old Generation"),
        Map.entry("floecat.profiling.policy.gc.cooldown", "PT0S"),
        Map.entry("floecat.profiling.policy-poll-interval", "PT2S"));
  }
}
