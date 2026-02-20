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

public class ProfilingTestProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "quarkus.profile", "telemetry-prof",
        "floecat.profiling.enabled", "true",
        "floecat.profiling.endpoints-enabled", "true",
        "floecat.profiling.capture-duration", "100MS",
        "floecat.profiling.max-capture-bytes", "1048576",
        "floecat.profiling.rate-limit", "5",
        "floecat.profiling.rate-window", "1S");
  }
}
