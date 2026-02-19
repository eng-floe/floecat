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

package ai.floedb.floecat.telemetry.profiling;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.floedb.floecat.telemetry.TelemetryRegistry;
import org.junit.jupiter.api.Test;

class ProfilingTelemetryContributorTest {
  @Test
  void registersProfilingMetric() {
    TelemetryRegistry registry = new TelemetryRegistry();
    new ProfilingTelemetryContributor().contribute(registry);
    assertNotNull(registry.metric("floecat.profiling.captures.total"));
  }
}
