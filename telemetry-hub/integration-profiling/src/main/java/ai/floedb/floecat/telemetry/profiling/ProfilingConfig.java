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

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Optional;

/**
 * Configuration for Floecat's single profiling workflow: continuous JVM profiling exported to
 * Pyroscope. Lives under {@code floecat.profiling.*}.
 */
@ConfigMapping(prefix = "floecat.profiling")
public interface ProfilingConfig {
  /** Whether to start the continuous Pyroscope JVM profiler on startup. */
  @WithDefault("false")
  boolean enabled();

  /**
   * Pyroscope server URL. Mapped from the {@code FLOECAT_PYROSCOPE_ENDPOINT} / {@code
   * FLOESCAN_PYROSCOPE_ENDPOINT} environment variables in {@code application.properties}.
   */
  Optional<String> serverUrl();

  /** Optional multi-tenant id forwarded to Pyroscope as the {@code X-Scope-OrgID} tenant. */
  Optional<String> tenantId();
}
