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
import java.time.Duration;

@ConfigMapping(prefix = "floecat.profiling")
public interface ProfilingConfig {
  @WithDefault("false")
  boolean enabled();

  @WithDefault("30S")
  Duration captureDuration();

  @WithDefault("52428800")
  long maxCaptureBytes();

  @WithDefault("524288000")
  long totalMaxBytes();

  @WithDefault("1")
  int rateLimit();

  @WithDefault("60S")
  Duration rateWindow();

  @WithDefault("${java.io.tmpdir}/floecat/profiling/captures")
  String artifactDir();

  @WithDefault("false")
  boolean endpointsEnabled();

  Policy policy();

  interface Policy {
    @WithDefault("false")
    boolean enabled();

    LatencyPolicy latency();

    ExecutorQueuePolicy executorQueue();

    GcPolicy gc();
  }

  interface PolicySpec {
    @WithDefault("false")
    boolean enabled();

    @WithDefault("PT30S")
    Duration window();

    @WithDefault("PT1M")
    Duration cooldown();
  }

  interface LatencyPolicy extends PolicySpec {
    @WithDefault("PT0.5S")
    Duration threshold();
  }

  interface ExecutorQueuePolicy extends PolicySpec {
    @WithDefault("32")
    int threshold();

    @WithDefault("vert.x-worker-thread")
    String poolName();
  }

  interface GcPolicy extends PolicySpec {
    @WithDefault("67108864")
    long thresholdBytes();

    @WithDefault("G1 Old Generation")
    String gcName();
  }

  @WithDefault("30S")
  Duration policyPollInterval();
}
