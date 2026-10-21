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

import io.pyroscope.http.Format;
import io.pyroscope.javaagent.EventType;
import io.pyroscope.javaagent.PyroscopeAgent;
import io.pyroscope.javaagent.config.Config;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.jboss.logging.Logger;

/** Starts continuous JVM profiling when a Pyroscope endpoint is configured. */
@ApplicationScoped
public class ContinuousProfilingStartup {
  private static final Logger LOG = Logger.getLogger(ContinuousProfilingStartup.class);

  void start(@Observes StartupEvent ignored) {
    String endpoint = System.getenv("FLOECAT_PYROSCOPE_ENDPOINT");
    if (endpoint == null || endpoint.isBlank()) {
      endpoint = System.getenv("FLOESCAN_PYROSCOPE_ENDPOINT");
    }
    if (endpoint == null || endpoint.isBlank()) {
      return;
    }

    try {
      Config config =
          new Config.Builder()
              .setApplicationName("floecat-service")
              .setProfilingEvent(EventType.ITIMER)
              .setFormat(Format.JFR)
              .setServerAddress(endpoint)
              .setProfilingAlloc("512k")
              .setAllocLive(true)
              .build();
      PyroscopeAgent.start(config);
      LOG.info("Continuous Pyroscope CPU and live allocation profiling started");
    } catch (RuntimeException e) {
      // Profiling is best effort: an unavailable profiler must not prevent Floecat from serving.
      LOG.warn("Could not start continuous Pyroscope profiling; continuing without it", e);
    }
  }
}
