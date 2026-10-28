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

import io.pyroscope.http.Format;
import io.pyroscope.javaagent.EventType;
import io.pyroscope.javaagent.PyroscopeAgent;
import io.pyroscope.javaagent.config.Config;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Starts continuous JVM profiling to Pyroscope when {@code floecat.profiling.enabled} is set and a
 * server URL is configured. This is Floecat's single profiling workflow (CPU + live allocation); it
 * replaces the former local JFR capture subsystem.
 */
@ApplicationScoped
public class ContinuousProfilingStartup {
  private static final Logger LOG = Logger.getLogger(ContinuousProfilingStartup.class);

  /** Seam over the Pyroscope agent so startup, shutdown, and failure handling stay testable. */
  interface ProfilerLauncher {
    void start(Config config);

    void stop();
  }

  private static final ProfilerLauncher PYROSCOPE_LAUNCHER =
      new ProfilerLauncher() {
        @Override
        public void start(Config config) {
          PyroscopeAgent.start(config);
        }

        @Override
        public void stop() {
          PyroscopeAgent.stop();
        }
      };

  private final ProfilingConfig config;
  private final String applicationName;
  private final ProfilerLauncher launcher;
  private volatile boolean started;

  @Inject
  public ContinuousProfilingStartup(
      ProfilingConfig config,
      @ConfigProperty(name = "quarkus.application.name", defaultValue = "floecat")
          String applicationName) {
    this(config, applicationName, PYROSCOPE_LAUNCHER);
  }

  // Visible for testing: injects a launcher seam so start/stop sequencing and native-load failure
  // handling can be exercised without loading the Pyroscope native agent.
  ContinuousProfilingStartup(
      ProfilingConfig config, String applicationName, ProfilerLauncher launcher) {
    this.config = config;
    this.applicationName = applicationName;
    this.launcher = launcher;
  }

  void onStart(@Observes StartupEvent ignored) {
    if (!config.enabled()) {
      return;
    }
    Optional<String> serverUrl = config.serverUrl().filter(url -> !url.isBlank());
    if (serverUrl.isEmpty()) {
      LOG.info(
          "Continuous profiling enabled but no floecat.profiling.server-url configured; skipping");
      return;
    }
    try {
      Config.Builder builder =
          new Config.Builder()
              .setApplicationName(applicationName)
              .setProfilingEvent(EventType.ITIMER)
              .setFormat(Format.JFR)
              .setServerAddress(serverUrl.get())
              .setProfilingAlloc("512k")
              .setAllocLive(true);
      config.tenantId().filter(id -> !id.isBlank()).ifPresent(builder::setTenantID);
      launcher.start(builder.build());
      started = true;
      LOG.info("Continuous Pyroscope CPU and live allocation profiling started");
    } catch (RuntimeException | LinkageError e) {
      // Best effort: a missing or failed native profiler must never prevent Floecat from serving.
      // Catch LinkageError too — a missing/incompatible agent native library surfaces there, not as
      // a RuntimeException.
      LOG.warn("Could not start continuous Pyroscope profiling; continuing without it", e);
    }
  }

  void onStop(@Observes ShutdownEvent ignored) {
    if (!started) {
      return;
    }
    try {
      launcher.stop();
    } catch (RuntimeException | LinkageError e) {
      LOG.warn("Error stopping continuous Pyroscope profiling", e);
    }
  }

  /** Visible for testing. */
  boolean isStarted() {
    return started;
  }
}
