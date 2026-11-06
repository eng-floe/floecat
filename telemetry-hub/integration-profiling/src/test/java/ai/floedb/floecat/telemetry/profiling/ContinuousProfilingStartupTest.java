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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.pyroscope.javaagent.config.Config;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ContinuousProfilingStartupTest {
  private static final String URL = "http://pyroscope:4040";

  @Test
  void startsWhenEnabledWithServerUrl() {
    RecordingLauncher launcher = new RecordingLauncher();
    ContinuousProfilingStartup startup =
        new ContinuousProfilingStartup(config(true, URL, null), "floecat", launcher);

    startup.onStart(new StartupEvent());

    assertEquals(1, launcher.starts.get());
    assertTrue(startup.isStarted());
  }

  @Test
  void doesNotStartWhenDisabled() {
    RecordingLauncher launcher = new RecordingLauncher();
    ContinuousProfilingStartup startup =
        new ContinuousProfilingStartup(config(false, URL, null), "floecat", launcher);

    startup.onStart(new StartupEvent());

    assertEquals(0, launcher.starts.get());
    assertFalse(startup.isStarted());
  }

  @Test
  void doesNotStartWhenServerUrlBlank() {
    RecordingLauncher launcher = new RecordingLauncher();
    ContinuousProfilingStartup startup =
        new ContinuousProfilingStartup(config(true, "  ", null), "floecat", launcher);

    startup.onStart(new StartupEvent());

    assertEquals(0, launcher.starts.get());
    assertFalse(startup.isStarted());
  }

  @Test
  void nativeLoadFailureDoesNotPropagate() {
    RecordingLauncher launcher = new RecordingLauncher();
    launcher.startFailure = new LinkageError("missing async-profiler native library");
    ContinuousProfilingStartup startup =
        new ContinuousProfilingStartup(config(true, URL, null), "floecat", launcher);

    assertDoesNotThrow(() -> startup.onStart(new StartupEvent()));
    assertFalse(startup.isStarted());
  }

  @Test
  void stopsOnShutdownOnlyWhenStarted() {
    RecordingLauncher launcher = new RecordingLauncher();
    ContinuousProfilingStartup startup =
        new ContinuousProfilingStartup(config(true, URL, null), "floecat", launcher);

    // Never started: shutdown is a no-op.
    startup.onStop(new ShutdownEvent());
    assertEquals(0, launcher.stops.get());

    startup.onStart(new StartupEvent());
    startup.onStop(new ShutdownEvent());
    assertEquals(1, launcher.stops.get());
  }

  private static ProfilingConfig config(boolean enabled, String serverUrl, String tenantId) {
    return new ProfilingConfig() {
      @Override
      public boolean enabled() {
        return enabled;
      }

      @Override
      public Optional<String> serverUrl() {
        return Optional.ofNullable(serverUrl);
      }

      @Override
      public Optional<String> tenantId() {
        return Optional.ofNullable(tenantId);
      }
    };
  }

  private static final class RecordingLauncher
      implements ContinuousProfilingStartup.ProfilerLauncher {
    final AtomicInteger starts = new AtomicInteger();
    final AtomicInteger stops = new AtomicInteger();
    Error startFailure;

    @Override
    public void start(Config config) {
      starts.incrementAndGet();
      if (startFailure != null) {
        throw startFailure;
      }
    }

    @Override
    public void stop() {
      stops.incrementAndGet();
    }
  }
}
