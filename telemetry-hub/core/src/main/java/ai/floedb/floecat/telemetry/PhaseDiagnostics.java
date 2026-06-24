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
package ai.floedb.floecat.telemetry;

import java.util.function.Supplier;

/**
 * Request-local phase diagnostics for trace summary events.
 *
 * <p>This is for readable "what happened inside this operation" data. It is not a replacement for
 * operation spans or durable metrics. Implementations should emit one compact event on the current
 * trace span.
 */
public interface PhaseDiagnostics {

  PhaseDiagnostics NOOP =
      new PhaseDiagnostics() {
        @Override
        public Timer timer(String key) {
          return Timer.NOOP;
        }

        @Override
        public void nanos(String key, long nanos) {}

        @Override
        public void count(String key) {}

        @Override
        public void add(String key, long amount) {}

        @Override
        public void put(String key, String value) {}

        @Override
        public void put(String key, long value) {}

        @Override
        public void put(String key, double value) {}

        @Override
        public void put(String key, boolean value) {}

        @Override
        public void emit(String eventName) {}
      };

  Timer timer(String key);

  void nanos(String key, long nanos);

  void count(String key);

  void add(String key, long amount);

  void put(String key, String value);

  void put(String key, long value);

  void put(String key, double value);

  void put(String key, boolean value);

  void emit(String eventName);

  default void count(String key, boolean condition) {
    if (condition) {
      count(key);
    }
  }

  default <T> T time(String key, Supplier<T> supplier) {
    try (Timer ignored = timer(key)) {
      return supplier.get();
    }
  }

  default void time(String key, Runnable runnable) {
    try (Timer ignored = timer(key)) {
      runnable.run();
    }
  }

  interface Timer extends AutoCloseable {
    Timer NOOP = () -> {};

    @Override
    void close();
  }
}
