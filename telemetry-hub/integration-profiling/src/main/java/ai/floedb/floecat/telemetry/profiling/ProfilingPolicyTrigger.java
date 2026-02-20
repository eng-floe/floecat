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

import java.time.Duration;

/** Describes why an automated policy triggered a profiling capture. */
public final class ProfilingPolicyTrigger {
  private final String name;
  private final String signal;
  private final double value;
  private final double threshold;
  private final Duration window;

  public ProfilingPolicyTrigger(
      String name, String signal, double value, double threshold, Duration window) {
    this.name = name;
    this.signal = signal;
    this.value = value;
    this.threshold = threshold;
    this.window = window;
  }

  public String name() {
    return name;
  }

  public String signal() {
    return signal;
  }

  public double value() {
    return value;
  }

  public double threshold() {
    return threshold;
  }

  public Duration window() {
    return window;
  }
}
