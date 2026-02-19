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
