package ai.floedb.floecat.telemetry;

/** Defines telemetry validation behavior. */
public enum TelemetryPolicy {
  STRICT,
  LENIENT;

  public boolean isStrict() {
    return this == STRICT;
  }
}
