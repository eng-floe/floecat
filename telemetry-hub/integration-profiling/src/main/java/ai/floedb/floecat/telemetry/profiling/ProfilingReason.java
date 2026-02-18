package ai.floedb.floecat.telemetry.profiling;

public enum ProfilingReason {
  DISABLED,
  RATE_LIMIT,
  ALREADY_RUNNING,
  DISK_CAP,
  IO_ERROR,
  UNKNOWN;

  public String tagValue() {
    return name().toLowerCase();
  }
}
