package ai.floedb.floecat.telemetry.profiling;

public class ProfilingException extends RuntimeException {
  private final ProfilingReason reason;

  public ProfilingException(String message, ProfilingReason reason) {
    super(message);
    this.reason = reason == null ? ProfilingReason.UNKNOWN : reason;
  }

  public ProfilingReason reason() {
    return reason;
  }
}
