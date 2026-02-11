package ai.floedb.floecat.telemetry;

/** Lifecycle handle for scoped observations. */
public interface ObservationScope extends AutoCloseable {

  /** Marks the scope as a success. */
  void success();

  /** Marks the scope as an error (providing the cause). */
  void error(Throwable throwable);

  /** Signals a retry event inside the scope. */
  void retry();

  @Override
  default void close() {}
}
