package ai.floedb.floecat.telemetry;

/** Lifecycle handle for scoped observations. */
public interface ObservationScope extends AutoCloseable {

  /** Marks the scope as a success. */
  void success();

  /** Marks the scope as an error (providing the cause). */
  void error(Throwable throwable);

  /** Signals a retry event inside the scope. */
  void retry();

  /** Adds a runtime status tag (e.g., RPC status) before closing the scope. */
  default void status(String status) {}

  @Override
  default void close() {
    // Closing does not mutate the outcome; callers must call success()/error() explicitly.
  }
}
