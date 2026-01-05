package ai.floedb.floecat.service.common;

import io.grpc.Context;
import java.util.Objects;
import java.util.concurrent.Callable;

/** Utility helpers for capturing and reusing the current gRPC {@link Context} across threads. */
public final class GrpcContextUtil {
  private final Context context;

  private GrpcContextUtil(Context context) {
    this.context = context;
  }

  /** Captures the current gRPC context (principal/correlation id, etc.) for later execution. */
  public static GrpcContextUtil capture() {
    return new GrpcContextUtil(Context.current());
  }

  /** Executes {@code runnable} inside the captured context. */
  public void run(Runnable runnable) {
    Objects.requireNonNull(runnable, "runnable");
    context.run(runnable);
  }

  /**
   * Executes {@code callable} inside the captured context and returns the result.
   *
   * <p>Checked exceptions are wrapped in a {@link RuntimeException}.
   */
  public <T> T call(Callable<T> callable) {
    Objects.requireNonNull(callable, "callable");
    try {
      return context.call(callable);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
