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
package ai.floedb.floecat.service.concurrent;

import ai.floedb.floecat.service.context.PropagatedContext;
import jakarta.annotation.Priority;
import jakarta.inject.Inject;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InvocationContext;
import java.util.function.BooleanSupplier;

/**
 * Applies the process-wide metadata-I/O ceiling to every {@link BoundMetadataIo} repository read,
 * so admission is enforced at the store instead of opted into by callers. Each read runs on the
 * shared admission pool (tier 2), which the ceiling bounds; the caller's fan-out (tier 1) supplies
 * parallelism, and a cancellable read blocked mid-flight is abandoned promptly on cancel rather
 * than pinning the request thread. Admission is re-entrant, so a read nested in another admitted
 * scope reuses the held permit and runs inline rather than re-dispatching.
 */
@BoundMetadataIo
@Interceptor
@Priority(Interceptor.Priority.LIBRARY_BEFORE)
public class MetadataIoAdmissionInterceptor {

  private static final CancellableCallRunner.FailureMessages ADMISSION_FAILURES =
      new CancellableCallRunner.FailureMessages(
          "metadata read cancelled", "interrupted while awaiting metadata-I/O admission");

  private final MetadataIoRunner admission;

  @Inject
  MetadataIoAdmissionInterceptor(MetadataIoRunner admission) {
    this.admission = admission;
  }

  @AroundInvoke
  Object admit(InvocationContext ctx) throws Exception {
    // The request's cancellation signal, propagated here even on a fan-out worker off the request
    // thread. Present on a cancellable request (e.g. a GetUserObjects stream), absent otherwise.
    BooleanSupplier cancelled = PropagatedContext.currentCancellation();
    try {
      // Repository reads are thread-safe blocking I/O, so run each on the shared admission pool
      // (tier 2) rather than the caller thread: the ceiling bounds the pool, the caller's fan-out
      // (tier 1) supplies parallelism, and — crucially — a cancellable read that blocks mid-flight
      // is abandoned promptly on cancel (the caller returns while the worker unwinds) instead of
      // pinning the request thread. A read that re-enters admission on the pool worker reuses that
      // worker's permit and runs inline (see MetadataIoRunner), so nesting does not re-dispatch.
      return cancelled == null
          ? admission.callWithoutCancellation(() -> proceedUnchecked(ctx), ADMISSION_FAILURES)
          : admission.call(cancelled, () -> proceedUnchecked(ctx), ADMISSION_FAILURES);
    } catch (ProceedFailure wrapped) {
      // Restore the repository's own checked exception, hidden across the Supplier boundary.
      throw wrapped.checkedCause();
    }
  }

  /**
   * Run the intercepted call, tunneling its checked exception through the unchecked Supplier API.
   */
  private static Object proceedUnchecked(InvocationContext ctx) {
    try {
      return ctx.proceed();
    } catch (RuntimeException | Error unchecked) {
      throw unchecked;
    } catch (Exception checked) {
      throw new ProceedFailure(checked);
    }
  }

  /** Carries a checked exception from the intercepted call back across admission's Supplier. */
  private static final class ProceedFailure extends RuntimeException {
    private ProceedFailure(Exception cause) {
      super(cause);
    }

    private Exception checkedCause() {
      return (Exception) getCause();
    }
  }
}
