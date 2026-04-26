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

package ai.floedb.floecat.reconciler.spi;

import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * Executes a leased reconcile job.
 *
 * <p>The scheduler owns lease management and terminal state writes. Executors only execute the job
 * body and report progress/results.
 */
public interface ReconcileExecutor {
  String id();

  default boolean enabled() {
    return true;
  }

  default int priority() {
    return 100;
  }

  default boolean supports(ReconcileJobStore.LeasedJob lease) {
    return true;
  }

  default Set<ReconcileExecutionClass> supportedExecutionClasses() {
    return EnumSet.allOf(ReconcileExecutionClass.class);
  }

  default Set<ReconcileJobKind> supportedJobKinds() {
    return EnumSet.allOf(ReconcileJobKind.class);
  }

  default Set<String> supportedLanes() {
    return Set.of("");
  }

  default boolean supportsExecutionClass(ReconcileExecutionClass executionClass) {
    return supportedExecutionClasses().contains(executionClass);
  }

  default boolean supportsJobKind(ReconcileJobKind jobKind) {
    return supportedJobKinds()
        .contains(jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind);
  }

  default boolean supportsLane(String lane) {
    return supportedLanes().contains(lane == null ? "" : lane.trim());
  }

  ExecutionResult execute(ExecutionContext context);

  @FunctionalInterface
  interface ProgressListener {
    void onProgress(
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        String message);
  }

  record ExecutionContext(
      ReconcileJobStore.LeasedJob lease,
      BooleanSupplier shouldStop,
      ProgressListener progressListener) {
    public ExecutionContext {
      lease = Objects.requireNonNull(lease, "lease");
      shouldStop = Objects.requireNonNull(shouldStop, "shouldStop");
      progressListener = Objects.requireNonNull(progressListener, "progressListener");
    }
  }

  final class ExecutionResult {
    public enum FailureKind {
      NONE,
      CONNECTOR_MISSING,
      TABLE_MISSING,
      INTERNAL
    }

    public final long tablesScanned;
    public final long tablesChanged;
    public final long viewsScanned;
    public final long viewsChanged;
    public final long scanned;
    public final long changed;
    public final long errors;
    public final long snapshotsProcessed;
    public final long statsProcessed;
    public final boolean cancelled;
    public final FailureKind failureKind;
    public final String message;
    public final Exception error;

    private ExecutionResult(
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        boolean cancelled,
        FailureKind failureKind,
        String message,
        Exception error) {
      this.tablesScanned = tablesScanned;
      this.tablesChanged = tablesChanged;
      this.viewsScanned = viewsScanned;
      this.viewsChanged = viewsChanged;
      this.scanned = tablesScanned + viewsScanned;
      this.changed = tablesChanged + viewsChanged;
      this.errors = errors;
      this.snapshotsProcessed = snapshotsProcessed;
      this.statsProcessed = statsProcessed;
      this.cancelled = cancelled;
      this.failureKind = failureKind == null ? FailureKind.NONE : failureKind;
      this.message = message == null ? "" : message;
      this.error = error;
    }

    public static ExecutionResult success(
        long tablesScanned,
        long tablesChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        String message) {
      return success(
          tablesScanned, tablesChanged, 0, 0, errors, snapshotsProcessed, statsProcessed, message);
    }

    public static ExecutionResult success(
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        String message) {
      return new ExecutionResult(
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed,
          false,
          FailureKind.NONE,
          message,
          null);
    }

    public static ExecutionResult cancelled(
        long tablesScanned,
        long tablesChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        String message) {
      return cancelled(
          tablesScanned, tablesChanged, 0, 0, errors, snapshotsProcessed, statsProcessed, message);
    }

    public static ExecutionResult cancelled(
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        String message) {
      return new ExecutionResult(
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed,
          true,
          FailureKind.NONE,
          message,
          null);
    }

    public static ExecutionResult failure(
        long tablesScanned,
        long tablesChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        String message,
        Exception error) {
      return failure(
          tablesScanned,
          tablesChanged,
          0,
          0,
          errors,
          snapshotsProcessed,
          statsProcessed,
          message,
          error);
    }

    public static ExecutionResult failure(
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        String message,
        Exception error) {
      return new ExecutionResult(
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed,
          false,
          FailureKind.INTERNAL,
          message,
          error);
    }

    public static ExecutionResult failure(
        long tablesScanned,
        long tablesChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        FailureKind failureKind,
        String message,
        Exception error) {
      return failure(
          tablesScanned,
          tablesChanged,
          0,
          0,
          errors,
          snapshotsProcessed,
          statsProcessed,
          failureKind,
          message,
          error);
    }

    public static ExecutionResult failure(
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        FailureKind failureKind,
        String message,
        Exception error) {
      return new ExecutionResult(
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed,
          false,
          failureKind,
          message,
          error);
    }

    public boolean ok() {
      return !cancelled && error == null;
    }
  }
}
