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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.repo.model.Keys;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public final class ReadyQueueKeys {
  private ReadyQueueKeys() {}

  public static long readyPointerDueAt(StoredReconcileJob record) {
    return record != null && record.nextAttemptAtMs > 0L
        ? record.nextAttemptAtMs
        : System.currentTimeMillis();
  }

  public static String readyPointerKeyFor(StoredReconcileJob record, long dueAtMs) {
    return readyPointerKeyForDue(record.accountId, record.laneKey, record.jobId, dueAtMs);
  }

  public static String readyPointerKeyForDue(
      String accountId, String laneKey, String jobId, long dueAtMs) {
    return Keys.reconcileReadyPointerByDue(dueAtMs, accountId, laneKey, jobId);
  }

  public static String readyPointerKeyFor(
      StoredReconcileJob record,
      ReconcileReadyQueueStore.ReadyIndexType indexType,
      long dueAtMs,
      String filterValue) {
    if (record == null) {
      return "";
    }
    String normalizedFilterValue = blankToEmpty(filterValue);
    return switch (indexType) {
      case GLOBAL -> readyPointerKeyFor(record, dueAtMs);
      case EXECUTION_CLASS ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByExecutionClassPointerByDue(
                  dueAtMs, normalizedFilterValue, record.accountId, record.jobId);
      case EXECUTION_LANE ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByExecutionLanePointerByDue(
                  dueAtMs, normalizedFilterValue, record.accountId, record.jobId);
      case PINNED_EXECUTOR ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByPinnedExecutorPointerByDue(
                  dueAtMs, normalizedFilterValue, record.accountId, record.jobId);
      case JOB_KIND ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByJobKindPointerByDue(
                  dueAtMs, normalizedFilterValue, record.accountId, record.jobId);
    };
  }

  public static List<String> readyPointerKeys(
      StoredReconcileJob record, Predicate<StoredReconcileJob> requiresReadyPointer) {
    return readyPointerKeys(record, readyPointerDueAt(record), requiresReadyPointer);
  }

  public static List<String> readyPointerKeys(
      StoredReconcileJob record, long dueAtMs, Predicate<StoredReconcileJob> requiresReadyPointer) {
    if (record == null
        || requiresReadyPointer == null
        || !Boolean.TRUE.equals(requiresReadyPointer.test(record))
        || dueAtMs <= 0L) {
      return List.of();
    }
    List<String> keys = new ArrayList<>();
    String pinnedExecutorId = record.pinnedExecutorId();
    if (!blank(pinnedExecutorId)) {
      String pinnedExecutorKey =
          readyPointerKeyFor(
              record,
              ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR,
              dueAtMs,
              pinnedExecutorId);
      return pinnedExecutorKey.isBlank() ? List.of() : List.of(pinnedExecutorKey);
    }
    keys.add(readyPointerKeyFor(record, dueAtMs));
    String executionClassKey =
        readyPointerKeyFor(
            record,
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_CLASS,
            dueAtMs,
            record.executionPolicy().executionClass().name());
    if (!executionClassKey.isBlank()) {
      keys.add(executionClassKey);
    }
    String executionLaneKey =
        readyPointerKeyFor(
            record,
            ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE,
            dueAtMs,
            record.laneKey);
    if (!executionLaneKey.isBlank()) {
      keys.add(executionLaneKey);
    }
    String jobKindKey =
        readyPointerKeyFor(
            record,
            ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND,
            dueAtMs,
            record.jobKind().name());
    if (!jobKindKey.isBlank()) {
      keys.add(jobKindKey);
    }
    return List.copyOf(keys);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
