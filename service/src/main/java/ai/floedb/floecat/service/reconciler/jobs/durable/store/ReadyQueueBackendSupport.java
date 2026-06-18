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

import ai.floedb.floecat.service.repo.model.Keys;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

final class ReadyQueueBackendSupport {
  private static final long INVALID_ORDERED_POINTER_MS = -1L;
  private static final String READY_GLOBAL_PARTITION = "reconcile-ready#global";
  private static final String READY_EXECUTION_CLASS_PARTITION = "reconcile-ready#execution-class#";
  private static final String READY_EXECUTION_LANE_PARTITION = "reconcile-ready#execution-lane#";
  private static final String READY_PINNED_EXECUTOR_PARTITION = "reconcile-ready#pinned-executor#";
  private static final String READY_JOB_KIND_PARTITION = "reconcile-ready#job-kind#";

  private ReadyQueueBackendSupport() {}

  record ReadyQueueRow(
      String partitionKey, String sortKey, ReconcileReadyQueueStore.ReadyQueueEntry entry) {}

  record ReadyRowCursor(String partitionKey, String sortKey) {}

  static String readyIndexPrefix(ReconcileReadyQueueBackend.ReadyQueueSlice slice) {
    String normalizedFilterValue = blankToEmpty(slice.filterValue());
    return switch (slice.indexType()) {
      case GLOBAL -> Keys.reconcileReadyPointerPrefix();
      case EXECUTION_CLASS ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByExecutionClassPointerPrefix(normalizedFilterValue);
      case EXECUTION_LANE ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByExecutionLanePointerPrefix(normalizedFilterValue);
      case PINNED_EXECUTOR ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByPinnedExecutorPointerPrefix(normalizedFilterValue);
      case JOB_KIND ->
          normalizedFilterValue.isBlank()
              ? ""
              : Keys.reconcileReadyByJobKindPointerPrefix(normalizedFilterValue);
    };
  }

  static ReconcileReadyQueueStore.ReadyQueueEntry decodeReadyQueueEntry(
      String readyPointerKey,
      String canonicalPointerKey,
      ReconcileReadyQueueBackend.ReadyQueueSlice slice) {
    if (blankToEmpty(readyPointerKey).isBlank() || blankToEmpty(canonicalPointerKey).isBlank()) {
      return null;
    }
    ReadyPointerKeyParts parts = parseReadyPointerKeyParts(readyPointerKey, slice);
    if (parts == null) {
      return null;
    }
    return new ReconcileReadyQueueStore.ReadyQueueEntry(
        readyPointerKey,
        canonicalPointerKey,
        parts.accountId(),
        parts.jobId(),
        parts.dueAtMs(),
        slice.indexType(),
        blankToEmpty(slice.filterValue()));
  }

  static ReadyQueueRow toReadyQueueRow(String readyPointerKey, String canonicalPointerKey) {
    ReconcileReadyQueueBackend.ReadyQueueSlice slice = sliceForReadyPointerKey(readyPointerKey);
    if (slice == null) {
      return null;
    }
    ReconcileReadyQueueStore.ReadyQueueEntry entry =
        decodeReadyQueueEntry(readyPointerKey, canonicalPointerKey, slice);
    if (entry == null) {
      return null;
    }
    return new ReadyQueueRow(partitionKey(slice), sortKeyForEntry(entry), entry);
  }

  /**
   * Resolve the storage row (partition + sort key) for a ready pointer from the pointer key alone.
   *
   * <p>The canonical pointer key is a stored attribute, never part of the ready entry's primary
   * key, so locating an entry for deletion must not depend on it. Prune/GC callers cleaning up a
   * leaked pointer often no longer have the canonical pointer it referenced, and {@link
   * #decodeReadyQueueEntry} deliberately rejects a blank canonical, so deletes resolve their key
   * through this path instead. Returns {@code null} only when the pointer key itself is
   * unrecognized or unparseable. The returned entry carries a blank canonical pointer key.
   */
  static ReadyQueueRow toReadyQueueRow(String readyPointerKey) {
    ReconcileReadyQueueBackend.ReadyQueueSlice slice = sliceForReadyPointerKey(readyPointerKey);
    if (slice == null) {
      return null;
    }
    ReadyPointerKeyParts parts = parseReadyPointerKeyParts(readyPointerKey, slice);
    if (parts == null) {
      return null;
    }
    ReconcileReadyQueueStore.ReadyQueueEntry entry =
        new ReconcileReadyQueueStore.ReadyQueueEntry(
            readyPointerKey,
            "",
            parts.accountId(),
            parts.jobId(),
            parts.dueAtMs(),
            slice.indexType(),
            blankToEmpty(slice.filterValue()));
    return new ReadyQueueRow(partitionKey(slice), sortKeyForEntry(entry), entry);
  }

  private static String sortKeyForEntry(ReconcileReadyQueueStore.ReadyQueueEntry entry) {
    return String.format(
        "%019d/%s/%s",
        entry.dueAtMs(),
        Keys.encodeSegment(entry.accountId()),
        Keys.encodeSegment(entry.jobId()));
  }

  private record ReadyPointerKeyParts(String accountId, String jobId, long dueAtMs) {}

  private static ReadyPointerKeyParts parseReadyPointerKeyParts(
      String readyPointerKey, ReconcileReadyQueueBackend.ReadyQueueSlice slice) {
    long dueAt = parseTimestampFromOrderedPointer(readyPointerKey, readyIndexPrefix(slice));
    if (dueAt == INVALID_ORDERED_POINTER_MS) {
      return null;
    }
    String normalizedKey = normalizePointerKey(readyPointerKey);
    String prefix = normalizePointerKey(readyIndexPrefix(slice));
    if (!normalizedKey.startsWith(prefix)) {
      return null;
    }
    String[] parts = normalizedKey.substring(prefix.length()).split("/");
    try {
      String accountId;
      String jobId;
      if (slice.indexType() == ReconcileReadyQueueStore.ReadyIndexType.GLOBAL) {
        if (parts.length != 4) {
          return null;
        }
        accountId = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
        jobId = URLDecoder.decode(parts[3], StandardCharsets.UTF_8);
      } else {
        if (parts.length != 3) {
          return null;
        }
        accountId = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
        jobId = URLDecoder.decode(parts[2], StandardCharsets.UTF_8);
      }
      return new ReadyPointerKeyParts(accountId, jobId, dueAt);
    } catch (Exception e) {
      return null;
    }
  }

  static ReadyQueueRow rowFromNativeReadyItem(
      String readyPointerKey,
      String canonicalPointerKey,
      String partitionKey,
      String sortKey,
      String filterValue,
      String indexType,
      String accountId,
      String jobId,
      long dueAtMs) {
    if (blankToEmpty(partitionKey).isBlank()
        || blankToEmpty(sortKey).isBlank()
        || blankToEmpty(readyPointerKey).isBlank()
        || blankToEmpty(canonicalPointerKey).isBlank()) {
      return null;
    }
    ReconcileReadyQueueStore.ReadyIndexType type;
    try {
      type = ReconcileReadyQueueStore.ReadyIndexType.valueOf(blankToEmpty(indexType));
    } catch (Exception ignored) {
      return null;
    }
    return new ReadyQueueRow(
        partitionKey,
        sortKey,
        new ReconcileReadyQueueStore.ReadyQueueEntry(
            readyPointerKey,
            canonicalPointerKey,
            accountId,
            jobId,
            dueAtMs,
            type,
            blankToEmpty(filterValue)));
  }

  static String partitionKey(ReconcileReadyQueueBackend.ReadyQueueSlice slice) {
    String filterValue = blankToEmpty(slice.filterValue());
    return switch (slice.indexType()) {
      case GLOBAL -> READY_GLOBAL_PARTITION;
      case EXECUTION_CLASS -> READY_EXECUTION_CLASS_PARTITION + filterValue;
      case EXECUTION_LANE -> READY_EXECUTION_LANE_PARTITION + filterValue;
      case PINNED_EXECUTOR -> READY_PINNED_EXECUTOR_PARTITION + filterValue;
      case JOB_KIND -> READY_JOB_KIND_PARTITION + filterValue;
    };
  }

  static ReconcileReadyQueueBackend.ReadyQueueSlice sliceForReadyPointerKey(
      String readyPointerKey) {
    String normalized = normalizePointerKey(readyPointerKey);
    String filterValue =
        extractEncodedFilterValue(
            normalized, normalizePointerKey(Keys.reconcileReadyByExecutionClassPointerPrefix()));
    if (filterValue != null) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_CLASS, filterValue);
    }
    filterValue =
        extractEncodedFilterValue(
            normalized, normalizePointerKey(Keys.reconcileReadyByExecutionLanePointerPrefix()));
    if (filterValue != null) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE, filterValue);
    }
    filterValue =
        extractEncodedFilterValue(
            normalized, normalizePointerKey(Keys.reconcileReadyByPinnedExecutorPointerPrefix()));
    if (filterValue != null) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR, filterValue);
    }
    filterValue =
        extractEncodedFilterValue(
            normalized, normalizePointerKey(Keys.reconcileReadyByJobKindPointerPrefix()));
    if (filterValue != null) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND, filterValue);
    }
    if (normalized.startsWith(normalizePointerKey(Keys.reconcileReadyPointerPrefix()))) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.GLOBAL, "");
    }
    return null;
  }

  static String normalizePointerKey(String key) {
    if (key == null || key.isBlank()) {
      return "";
    }
    return key.startsWith("/") ? key : "/" + key;
  }

  static String stripLeadingSlash(String key) {
    String normalized = normalizePointerKey(key);
    return normalized.startsWith("/") ? normalized.substring(1) : normalized;
  }

  static String encodeCursor(String partitionKey, String sortKey) {
    return blankToEmpty(partitionKey) + "\t" + blankToEmpty(sortKey);
  }

  static ReadyRowCursor decodeCursor(String token) {
    if (blankToEmpty(token).isBlank()) {
      return null;
    }
    int split = token.indexOf('\t');
    if (split < 0) {
      return null;
    }
    return new ReadyRowCursor(token.substring(0, split), token.substring(split + 1));
  }

  private static String extractEncodedFilterValue(String normalizedKey, String prefix) {
    if (!normalizedKey.startsWith(prefix)) {
      return null;
    }
    int slash = normalizedKey.indexOf('/', prefix.length());
    if (slash < 0) {
      return null;
    }
    try {
      return URLDecoder.decode(
          normalizedKey.substring(prefix.length(), slash), StandardCharsets.UTF_8);
    } catch (Exception e) {
      return null;
    }
  }

  private static long parseTimestampFromOrderedPointer(String pointerKey, String prefix) {
    if (pointerKey == null || pointerKey.isBlank()) {
      return INVALID_ORDERED_POINTER_MS;
    }
    String normalizedKey = normalizePointerKey(pointerKey);
    String normalizedPrefix = normalizePointerKey(prefix);
    if (!normalizedKey.startsWith(normalizedPrefix)) {
      return INVALID_ORDERED_POINTER_MS;
    }
    int slash = normalizedKey.indexOf('/', normalizedPrefix.length());
    if (slash < 0) {
      return INVALID_ORDERED_POINTER_MS;
    }
    String token = normalizedKey.substring(normalizedPrefix.length(), slash);
    try {
      return Long.parseLong(token);
    } catch (NumberFormatException nfe) {
      return INVALID_ORDERED_POINTER_MS;
    }
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
