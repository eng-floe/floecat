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

public final class ReadyQueueBackendSupport {
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
    return "";
  }

  static ReconcileReadyQueueStore.ReadyQueueEntry decodeReadyQueueEntry(
      String readyPointerKey,
      String canonicalPointerKey,
      ReconcileReadyQueueBackend.ReadyQueueSlice slice) {
    if (blankToEmpty(readyPointerKey).isBlank() || blankToEmpty(canonicalPointerKey).isBlank()) {
      return null;
    }
    String normalizedKey = normalizePointerKey(readyPointerKey);
    String[] parts = stripLeadingSlash(normalizedKey).split("/");
    try {
      ParsedReadyPointer parsed = parseReadyPointer(parts, slice.indexType());
      if (parsed == null) {
        return null;
      }
      if (!blankToEmpty(slice.filterValue()).equals(blankToEmpty(parsed.filterValue()))) {
        return null;
      }
      return new ReconcileReadyQueueStore.ReadyQueueEntry(
          readyPointerKey,
          canonicalPointerKey,
          parsed.accountId(),
          parsed.jobId(),
          parsed.dueAtMs(),
          slice.indexType(),
          parsed.filterValue());
    } catch (Exception e) {
      return null;
    }
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
    String sortKey =
        String.format(
            "%019d/%s/%s",
            entry.dueAtMs(),
            Keys.encodeSegment(entry.accountId()),
            Keys.encodeSegment(entry.jobId()));
    return new ReadyQueueRow(partitionKey(slice), sortKey, entry);
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
    String[] parts = stripLeadingSlash(readyPointerKey).split("/");
    if (parts.length < 6
        || !"accounts".equals(parts[0])
        || !"reconcile".equals(parts[2])
        || !"jobs".equals(parts[3])
        || !"ready".equals(parts[4])) {
      return null;
    }
    if (parts.length >= 7 && "by-execution-class".equals(parts[5])) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_CLASS, decodeSegment(parts[6]));
    }
    if (parts.length >= 7 && "by-execution-lane".equals(parts[5])) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.EXECUTION_LANE, decodeSegment(parts[6]));
    }
    if (parts.length >= 7 && "by-pinned-executor".equals(parts[5])) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.PINNED_EXECUTOR, decodeSegment(parts[6]));
    }
    if (parts.length >= 7 && "by-job-kind".equals(parts[5])) {
      return new ReconcileReadyQueueBackend.ReadyQueueSlice(
          ReconcileReadyQueueStore.ReadyIndexType.JOB_KIND, decodeSegment(parts[6]));
    }
    return new ReconcileReadyQueueBackend.ReadyQueueSlice(
        ReconcileReadyQueueStore.ReadyIndexType.GLOBAL, "");
  }

  public static long parseDueAtMs(String readyPointerKey) {
    ReconcileReadyQueueBackend.ReadyQueueSlice slice = sliceForReadyPointerKey(readyPointerKey);
    if (slice == null) {
      return INVALID_ORDERED_POINTER_MS;
    }
    String[] parts = stripLeadingSlash(readyPointerKey).split("/");
    ParsedReadyPointer parsed = parseReadyPointer(parts, slice.indexType());
    return parsed == null ? INVALID_ORDERED_POINTER_MS : parsed.dueAtMs();
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

  private static ParsedReadyPointer parseReadyPointer(
      String[] parts, ReconcileReadyQueueStore.ReadyIndexType indexType) {
    try {
      if (parts.length < 8
          || !"accounts".equals(parts[0])
          || !"reconcile".equals(parts[2])
          || !"jobs".equals(parts[3])
          || !"ready".equals(parts[4])) {
        return null;
      }
      String accountId = decodeSegment(parts[1]);
      return switch (indexType) {
        case GLOBAL -> {
          if (parts.length != 8) {
            yield null;
          }
          yield new ParsedReadyPointer(
              accountId, decodeSegment(parts[7]), Long.parseLong(parts[5]), "");
        }
        case EXECUTION_CLASS -> {
          if (parts.length != 9 || !"by-execution-class".equals(parts[5])) {
            yield null;
          }
          yield new ParsedReadyPointer(
              accountId,
              decodeSegment(parts[8]),
              Long.parseLong(parts[7]),
              decodeSegment(parts[6]));
        }
        case EXECUTION_LANE -> {
          if (parts.length != 9 || !"by-execution-lane".equals(parts[5])) {
            yield null;
          }
          yield new ParsedReadyPointer(
              accountId,
              decodeSegment(parts[8]),
              Long.parseLong(parts[7]),
              decodeSegment(parts[6]));
        }
        case PINNED_EXECUTOR -> {
          if (parts.length != 9 || !"by-pinned-executor".equals(parts[5])) {
            yield null;
          }
          yield new ParsedReadyPointer(
              accountId,
              decodeSegment(parts[8]),
              Long.parseLong(parts[7]),
              decodeSegment(parts[6]));
        }
        case JOB_KIND -> {
          if (parts.length != 9 || !"by-job-kind".equals(parts[5])) {
            yield null;
          }
          yield new ParsedReadyPointer(
              accountId,
              decodeSegment(parts[8]),
              Long.parseLong(parts[7]),
              decodeSegment(parts[6]));
        }
      };
    } catch (Exception e) {
      return null;
    }
  }

  private static String decodeSegment(String value) {
    return URLDecoder.decode(blankToEmpty(value), StandardCharsets.UTF_8);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }

  private record ParsedReadyPointer(
      String accountId, String jobId, long dueAtMs, String filterValue) {}
}
