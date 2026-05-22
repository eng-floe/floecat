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

package ai.floedb.floecat.service.reconciler.jobs.durable.queue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeaseRequest;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeasedJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileReadyQueue {
  private static final Logger LOG = Logger.getLogger(ReconcileReadyQueue.class);
  private static final long INVALID_ORDERED_POINTER_MS = -1L;

  @FunctionalInterface
  public interface RepairLookupPointer {
    boolean apply(
        String jobId, String canonicalPointerKey, String repairPhase, String repairReason);
  }

  @FunctionalInterface
  public interface RepairReadyPointer {
    boolean apply(
        String canonicalPointerKey,
        StoredReconcileJob record,
        String repairPhase,
        String repairReason);
  }

  @FunctionalInterface
  public interface RepairCanonicalPointersIfNeeded {
    StoredReconcileJob apply(
        String canonicalPointerKey,
        StoredReconcileJob record,
        String repairPhase,
        String repairReason);
  }

  @FunctionalInterface
  public interface DeferRepairIfHotPath {
    boolean apply(
        String repairPhase,
        String repairReason,
        String repairTargetKind,
        String canonicalPointerKey,
        String jobId,
        String state);
  }

  public enum ReadyIndexType {
    GLOBAL,
    EXECUTION_CLASS,
    EXECUTION_LANE,
    PINNED_EXECUTOR,
    JOB_KIND
  }

  private record ReadyIndexSelection(String prefix, ReadyIndexType type, String filterValue) {}

  private record ReadyPointerTarget(
      String canonicalPointerKey,
      String accountId,
      String jobId,
      long dueAtMs,
      ReadyIndexType indexType,
      String filterValue) {}

  public static final class LeaseScanStats {
    public int scanCount;
    public int candidateCount;
  }

  private PointerStore pointerStore;
  private ReconcilePayloadStore payloadStore;
  private ReconcileLeaseManager leaseManager;
  private int readyScanLimit;
  private Function<String, Optional<StoredReconcileJob>> readCanonicalRecordByKey;
  private Function<Pointer, Optional<StoredReconcileJob>> readRecord;
  private Function<StoredReconcileJob, List<String>> statePointerKeys;
  private BiPredicate<String, String> hasValidLookupPointer;
  private RepairLookupPointer repairLookupPointer;
  private BiPredicate<StoredReconcileJob, String> hasValidReadyPointers;
  private RepairReadyPointer repairReadyPointer;
  private RepairCanonicalPointersIfNeeded repairCanonicalPointersIfNeeded;
  private DeferRepairIfHotPath deferRepairIfHotPath;
  private BiConsumer<StoredReconcileJob, String> clearReadyPointersIfOwned;
  private Predicate<StoredReconcileJob> requiresReadyPointer;
  private Predicate<String> isTerminalState;

  public void bind(
      PointerStore pointerStore,
      ReconcilePayloadStore payloadStore,
      ReconcileLeaseManager leaseManager,
      int readyScanLimit,
      Function<String, Optional<StoredReconcileJob>> readCanonicalRecordByKey,
      Function<Pointer, Optional<StoredReconcileJob>> readRecord,
      Function<StoredReconcileJob, List<String>> statePointerKeys,
      BiPredicate<String, String> hasValidLookupPointer,
      RepairLookupPointer repairLookupPointer,
      BiPredicate<StoredReconcileJob, String> hasValidReadyPointers,
      RepairReadyPointer repairReadyPointer,
      RepairCanonicalPointersIfNeeded repairCanonicalPointersIfNeeded,
      DeferRepairIfHotPath deferRepairIfHotPath,
      BiConsumer<StoredReconcileJob, String> clearReadyPointersIfOwned,
      Predicate<StoredReconcileJob> requiresReadyPointer,
      Predicate<String> isTerminalState) {
    this.pointerStore = pointerStore;
    this.payloadStore = payloadStore;
    this.leaseManager = leaseManager;
    this.readyScanLimit = readyScanLimit;
    this.readCanonicalRecordByKey = readCanonicalRecordByKey;
    this.readRecord = readRecord;
    this.statePointerKeys = statePointerKeys;
    this.hasValidLookupPointer = hasValidLookupPointer;
    this.repairLookupPointer = repairLookupPointer;
    this.hasValidReadyPointers = hasValidReadyPointers;
    this.repairReadyPointer = repairReadyPointer;
    this.repairCanonicalPointersIfNeeded = repairCanonicalPointersIfNeeded;
    this.deferRepairIfHotPath = deferRepairIfHotPath;
    this.clearReadyPointersIfOwned = clearReadyPointersIfOwned;
    this.requiresReadyPointer = requiresReadyPointer;
    this.isTerminalState = isTerminalState;
  }

  public Optional<StoredReconcileJob> readCurrentRecordFromIndexPointer(Pointer indexPointer) {
    if (indexPointer == null || blank(indexPointer.getBlobUri())) {
      return Optional.empty();
    }
    String canonicalPointerKey = indexPointer.getBlobUri();
    Pointer canonicalPointer = pointerStore.get(canonicalPointerKey).orElse(null);
    if (canonicalPointer == null) {
      pointerStore.compareAndDelete(indexPointer.getKey(), indexPointer.getVersion());
      return Optional.empty();
    }
    var canonicalRecord = readRecord.apply(canonicalPointer);
    if (canonicalRecord.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(canonicalRecord.get());
  }

  public Optional<StoredReconcileJob> readCurrentRecordFromStateIndexPointer(
      Pointer indexPointer, Predicate<StoredReconcileJob> filter) {
    var current = readCurrentRecordFromIndexPointer(indexPointer);
    if (current.isEmpty()) {
      return Optional.empty();
    }
    if (filter == null || filter.test(current.get())) {
      return current;
    }
    if (!statePointerKeys.apply(current.get()).contains(indexPointer.getKey())) {
      pointerStore.compareAndDelete(indexPointer.getKey(), indexPointer.getVersion());
    }
    return Optional.empty();
  }

  public Optional<StoredReconcileJob> loadActiveFromDedupe(String dedupePointerKey) {
    Pointer dedupePointer = pointerStore.get(dedupePointerKey).orElse(null);
    if (dedupePointer == null) {
      return Optional.empty();
    }

    String canonicalPointerKey = dedupePointer.getBlobUri();
    Pointer canonicalPointer = pointerStore.get(canonicalPointerKey).orElse(null);
    if (canonicalPointer == null) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }

    var canonicalRecordOpt = readRecord.apply(canonicalPointer);
    if (canonicalRecordOpt.isEmpty()) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }
    StoredReconcileJob record = canonicalRecordOpt.get();

    if (Boolean.TRUE.equals(isTerminalState.test(record.state))) {
      pointerStore.compareAndDelete(dedupePointerKey, dedupePointer.getVersion());
      return Optional.empty();
    }

    record =
        repairCanonicalPointersIfNeeded.apply(
            canonicalPointerKey, record, "active_dedupe_lookup", "canonical_pointer_drift");
    if (Boolean.TRUE.equals(requiresReadyPointer.test(record))
        && !hasValidReadyPointers.test(record, canonicalPointerKey)
        && !repairReadyPointer.apply(
            canonicalPointerKey, record, "active_dedupe_lookup", "ready_pointer_missing")) {
      LOG.warnf(
          "Active reconcile job %s state=%s has invalid reconcile ready pointers and repair failed",
          record.jobId, record.state);
    }
    if (!hasValidLookupPointer.test(record.jobId, canonicalPointerKey)
        && !repairLookupPointer.apply(
            record.jobId, canonicalPointerKey, "active_dedupe_lookup", "lookup_pointer_missing")) {
      LOG.warnf(
          "Active reconcile job %s state=%s has no valid lookup pointer and repair failed",
          record.jobId, record.state);
    }

    return Optional.of(record);
  }

  public Optional<LeasedJob> leaseReadyDue(long nowMs, LeaseRequest request) {
    return leaseReadyDue(nowMs, request, null);
  }

  public Optional<LeasedJob> leaseReadyDue(
      long nowMs, LeaseRequest request, LeaseScanStats scanStats) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    for (ReadyIndexSelection selection : readyScanSelections(effective)) {
      Optional<LeasedJob> leased = leaseReadyDueFromPrefix(nowMs, effective, selection, scanStats);
      if (leased.isPresent()) {
        return leased;
      }
    }
    return Optional.empty();
  }

  public boolean matchesLeaseRequest(StoredReconcileJob record, LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    String pinnedExecutorId = record == null ? "" : record.pinnedExecutorId();
    if (!blank(pinnedExecutorId) && !effective.executorIds.contains(pinnedExecutorId)) {
      return false;
    }
    return record != null
        && effective.matches(record.executionPolicy(), pinnedExecutorId, record.jobKind());
  }

  public long readyPointerDueAt(StoredReconcileJob record) {
    return record != null && record.nextAttemptAtMs > 0L
        ? record.nextAttemptAtMs
        : System.currentTimeMillis();
  }

  public String readyPointerKeyFor(StoredReconcileJob record, long dueAtMs) {
    return readyPointerKeyForDue(record.accountId, record.laneKey, record.jobId, dueAtMs);
  }

  public String readyPointerKeyForDue(
      String accountId, String laneKey, String jobId, long dueAtMs) {
    return Keys.reconcileReadyPointerByDue(dueAtMs, accountId, laneKey, jobId);
  }

  public String readyPointerKeyFor(
      StoredReconcileJob record, ReadyIndexType indexType, long dueAtMs, String filterValue) {
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

  public List<String> readyPointerKeys(StoredReconcileJob record) {
    if (record == null || !Boolean.TRUE.equals(requiresReadyPointer.test(record))) {
      return List.of();
    }
    long dueAtMs = readyPointerDueAt(record);
    ReconcileExecutionPolicy executionPolicy = record.executionPolicy();
    List<String> readyKeys = new ArrayList<>();
    readyKeys.add(readyPointerKeyFor(record, dueAtMs));
    String executionClassReadyKey =
        readyPointerKeyFor(
            record,
            ReadyIndexType.EXECUTION_CLASS,
            dueAtMs,
            executionPolicy.executionClass().name());
    if (!executionClassReadyKey.isBlank()) {
      readyKeys.add(executionClassReadyKey);
    }
    String executionLaneReadyKey =
        readyPointerKeyFor(record, ReadyIndexType.EXECUTION_LANE, dueAtMs, executionPolicy.lane());
    if (!executionLaneReadyKey.isBlank()) {
      readyKeys.add(executionLaneReadyKey);
    }
    if (!blank(record.pinnedExecutorId())) {
      String pinnedReadyKey =
          readyPointerKeyFor(
              record, ReadyIndexType.PINNED_EXECUTOR, dueAtMs, record.pinnedExecutorId());
      if (!pinnedReadyKey.isBlank()) {
        readyKeys.add(pinnedReadyKey);
      }
    }
    String kindReadyKey =
        readyPointerKeyFor(record, ReadyIndexType.JOB_KIND, dueAtMs, record.jobKind().name());
    if (!kindReadyKey.isBlank()) {
      readyKeys.add(kindReadyKey);
    }
    return readyKeys;
  }

  public void repairReadyPointersIfStillCurrent(String canonicalPointerKey) {
    var current = readCanonicalRecordByKey.apply(canonicalPointerKey);
    current.ifPresent(
        record -> {
          if (Boolean.TRUE.equals(requiresReadyPointer.test(record))) {
            if (deferRepairIfHotPath.apply(
                "lease_conflict",
                "ready_pointer_missing_after_lease_conflict",
                "ready_pointer",
                canonicalPointerKey,
                record.jobId,
                record.state)) {
              return;
            }
            repairReadyPointer.apply(
                canonicalPointerKey,
                record,
                "lease_conflict",
                "ready_pointer_missing_after_lease_conflict");
          }
        });
  }

  private Optional<LeasedJob> leaseReadyDueFromPrefix(
      long nowMs, LeaseRequest request, ReadyIndexSelection selection, LeaseScanStats scanStats) {
    String token = "";
    int pages = 0;
    while (true) {
      if (scanStats != null) {
        scanStats.scanCount++;
      }
      StringBuilder next = new StringBuilder();
      List<Pointer> ready =
          pointerStore.listPointersByPrefix(selection.prefix(), readyScanLimit, token, next);
      if (ready.isEmpty()) {
        return Optional.empty();
      }

      for (Pointer candidate : ready) {
        if (scanStats != null) {
          scanStats.candidateCount++;
        }
        var readyTarget = decodeReadyPointerTarget(candidate.getKey(), selection);
        if (readyTarget == null) {
          if (!shouldSkipMalformedReadyPointer(candidate.getKey(), selection)) {
            pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
          }
          continue;
        }
        if (readyTarget.dueAtMs() > nowMs) {
          return Optional.empty();
        }
        if (!readyTarget.canonicalPointerKey().equals(candidate.getBlobUri())) {
          pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
          continue;
        }
        Pointer canonicalPointer = pointerStore.get(readyTarget.canonicalPointerKey()).orElse(null);
        if (canonicalPointer == null) {
          pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
          continue;
        }
        var recordOpt = readRecord.apply(canonicalPointer);
        if (recordOpt.isEmpty()) {
          pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
          continue;
        }
        StoredReconcileJob record = recordOpt.get();
        if ("JS_WAITING".equals(record.state)) {
          clearReadyPointersIfOwned.accept(record, readyTarget.canonicalPointerKey());
          pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
          continue;
        }
        if (!readyPointerMatchesRecord(candidate.getKey(), readyTarget, record)) {
          pointerStore.compareAndDelete(candidate.getKey(), candidate.getVersion());
          continue;
        }
        if (!hasValidLookupPointer.test(record.jobId, readyTarget.canonicalPointerKey())) {
          repairLookupPointer.apply(
              record.jobId,
              readyTarget.canonicalPointerKey(),
              "ready_scan",
              "lookup_pointer_missing");
        }
        if (!matchesLeaseRequest(record, request)) {
          continue;
        }
        if (!leaseManager.tryAcquireLaneLease(record, readyTarget.canonicalPointerKey(), nowMs)) {
          continue;
        }
        var leased =
            leaseManager.leaseCanonical(
                readyTarget.canonicalPointerKey(),
                candidate.getKey(),
                nowMs,
                canonicalPointer,
                record);
        if (leased.isPresent()) {
          return leased;
        }
        restoreReadyPointerIfStillCurrent(candidate.getKey(), readyTarget);
        leaseManager.clearLaneLeaseIfOwned(record, readyTarget.canonicalPointerKey());
      }

      String nextToken = next.toString();
      if (nextToken.isBlank()) {
        return Optional.empty();
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile ready pagination token did not advance; aborting ready scan to avoid"
                + " livelock");
        return Optional.empty();
      }
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile ready pagination hit safety page cap; aborting scan");
        return Optional.empty();
      }
    }
  }

  private List<ReadyIndexSelection> readyScanSelections(LeaseRequest request) {
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    List<ReadyIndexSelection> selections = new ArrayList<>();

    List<String> executorIds =
        effective.executorIds.stream()
            .sorted()
            .filter(executorId -> !executorId.isBlank())
            .toList();
    for (String executorId : executorIds) {
      selections.add(
          new ReadyIndexSelection(
              Keys.reconcileReadyByPinnedExecutorPointerPrefix(executorId),
              ReadyIndexType.PINNED_EXECUTOR,
              executorId));
    }

    if (!effective.lanes.isEmpty() && !effective.lanes.contains(LeaseRequest.anyLaneToken())) {
      effective.lanes.stream()
          .sorted()
          .filter(lane -> !lane.isBlank())
          .forEach(
              lane ->
                  selections.add(
                      new ReadyIndexSelection(
                          Keys.reconcileReadyByExecutionLanePointerPrefix(lane),
                          ReadyIndexType.EXECUTION_LANE,
                          lane)));
    }

    if (!effective.jobKinds.isEmpty()) {
      effective.jobKinds.stream()
          .map(Enum::name)
          .sorted()
          .forEach(
              jobKind ->
                  selections.add(
                      new ReadyIndexSelection(
                          Keys.reconcileReadyByJobKindPointerPrefix(jobKind),
                          ReadyIndexType.JOB_KIND,
                          jobKind)));
    }

    if (!effective.executionClasses.isEmpty()) {
      effective.executionClasses.stream()
          .map(Enum::name)
          .sorted()
          .forEach(
              executionClass ->
                  selections.add(
                      new ReadyIndexSelection(
                          Keys.reconcileReadyByExecutionClassPointerPrefix(executionClass),
                          ReadyIndexType.EXECUTION_CLASS,
                          executionClass)));
    }

    selections.add(
        new ReadyIndexSelection(Keys.reconcileReadyPointerPrefix(), ReadyIndexType.GLOBAL, ""));
    return selections;
  }

  private ReadyPointerTarget decodeReadyPointerTarget(
      String readyPointerKey, ReadyIndexSelection selection) {
    long dueAt = parseTimestampFromOrderedPointer(readyPointerKey, selection.prefix());
    if (dueAt == INVALID_ORDERED_POINTER_MS) {
      return null;
    }
    String normalizedKey = normalizePointerKey(readyPointerKey);
    String prefix = normalizePointerKey(selection.prefix());
    if (!normalizedKey.startsWith(prefix)) {
      return null;
    }
    String[] parts = normalizedKey.substring(prefix.length()).split("/");
    try {
      String accountId;
      String jobId;
      if (selection.type() == ReadyIndexType.GLOBAL) {
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
      return new ReadyPointerTarget(
          Keys.reconcileJobStateRowById(accountId, jobId),
          accountId,
          jobId,
          dueAt,
          selection.type(),
          selection.filterValue());
    } catch (Exception e) {
      return null;
    }
  }

  private boolean readyPointerMatchesRecord(
      String candidateKey, ReadyPointerTarget target, StoredReconcileJob record) {
    if (record == null || target == null || candidateKey == null || candidateKey.isBlank()) {
      return false;
    }
    if ("JS_WAITING".equals(record.state)) {
      return false;
    }
    if (!Boolean.TRUE.equals(requiresReadyPointer.test(record))) {
      return false;
    }
    if (!target.accountId().equals(record.accountId) || !target.jobId().equals(record.jobId)) {
      return false;
    }
    if (record.nextAttemptAtMs != target.dueAtMs()) {
      return false;
    }
    if (!readyIndexFilterMatchesRecord(target, record)) {
      return false;
    }
    String expectedKey =
        readyPointerKeyFor(record, target.indexType(), target.dueAtMs(), target.filterValue());
    if (!candidateKey.equals(expectedKey)) {
      return false;
    }
    return target.indexType() != ReadyIndexType.GLOBAL
        || candidateKey.equals(record.readyPointerKey);
  }

  private boolean readyIndexFilterMatchesRecord(
      ReadyPointerTarget target, StoredReconcileJob record) {
    if (target == null || record == null) {
      return false;
    }
    ReconcileExecutionPolicy policy = record.executionPolicy();
    return switch (target.indexType()) {
      case GLOBAL -> true;
      case EXECUTION_CLASS -> target.filterValue().equals(policy.executionClass().name());
      case EXECUTION_LANE -> target.filterValue().equals(policy.lane());
      case PINNED_EXECUTOR -> target.filterValue().equals(record.pinnedExecutorId());
      case JOB_KIND -> target.filterValue().equals(record.jobKind().name());
    };
  }

  private void restoreReadyPointerIfStillCurrent(
      String readyPointerKey, ReadyPointerTarget target) {
    if (readyPointerKey == null || readyPointerKey.isBlank() || target == null) {
      return;
    }
    Pointer existing = pointerStore.get(readyPointerKey).orElse(null);
    if (existing != null && target.canonicalPointerKey().equals(existing.getBlobUri())) {
      return;
    }
    var current = readCanonicalRecordByKey.apply(target.canonicalPointerKey());
    if (current.isEmpty() || !readyPointerMatchesRecord(readyPointerKey, target, current.get())) {
      return;
    }
    repairReadyPointer.apply(target.canonicalPointerKey(), current.get(), "", "");
  }

  private boolean shouldSkipMalformedReadyPointer(
      String readyPointerKey, ReadyIndexSelection selection) {
    if (blank(readyPointerKey) || selection == null) {
      return false;
    }
    if (selection.type() != ReadyIndexType.GLOBAL) {
      return false;
    }
    return readyPointerKey.startsWith(Keys.reconcileReadyByExecutionClassPointerPrefix())
        || readyPointerKey.startsWith(Keys.reconcileReadyByExecutionLanePointerPrefix())
        || readyPointerKey.startsWith(Keys.reconcileReadyByPinnedExecutorPointerPrefix())
        || readyPointerKey.startsWith(Keys.reconcileReadyByJobKindPointerPrefix());
  }

  private long parseDueMillis(String readyPointerKey) {
    return parseTimestampFromOrderedPointer(readyPointerKey, Keys.reconcileReadyPointerPrefix());
  }

  private long parseTimestampFromOrderedPointer(String pointerKey, String prefix) {
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

  private static String normalizePointerKey(String key) {
    if (key == null || key.isBlank()) {
      return "/";
    }
    return key.startsWith("/") ? key : "/" + key;
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
