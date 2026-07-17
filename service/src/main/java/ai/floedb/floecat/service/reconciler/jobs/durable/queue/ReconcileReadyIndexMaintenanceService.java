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

import ai.floedb.floecat.common.rpc.PointerReferenceKind;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileReadyIndexMaintenanceService {
  private static final Logger LOG = Logger.getLogger(ReconcileReadyIndexMaintenanceService.class);
  private static final String QUEUED_STATE = "JS_QUEUED";
  public static final int CURRENT_READY_INDEX_VERSION = 2;

  private ReconcileJobIndexStore jobIndexStore;
  private ReconcileReadyQueueStore readyQueueStore;
  private int readyScanLimit;

  private volatile String queuedStateScanToken = "";

  public void bind(
      ReconcileJobIndexStore jobIndexStore,
      ReconcileReadyQueueStore readyQueueStore,
      int readyScanLimit) {
    this.jobIndexStore = jobIndexStore;
    this.readyQueueStore = readyQueueStore;
    this.readyScanLimit = Math.max(1, readyScanLimit);
  }

  public void runReadyIndexMaintenanceOnce(long maxMillis) {
    long startedAtMs = System.currentTimeMillis();
    long deadlineMs = maxMillis <= 0L ? startedAtMs : startedAtMs + Math.max(1L, maxMillis);
    ReadyIndexRepairStats stats = repairQueuedReadyIndexes(deadlineMs);
    logMaintenanceSummary(startedAtMs, stats);
  }

  private ReadyIndexRepairStats repairQueuedReadyIndexes(long deadlineMs) {
    if (jobIndexStore == null || readyQueueStore == null) {
      return ReadyIndexRepairStats.empty();
    }
    String token = blankToEmpty(queuedStateScanToken);
    int pages = 0;
    int scanned = 0;
    int jobsRepaired = 0;
    int readyWrites = 0;
    int chunks = 0;
    int failedChunks = 0;
    List<ReadyIndexRepairWork> pendingWork = new ArrayList<>();
    while (true) {
      if (System.currentTimeMillis() > deadlineMs) {
        RepairFlushResult flush = flushReadyRepairChunks(pendingWork);
        return new ReadyIndexRepairStats(
            false,
            pages,
            scanned,
            jobsRepaired + flush.jobsRepaired(),
            readyWrites + flush.readyWrites(),
            chunks + flush.chunks(),
            failedChunks + flush.failedChunks());
      }
      ReconcileJobIndexStore.StoredJobPage page =
          jobIndexStore.listStoredJobsInState(QUEUED_STATE, readyScanLimit, token);
      if (page.records().isEmpty()) {
        RepairFlushResult flush = flushReadyRepairChunks(pendingWork);
        queuedStateScanToken = "";
        return new ReadyIndexRepairStats(
            true,
            pages,
            scanned,
            jobsRepaired + flush.jobsRepaired(),
            readyWrites + flush.readyWrites(),
            chunks + flush.chunks(),
            failedChunks + flush.failedChunks());
      }
      for (StoredReconcileJob record : page.records()) {
        if (System.currentTimeMillis() > deadlineMs) {
          RepairFlushResult flush = flushReadyRepairChunks(pendingWork);
          return new ReadyIndexRepairStats(
              false,
              pages,
              scanned,
              jobsRepaired + flush.jobsRepaired(),
              readyWrites + flush.readyWrites(),
              chunks + flush.chunks(),
              failedChunks + flush.failedChunks());
        }
        scanned++;
        ReadyIndexRepairWork work = buildRepairWork(record);
        if (work == null) {
          continue;
        }
        if (work.writeItems() > maxWriteItems()) {
          RepairFlushResult flush = flushReadyRepairChunks(pendingWork);
          jobsRepaired += flush.jobsRepaired();
          readyWrites += flush.readyWrites();
          chunks += flush.chunks();
          failedChunks += flush.failedChunks();
          pendingWork.clear();
          RepairFlushResult oversizedFlush = flushOversizedReadyRepair(work);
          jobsRepaired += oversizedFlush.jobsRepaired();
          readyWrites += oversizedFlush.readyWrites();
          chunks += oversizedFlush.chunks();
          failedChunks += oversizedFlush.failedChunks();
          continue;
        }
        pendingWork.add(work);
      }

      RepairFlushResult pageFlush = flushReadyRepairChunks(pendingWork);
      jobsRepaired += pageFlush.jobsRepaired();
      readyWrites += pageFlush.readyWrites();
      chunks += pageFlush.chunks();
      failedChunks += pageFlush.failedChunks();
      pendingWork.clear();

      String nextToken = blankToEmpty(page.nextPageToken());
      if (nextToken.isBlank()) {
        RepairFlushResult flush = flushReadyRepairChunks(pendingWork);
        queuedStateScanToken = "";
        return new ReadyIndexRepairStats(
            true,
            pages + 1,
            scanned,
            jobsRepaired + flush.jobsRepaired(),
            readyWrites + flush.readyWrites(),
            chunks + flush.chunks(),
            failedChunks + flush.failedChunks());
      }
      if (nextToken.equals(token)) {
        RepairFlushResult flush = flushReadyRepairChunks(pendingWork);
        LOG.warn(
            "Reconcile ready-index repair pagination token did not advance; aborting scan to avoid"
                + " livelock");
        queuedStateScanToken = "";
        return new ReadyIndexRepairStats(
            true,
            pages + 1,
            scanned,
            jobsRepaired + flush.jobsRepaired(),
            readyWrites + flush.readyWrites(),
            chunks + flush.chunks(),
            failedChunks + flush.failedChunks());
      }
      queuedStateScanToken = nextToken;
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        RepairFlushResult flush = flushReadyRepairChunks(pendingWork);
        LOG.warn("Reconcile ready-index repair pagination hit safety page cap; aborting scan");
        queuedStateScanToken = "";
        return new ReadyIndexRepairStats(
            true,
            pages,
            scanned,
            jobsRepaired + flush.jobsRepaired(),
            readyWrites + flush.readyWrites(),
            chunks + flush.chunks(),
            failedChunks + flush.failedChunks());
      }
    }
  }

  private ReadyIndexRepairWork buildRepairWork(StoredReconcileJob listedRecord) {
    if (listedRecord == null
        || listedRecord.readyIndexVersion >= CURRENT_READY_INDEX_VERSION
        || !QUEUED_STATE.equals(blankToEmpty(listedRecord.state))
        || blank(listedRecord.canonicalPointerKey)) {
      return null;
    }
    CanonicalPointerSnapshot snapshot =
        jobIndexStore.loadCanonicalSnapshot(listedRecord.canonicalPointerKey).orElse(null);
    if (snapshot == null) {
      return null;
    }
    StoredReconcileJob current = jobIndexStore.readRecord(snapshot).orElse(null);
    if (current == null
        || current.readyIndexVersion >= CURRENT_READY_INDEX_VERSION
        || !QUEUED_STATE.equals(blankToEmpty(current.state))) {
      return null;
    }
    current.canonicalPointerKey = snapshot.canonicalPointerKey();
    List<ReconcileJobIndexStore.ReadyQueueWrite> writes = expectedReadyWrites(current);
    if (writes.isEmpty()) {
      return null;
    }
    StoredReconcileJob previous = jobIndexStore.cloneStoredRecord(current);
    StoredReconcileJob repaired = jobIndexStore.cloneStoredRecord(current);
    repaired.readyIndexVersion = CURRENT_READY_INDEX_VERSION;
    repaired.updatedAtMs = System.currentTimeMillis();
    repaired.canonicalPointerKey = snapshot.canonicalPointerKey();
    ReconcileJobIndexStore.CanonicalRecordMutation mutation =
        new ReconcileJobIndexStore.CanonicalRecordMutation(snapshot, previous, repaired);
    int writeItems =
        jobIndexStore.writeItemCount(readyRepairWriteBatch(mutation, writes), List.of());
    return new ReadyIndexRepairWork(mutation, writes, writeItems);
  }

  private List<ReconcileJobIndexStore.ReadyQueueWrite> expectedReadyWrites(
      StoredReconcileJob record) {
    if (record == null || !QUEUED_STATE.equals(blankToEmpty(record.state))) {
      return List.of();
    }
    Map<String, ReconcileJobIndexStore.ReadyQueueWrite> writesByReadyKey = new LinkedHashMap<>();
    for (String readyKey : readyQueueStore.readyPointerKeys(record)) {
      if (blank(readyKey)) {
        continue;
      }
      writesByReadyKey.put(
          readyKey,
          new ReconcileJobIndexStore.ReadyQueueWrite(
              readyKey, record.canonicalPointerKey, PointerReferenceKind.PRK_POINTER_KEY));
    }
    return List.copyOf(writesByReadyKey.values());
  }

  private RepairFlushResult flushReadyRepairChunks(List<ReadyIndexRepairWork> workItems) {
    if (workItems == null || workItems.isEmpty()) {
      return RepairFlushResult.empty();
    }
    List<ReconcileJobIndexStore.JobWritePlan<ReadyIndexRepairWork>> plans = new ArrayList<>();
    for (ReadyIndexRepairWork work : workItems) {
      if (work != null) {
        plans.add(
            new ReconcileJobIndexStore.JobWritePlan<>(
                work, readyRepairWriteBatch(work.mutation(), work.readyWrites()), List.of()));
      }
    }
    int jobsRepaired = 0;
    int readyWrites = 0;
    int chunks = 0;
    int failedChunks = 0;
    for (ReconcileJobIndexStore.JobWriteChunk<ReadyIndexRepairWork> chunk :
        jobIndexStore.chunkJobWritePlans(plans)) {
      chunks++;
      if (!jobIndexStore.compareAndSetBatchWithPointerOps(
          chunk.indexBatch(), chunk.extraPointerOps())) {
        failedChunks++;
        continue;
      }
      jobsRepaired += chunk.plans().size();
      for (ReconcileJobIndexStore.JobWritePlan<ReadyIndexRepairWork> plan : chunk.plans()) {
        readyWrites += plan.subject().readyWrites().size();
      }
    }
    return new RepairFlushResult(jobsRepaired, readyWrites, chunks, failedChunks);
  }

  private ReconcileJobIndexStore.JobIndexWriteBatch readyRepairWriteBatch(
      ReconcileJobIndexStore.CanonicalRecordMutation mutation,
      List<ReconcileJobIndexStore.ReadyQueueWrite> writes) {
    List<ReconcileJobIndexStore.JobIndexWriteBatch> batches = new ArrayList<>();
    if (mutation != null && mutation.snapshot() != null && mutation.current() != null) {
      batches.add(
          jobIndexStore.buildJobIndexWriteBatch(
              mutation.snapshot(), mutation.previous(), mutation.current()));
    }
    if (writes != null && !writes.isEmpty()) {
      batches.add(
          new ReconcileJobIndexStore.JobIndexWriteBatch(
              List.of(),
              new ReconcileJobIndexStore.ReadyQueueMutation(List.copyOf(writes), List.of())));
    }
    return jobIndexStore.combineWriteBatches(batches);
  }

  private RepairFlushResult flushOversizedReadyRepair(ReadyIndexRepairWork work) {
    int mutationWriteItems = canonicalMutationWriteItems(work.mutation());
    if (mutationWriteItems > maxWriteItems()) {
      LOG.warnf(
          "Reconcile ready-index repair skipped oversized mutation jobId=%s write_items=%d"
              + " max_write_items=%d",
          work.mutation() == null || work.mutation().current() == null
              ? ""
              : blankToEmpty(work.mutation().current().jobId),
          Integer.valueOf(mutationWriteItems),
          Integer.valueOf(maxWriteItems()));
      return RepairFlushResult.empty();
    }
    int jobsRepaired = 0;
    int readyWrites = 0;
    int chunks = 0;
    int failedChunks = 0;
    List<ReconcileJobIndexStore.ReadyQueueWrite> writes = work.readyWrites();
    int index = 0;
    int finalChunkCapacity = maxWriteItems() - mutationWriteItems;
    while (writes.size() - index > finalChunkCapacity) {
      int nextIndex = Math.min(writes.size(), index + maxWriteItems());
      RepairFlushResult flush = commitReadyRepairChunk(List.of(), writes.subList(index, nextIndex));
      readyWrites += flush.readyWrites();
      chunks += flush.chunks();
      failedChunks += flush.failedChunks();
      if (flush.failedChunks() > 0) {
        return new RepairFlushResult(jobsRepaired, readyWrites, chunks, failedChunks);
      }
      index = nextIndex;
    }
    RepairFlushResult finalFlush =
        commitReadyRepairChunk(List.of(work.mutation()), writes.subList(index, writes.size()));
    return new RepairFlushResult(
        jobsRepaired + finalFlush.jobsRepaired(),
        readyWrites + finalFlush.readyWrites(),
        chunks + finalFlush.chunks(),
        failedChunks + finalFlush.failedChunks());
  }

  private RepairFlushResult commitReadyRepairChunk(
      List<ReconcileJobIndexStore.CanonicalRecordMutation> mutations,
      List<ReconcileJobIndexStore.ReadyQueueWrite> writes) {
    if ((mutations == null || mutations.isEmpty()) && (writes == null || writes.isEmpty())) {
      return RepairFlushResult.empty();
    }
    int mutationCount = mutations == null ? 0 : mutations.size();
    int readyWriteCount = writes == null ? 0 : writes.size();
    int writeItemCount =
        repairWriteItems(
            mutations == null ? List.of() : mutations, writes == null ? List.of() : writes);
    LOG.infof(
        "Reconcile ready-index repair committing batch jobs=%d ready_writes=%d write_items=%d"
            + " max_write_items=%d",
        Integer.valueOf(mutationCount),
        Integer.valueOf(readyWriteCount),
        Integer.valueOf(writeItemCount),
        Integer.valueOf(maxWriteItems()));
    if (jobIndexStore.commitReadyQueueRepairBatch(
        mutations == null ? List.of() : mutations, writes == null ? List.of() : writes)) {
      return new RepairFlushResult(mutationCount, readyWriteCount, 1, 0);
    }
    return new RepairFlushResult(0, 0, 1, 1);
  }

  private int repairWriteItems(
      List<ReconcileJobIndexStore.CanonicalRecordMutation> mutations,
      List<ReconcileJobIndexStore.ReadyQueueWrite> writes) {
    List<ReconcileJobIndexStore.JobIndexWriteBatch> batches = new ArrayList<>();
    if (mutations != null) {
      for (ReconcileJobIndexStore.CanonicalRecordMutation mutation : mutations) {
        if (mutation != null && mutation.snapshot() != null && mutation.current() != null) {
          batches.add(
              jobIndexStore.buildJobIndexWriteBatch(
                  mutation.snapshot(), mutation.previous(), mutation.current()));
        }
      }
    }
    if (writes != null && !writes.isEmpty()) {
      batches.add(
          new ReconcileJobIndexStore.JobIndexWriteBatch(
              List.of(),
              new ReconcileJobIndexStore.ReadyQueueMutation(List.copyOf(writes), List.of())));
    }
    return jobIndexStore.writeItemCount(jobIndexStore.combineWriteBatches(batches), List.of());
  }

  private int canonicalMutationWriteItems(ReconcileJobIndexStore.CanonicalRecordMutation mutation) {
    if (mutation == null || mutation.snapshot() == null || mutation.current() == null) {
      return 0;
    }
    ReconcileJobIndexStore.JobIndexWriteBatch batch =
        jobIndexStore.buildJobIndexWriteBatch(
            mutation.snapshot(), mutation.previous(), mutation.current());
    return jobIndexStore.writeItemCount(batch, List.of());
  }

  private int maxWriteItems() {
    return jobIndexStore.maxWriteItemsPerBatch();
  }

  private void logMaintenanceSummary(long startedAtMs, ReadyIndexRepairStats stats) {
    long elapsedMs = System.currentTimeMillis() - startedAtMs;
    if (!stats.active() && elapsedMs <= 500L) {
      LOG.debugf(
          "runReadyIndexMaintenanceOnce total_ms=%d ready_index_completed=%s",
          Long.valueOf(elapsedMs), Boolean.valueOf(stats.completed()));
      return;
    }
    LOG.infof(
        "runReadyIndexMaintenanceOnce total_ms=%d ready_index_completed=%s"
            + " ready_index_pages=%d ready_index_jobs_scanned=%d"
            + " ready_index_jobs_repaired=%d ready_index_writes=%d"
            + " ready_index_chunks=%d ready_index_failed_chunks=%d",
        Long.valueOf(elapsedMs),
        Boolean.valueOf(stats.completed()),
        Integer.valueOf(stats.pages()),
        Integer.valueOf(stats.scanned()),
        Integer.valueOf(stats.jobsWithRepairs()),
        Integer.valueOf(stats.readyWrites()),
        Integer.valueOf(stats.chunks()),
        Integer.valueOf(stats.failedChunks()));
  }

  private record ReadyIndexRepairWork(
      ReconcileJobIndexStore.CanonicalRecordMutation mutation,
      List<ReconcileJobIndexStore.ReadyQueueWrite> readyWrites,
      int writeItems) {}

  private record RepairFlushResult(
      int jobsRepaired, int readyWrites, int chunks, int failedChunks) {
    private static RepairFlushResult empty() {
      return new RepairFlushResult(0, 0, 0, 0);
    }
  }

  private record ReadyIndexRepairStats(
      boolean completed,
      int pages,
      int scanned,
      int jobsWithRepairs,
      int readyWrites,
      int chunks,
      int failedChunks) {
    private static ReadyIndexRepairStats empty() {
      return new ReadyIndexRepairStats(true, 0, 0, 0, 0, 0, 0);
    }

    private boolean active() {
      return scanned > 0 || readyWrites > 0 || chunks > 0 || failedChunks > 0;
    }
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
