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

package ai.floedb.floecat.service.reconciler.jobs.durable.projection;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJob;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.ReconcileJobPage;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobListSummary;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@ApplicationScoped
public class ReconcileJobRootSummaryStore {
  private static final int CAS_MAX = 8;

  public record StoredSummaryPage(
      List<StoredReconcileJobListSummary> summaries, String nextPageToken) {}

  private PointerStore pointerStore;
  private ReconcilePayloadStore payloadStore;

  public void bind(PointerStore pointerStore, ReconcilePayloadStore payloadStore) {
    this.pointerStore = pointerStore;
    this.payloadStore = payloadStore;
  }

  public void upsert(StoredReconcileJobListSummary summary) {
    if (summary == null || blank(summary.accountId()) || blank(summary.jobId())) {
      return;
    }
    upsertPointer(
        Keys.reconcileRootJobSummaryByAccountPointer(
            summary.accountId(), sortableJobToken(summary.createdAtMs(), summary.jobId())),
        summary);
    if (!blank(summary.connectorId())) {
      upsertPointer(
          Keys.reconcileRootJobSummaryByConnectorPointer(
              summary.accountId(),
              summary.connectorId(),
              sortableJobToken(summary.createdAtMs(), summary.jobId())),
          summary);
    }
  }

  public void delete(String accountId, String connectorId, long createdAtMs, String jobId) {
    if (blank(accountId) || blank(jobId)) {
      return;
    }
    pointerStore.delete(
        Keys.reconcileRootJobSummaryByAccountPointer(
            accountId, sortableJobToken(createdAtMs, jobId)));
    if (!blank(connectorId)) {
      pointerStore.delete(
          Keys.reconcileRootJobSummaryByConnectorPointer(
              accountId, connectorId, sortableJobToken(createdAtMs, jobId)));
    }
  }

  public ReconcileJobPage list(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states) {
    StoredSummaryPage page = listSummaries(accountId, pageSize, pageToken, connectorId, states);
    List<ReconcileJob> jobs = new ArrayList<>(page.summaries().size());
    for (StoredReconcileJobListSummary summary : page.summaries()) {
      jobs.add(toPublicJob(summary));
    }
    return new ReconcileJobPage(jobs, page.nextPageToken());
  }

  public StoredSummaryPage listSummaries(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states) {
    int limit = Math.max(1, pageSize);
    List<StoredReconcileJobListSummary> out = new ArrayList<>(limit);
    String token = pageToken == null ? "" : pageToken;
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(prefixFor(accountId, connectorId), limit, token, next);
      if (pointers.isEmpty()) {
        return new StoredSummaryPage(out, "");
      }
      for (Pointer pointer : pointers) {
        StoredReconcileJobListSummary summary =
            payloadStore.readInlineJobListSummary(pointer.getBlobUri()).orElse(null);
        if (summary == null) {
          continue;
        }
        if (states != null
            && !states.isEmpty()
            && !states.contains(blankToEmpty(summary.state()))) {
          continue;
        }
        out.add(summary);
        if (out.size() >= limit) {
          return new StoredSummaryPage(out, next.toString());
        }
      }
      String nextToken = next.toString();
      if (nextToken.isBlank() || nextToken.equals(token)) {
        return new StoredSummaryPage(out, "");
      }
      token = nextToken;
    }
  }

  private void upsertPointer(String key, StoredReconcileJobListSummary summary) {
    String blobUri = payloadStore.encodeInlineJobListSummary(summary);
    for (int i = 0; i < CAS_MAX; i++) {
      Pointer current = pointerStore.get(key).orElse(null);
      long expectedVersion = current == null ? 0L : current.getVersion();
      if (current != null && blobUri.equals(current.getBlobUri())) {
        return;
      }
      Pointer next = PointerReferences.inlineJsonPointer(key, blobUri, expectedVersion + 1L);
      if (pointerStore.compareAndSet(key, expectedVersion, next)) {
        return;
      }
    }
    throw new IllegalStateException(
        "Failed to upsert reconcile root summary for job " + summary.jobId());
  }

  private String prefixFor(String accountId, String connectorId) {
    return blank(connectorId)
        ? Keys.reconcileRootJobSummaryByAccountPointerPrefix(accountId)
        : Keys.reconcileRootJobSummaryByConnectorPointerPrefix(accountId, connectorId);
  }

  private static String sortableJobToken(long createdAtMs, String jobId) {
    long created = Math.max(0L, createdAtMs);
    long reversedCreated = Long.MAX_VALUE - created;
    return String.format("%019d-%s", reversedCreated, jobId);
  }

  public static ReconcileJob toPublicJob(StoredReconcileJobListSummary summary) {
    return new ReconcileJob(
        blankToEmpty(summary.jobId()),
        blankToEmpty(summary.accountId()),
        blankToEmpty(summary.connectorId()),
        blankToEmpty(summary.state()),
        blankToEmpty(summary.message()),
        summary.startedAtMs(),
        summary.finishedAtMs(),
        summary.tablesScanned(),
        summary.tablesChanged(),
        summary.viewsScanned(),
        summary.viewsChanged(),
        summary.errors(),
        summary.fullRescan(),
        summary.captureMode(),
        summary.snapshotsProcessed(),
        summary.statsProcessed(),
        summary.indexesProcessed(),
        true,
        ReconcileScope.empty(),
        ReconcileExecutionPolicy.of(
            summary.executionClass(), summary.executionLane(), summary.executionAttributes()),
        "",
        blankToEmpty(summary.executorId()),
        summary.jobKind() == null ? ReconcileJobKind.PLAN_CONNECTOR : summary.jobKind(),
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        summary.plannedFileGroups(),
        summary.plannedFiles(),
        summary.completedFileGroups(),
        summary.failedFileGroups(),
        summary.completedFiles(),
        summary.failedFiles(),
        "");
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
