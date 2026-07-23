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

package ai.floedb.floecat.service.gc;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.reconciler.jobs.DurableReconcileJobStore;
import ai.floedb.floecat.service.reconciler.jobs.ReconcilerSettingsStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJobListSummary;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.JobIndexEntrySnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReadyQueueKeys;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexCleanupManifest;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileReadyQueueBackend;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobGc {

  private static final Logger LOG = Logger.getLogger(ReconcileJobGc.class);

  private static final String INLINE_JOB_STATE_PREFIX = "inline:reconcile-job:";
  private static final String INLINE_JOB_LIST_SUMMARY_PREFIX = "inline:reconcile-job-list-summary:";
  private static final Set<String> TERMINAL_STATES =
      Set.of("JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED");
  private static final long INVALID_ORDERED_POINTER_MS = Long.MIN_VALUE;

  @Inject BlobStore blobStore;
  @Inject ObjectMapper mapper;
  @Inject ReconcilerSettingsStore settings;
  @Inject ReconcileJobIndexBackend jobIndexBackend;
  @Inject ReconcileJobIndexStore jobIndexStore;
  @Inject ReconcileReadyQueueBackend readyQueueBackend;
  @Inject ReconcileJobIndexes jobIndexes;
  @Inject PointerStore pointerStore;
  @Inject Instance<DurableReconcileJobStore> durableJobStore;
  java.util.function.LongSupplier clock = System::currentTimeMillis;

  @PostConstruct
  void initializeDurableJobStore() {
    if (durableJobStore.isResolvable()) {
      durableJobStore.get().initializeJobIndexStore();
    }
  }

  public record AccountResult(
      int scanned,
      int expired,
      int ptrDeleted,
      int blobDeleted,
      int dedupeDeleted,
      int readyDeleted,
      int canonicalQuarantined,
      int dedupeQuarantined,
      int rootSummaryQuarantined,
      String nextJobToken,
      String nextCanonicalQuarantineToken,
      String nextDedupeToken,
      String nextRootSummaryToken,
      String nextConnectorRootSummaryToken) {}

  public record GlobalResult(int scanned, int deleted, int quarantined, String nextToken) {}

  public record LookupMigrationResult(
      int scanned, int migrated, int conflicted, int retryable, String nextToken) {}

  public record CleanupMigrationResult(
      int scanned,
      int manifestsUpdated,
      int indexesBackfilled,
      int unresolvable,
      int conflicted,
      int retryable,
      String nextToken) {
    public CleanupMigrationResult(
        int scanned,
        int manifestsUpdated,
        int unresolvable,
        int conflicted,
        int retryable,
        String nextToken) {
      this(scanned, manifestsUpdated, 0, unresolvable, conflicted, retryable, nextToken);
    }
  }

  public CleanupMigrationResult runLegacyCleanupMigrationSlice(String pageTokenIn) {
    int pageSize =
        ConfigProvider.getConfig()
            .getOptionalValue(
                "floecat.gc.reconcile-jobs.legacy-cleanup-migration-page-size", Integer.class)
            .orElse(500);
    var page =
        jobIndexBackend.migrateLegacyCleanupManifests(
            Math.max(1, pageSize), pageTokenIn == null ? "" : pageTokenIn);
    int indexesBackfilled = 0;
    int retryable = page.retryable();
    for (String canonicalPointerKey : page.canonicalPointerKeys()) {
      var backfill = jobIndexStore.backfillStoredJobIndexes(canonicalPointerKey);
      if (backfill.updated()) {
        indexesBackfilled++;
      }
      if (backfill.retryable()) {
        retryable++;
      }
    }
    return new CleanupMigrationResult(
        page.scanned(),
        page.manifestsUpdated(),
        indexesBackfilled,
        page.unresolvable(),
        page.conflicted(),
        retryable,
        page.nextPageToken());
  }

  public LookupMigrationResult runLegacyLookupMigrationSlice(String pageTokenIn) {
    int pageSize =
        ConfigProvider.getConfig()
            .getOptionalValue(
                "floecat.gc.reconcile-jobs.legacy-lookup-migration-page-size", Integer.class)
            .orElse(100);
    var page =
        jobIndexBackend.migrateLegacyLookupEntries(
            Math.max(1, pageSize), pageTokenIn == null ? "" : pageTokenIn);
    return new LookupMigrationResult(
        page.scanned(), page.migrated(), page.conflicted(), page.retryable(), page.nextPageToken());
  }

  public Optional<ReconcileJobIndexBackend.LegacyMigrationLease> acquireLegacyMigrationLease(
      ReconcileJobIndexBackend.LegacyMigration migration,
      String ownerId,
      long nowMs,
      long leaseDurationMs) {
    return jobIndexBackend.acquireLegacyMigrationLease(migration, ownerId, nowMs, leaseDurationMs);
  }

  public boolean checkpointLegacyMigration(
      ReconcileJobIndexBackend.LegacyMigration migration,
      String ownerId,
      long fence,
      ReconcileJobIndexBackend.LegacyMigrationProgress progress,
      long nowMs,
      long leaseDurationMs) {
    return jobIndexBackend.checkpointLegacyMigration(
        migration, ownerId, fence, progress, nowMs, leaseDurationMs);
  }

  public boolean completeLegacyMigration(
      ReconcileJobIndexBackend.LegacyMigration migration, String ownerId, long fence, long nowMs) {
    return jobIndexBackend.completeLegacyMigration(migration, ownerId, fence, nowMs);
  }

  public boolean legacyMigrationComplete(ReconcileJobIndexBackend.LegacyMigration migration) {
    return jobIndexBackend.legacyMigrationComplete(migration);
  }

  private enum RootSummaryReadStatus {
    READABLE,
    UNREADABLE
  }

  private record RootSummaryReadResult(
      RootSummaryReadStatus status, StoredReconcileJobListSummary summary) {}

  private record RootSummaryDeleteResult(int deleted, int quarantined) {}

  private record JobCleanupResult(int expired, int ptrDeleted, int blobDeleted, int failed) {}

  private static final class CleanupWriteBudget {
    private boolean attempted;

    private boolean canAttempt(long nowMs, long deadline) {
      return !attempted || nowMs < deadline;
    }

    private void recordAttempt() {
      attempted = true;
    }
  }

  public AccountResult runAccountSlice(String accountId, String jobTokenIn, String dedupeTokenIn) {
    return runAccountSlice(accountId, jobTokenIn, dedupeTokenIn, "");
  }

  public AccountResult runAccountSlice(
      String accountId, String jobTokenIn, String dedupeTokenIn, String rootSummaryTokenIn) {
    return runAccountSlice(accountId, jobTokenIn, dedupeTokenIn, rootSummaryTokenIn, "");
  }

  public AccountResult runAccountSlice(
      String accountId,
      String jobTokenIn,
      String dedupeTokenIn,
      String rootSummaryTokenIn,
      String connectorRootSummaryTokenIn) {
    return runAccountSlice(
        accountId, jobTokenIn, "", dedupeTokenIn, rootSummaryTokenIn, connectorRootSummaryTokenIn);
  }

  public AccountResult runAccountSlice(
      String accountId,
      String jobTokenIn,
      String canonicalQuarantineTokenIn,
      String dedupeTokenIn,
      String rootSummaryTokenIn,
      String connectorRootSummaryTokenIn) {
    var cfg = ConfigProvider.getConfig();
    final int pageSize =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.page-size", Integer.class).orElse(200);
    final int batchLimit =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.batch-limit", Integer.class).orElse(1000);
    final long sliceMillis =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.slice-millis", Long.class).orElse(4000L);
    final long retentionMs =
        Math.max(
            1L,
            settings != null
                ? settings.finishedJobRetentionMs()
                : cfg.getOptionalValue("floecat.gc.reconcile-jobs.retention-ms", Long.class)
                    .orElse(24L * 60L * 60L * 1000L));
    final long canonicalQuarantineRetentionMs =
        Math.max(
            0L,
            cfg.getOptionalValue(
                    "floecat.gc.reconcile-jobs.canonical-quarantine-retention-ms", Long.class)
                .orElse(24L * 60L * 60L * 1000L));

    final long nowMs = clock.getAsLong();
    final long deadline = nowMs + sliceMillis;

    String jobToken = jobTokenIn == null ? "" : jobTokenIn;
    String canonicalQuarantineToken =
        canonicalQuarantineTokenIn == null ? "" : canonicalQuarantineTokenIn;
    String dedupeToken = dedupeTokenIn == null ? "" : dedupeTokenIn;
    String rootSummaryToken = rootSummaryTokenIn == null ? "" : rootSummaryTokenIn;
    String connectorRootSummaryToken =
        connectorRootSummaryTokenIn == null ? "" : connectorRootSummaryTokenIn;

    int scanned = 0;
    int expired = 0;
    int ptrDeleted = 0;
    int blobDeleted = 0;
    int dedupeDeleted = 0;
    int readyDeleted = 0;
    int canonicalQuarantined = 0;
    int dedupeQuarantined = 0;
    int rootSummaryQuarantined = 0;

    String jobPrefix = Keys.reconcileJobPointerByIdPrefix(accountId);
    List<ReconcileJobIndexStore.JobWritePlan<String>> deletePlans = new ArrayList<>();
    List<ReconcileJobIndexStore.JobWritePlan<String>> quarantinedDeletePlans = new ArrayList<>();
    String cleanupRetryToken = null;
    while (scanned < batchLimit && clock.getAsLong() < deadline) {
      int limit = Math.min(pageSize, batchLimit - scanned);
      String jobPageStartToken = jobToken;
      var page = jobIndexBackend.listCanonicalEntries(accountId, limit, jobToken);
      var pointers = page.entries();
      jobToken = page.nextPageToken();
      if (pointers.isEmpty()) {
        break;
      }
      boolean partialPage = false;
      int preparedInPage = 0;
      String lastPreparedJobToken = jobPageStartToken;
      for (var canonical : pointers) {
        if (scanned >= batchLimit || (preparedInPage > 0 && clock.getAsLong() >= deadline)) {
          partialPage = true;
          break;
        }
        scanned++;
        preparedInPage++;
        lastPreparedJobToken = canonical.pointerKey();

        String jobId = decodeJobId(jobPrefix, canonical.pointerKey());
        if (canonical.cleanupLocked()) {
          ReconcileJobIndexStore.JobWritePlan<String> deletePlan =
              buildLockedCanonicalFootprintDeletePlan(accountId, jobId, canonical);
          if (deletePlan != null) {
            deletePlans.add(deletePlan);
            if (cleanupRetryToken == null) {
              cleanupRetryToken = jobPageStartToken;
            }
          }
        } else {
          JsonNode record = readRecordByReference(canonical.blobUri());
          if (record == null) {
            ReconcileJobIndexStore.JobWritePlan<String> deletePlan =
                buildQuarantinedCanonicalDeletePlan(
                    accountId, jobId, canonical, nowMs, canonicalQuarantineRetentionMs);
            if (deletePlan != null) {
              quarantinedDeletePlans.add(deletePlan);
              if (cleanupRetryToken == null) {
                cleanupRetryToken = jobPageStartToken;
              }
            } else {
              canonicalQuarantined++;
            }
          } else {
            String state = text(record, "state");
            long updatedAt =
                longValue(
                    record,
                    "updatedAtMs",
                    longValue(record, "finishedAtMs", longValue(record, "createdAtMs", nowMs)));

            if (TERMINAL_STATES.contains(state)) {
              boolean deletePlanned = false;
              if (updatedAt <= nowMs - retentionMs) {
                ReconcileJobIndexStore.JobWritePlan<String> deletePlan =
                    buildCanonicalFootprintDeletePlan(accountId, jobId, canonical, record);
                if (deletePlan != null) {
                  deletePlans.add(deletePlan);
                  deletePlanned = true;
                  if (cleanupRetryToken == null) {
                    cleanupRetryToken = jobPageStartToken;
                  }
                }
              }

              if (!deletePlanned) {
                // Terminal jobs that cannot yet be removed must not hold queue references. Root-job
                // dedupe ownership can also be released immediately; child dedupe ownership is
                // retained until canonical deletion because it is the durable identity used by
                // planner repair.
                StoredReconcileJob stored = storedJob(record);
                String dedupePointerKey = stored == null ? "" : jobIndexes.dedupePointerKey(stored);
                if (!dedupePointerKey.isBlank()
                    && (stored.parentJobId == null || stored.parentJobId.isBlank())
                    && deleteJobIndexPointerIfOwned(dedupePointerKey, canonical.pointerKey())) {
                  dedupeDeleted++;
                }
                for (String readyKey : readyPointerKeysForCleanup(record)) {
                  if (!readyKey.isBlank() && readyQueueBackend.deleteReadyEntry(readyKey)) {
                    readyDeleted++;
                  }
                }
              }
            }
          }
        }
      }

      if (partialPage) {
        // The backend page token resumes after the whole fetched page. When the slice deadline
        // interrupts preparation mid-page, resume after the last entry actually prepared so the
        // unvisited suffix is not skipped. Both native backends accept a canonical pointer key as
        // an exclusive-start token. A failed cleanup below still pins the cursor to the page start.
        jobToken = lastPreparedJobToken;
        break;
      }
      if (jobToken.isBlank()) {
        break;
      }
    }

    CleanupWriteBudget cleanupWriteBudget = new CleanupWriteBudget();
    JobCleanupResult cleanup =
        deleteCanonicalFootprints(accountId, deletePlans, deadline, cleanupWriteBudget);
    expired += cleanup.expired();
    ptrDeleted += cleanup.ptrDeleted();
    blobDeleted += cleanup.blobDeleted();

    JobCleanupResult quarantinedCleanup =
        deleteCanonicalFootprints(accountId, quarantinedDeletePlans, deadline, cleanupWriteBudget);
    expired += quarantinedCleanup.expired();
    ptrDeleted += quarantinedCleanup.ptrDeleted();
    blobDeleted += quarantinedCleanup.blobDeleted();
    canonicalQuarantined += quarantinedCleanup.failed();

    if ((cleanup.failed() > 0 || quarantinedCleanup.failed() > 0) && cleanupRetryToken != null) {
      jobToken = cleanupRetryToken;
    }

    while (scanned < batchLimit && clock.getAsLong() < deadline && pointerStore != null) {
      int limit = Math.min(pageSize, batchLimit - scanned);
      StringBuilder next = new StringBuilder();
      var markers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileCanonicalQuarantinePointerPrefix(accountId),
              limit,
              canonicalQuarantineToken,
              next);
      canonicalQuarantineToken = next.toString();
      if (markers.isEmpty()) {
        break;
      }
      for (Pointer marker : markers) {
        if (scanned >= batchLimit) {
          break;
        }
        scanned++;
        clearCanonicalQuarantineMarkerIfReadable(marker);
      }
      if (canonicalQuarantineToken.isBlank()) {
        break;
      }
    }

    String dedupePrefix = Keys.reconcileDedupePointerPrefix(accountId);
    while (scanned < batchLimit && clock.getAsLong() < deadline) {
      int limit = Math.min(pageSize, batchLimit - scanned);
      var dedupePage = jobIndexBackend.listDedupeEntries(accountId, limit, dedupeToken);
      var dedupePointers = dedupePage.entries();
      dedupeToken = dedupePage.nextPageToken();
      if (dedupePointers.isEmpty()) {
        break;
      }

      for (var dedupe : dedupePointers) {
        if (scanned >= batchLimit) {
          break;
        }
        scanned++;

        var canonical = jobIndexBackend.loadIndexEntry(dedupe.blobUri()).orElse(null);
        if (canonical == null) {
          if (deleteJobIndexPointerIfOwned(dedupe.pointerKey(), dedupe.blobUri())) {
            dedupeDeleted++;
          }
          continue;
        }
        JsonNode record = readRecordByReference(canonical.blobUri());
        if (record == null) {
          dedupeQuarantined++;
          continue;
        }

        // Direct-child dedupe ownership remains the durable identity for parent-first planner
        // repair. A child may finish before a later planner chunk is retried; deleting its dedupe
        // row here would let the retry create an N+1 replacement. Child GC deletes the row with
        // the canonical child. Root jobs retain the historical terminal-dedupe cleanup behavior.
        if (TERMINAL_STATES.contains(text(record, "state"))
            && text(record, "parentJobId").isBlank()) {
          if (deleteJobIndexPointerIfOwned(dedupe.pointerKey(), dedupe.blobUri())) {
            dedupeDeleted++;
          }
        }
      }

      if (dedupeToken.isBlank()) {
        break;
      }
    }

    String rootSummaryPrefix = Keys.reconcileRootJobSummaryByAccountPointerPrefix(accountId);
    while (scanned < batchLimit && clock.getAsLong() < deadline && pointerStore != null) {
      int limit = Math.min(pageSize, batchLimit - scanned);
      StringBuilder next = new StringBuilder();
      var summaries =
          pointerStore.listPointersByPrefix(rootSummaryPrefix, limit, rootSummaryToken, next);
      rootSummaryToken = next.toString();
      if (summaries.isEmpty()) {
        break;
      }

      for (Pointer summaryPointer : summaries) {
        if (scanned >= batchLimit) {
          break;
        }
        scanned++;
        RootSummaryDeleteResult result = deleteRootSummaryIfOrphan(accountId, summaryPointer);
        ptrDeleted += result.deleted();
        rootSummaryQuarantined += result.quarantined();
      }

      if (rootSummaryToken.isBlank()) {
        break;
      }
    }

    String connectorRootSummaryPrefix =
        Keys.reconcileRootJobSummaryByConnectorAccountPrefix(accountId);
    while (scanned < batchLimit && clock.getAsLong() < deadline && pointerStore != null) {
      int limit = Math.min(pageSize, batchLimit - scanned);
      StringBuilder next = new StringBuilder();
      var summaries =
          pointerStore.listPointersByPrefix(
              connectorRootSummaryPrefix, limit, connectorRootSummaryToken, next);
      connectorRootSummaryToken = next.toString();
      if (summaries.isEmpty()) {
        break;
      }

      for (Pointer summaryPointer : summaries) {
        if (scanned >= batchLimit) {
          break;
        }
        scanned++;
        RootSummaryDeleteResult result = deleteRootSummaryIfOrphan(accountId, summaryPointer);
        ptrDeleted += result.deleted();
        rootSummaryQuarantined += result.quarantined();
      }

      if (connectorRootSummaryToken.isBlank()) {
        break;
      }
    }

    return new AccountResult(
        scanned,
        expired,
        ptrDeleted,
        blobDeleted,
        dedupeDeleted,
        readyDeleted,
        canonicalQuarantined,
        dedupeQuarantined,
        rootSummaryQuarantined,
        jobToken,
        canonicalQuarantineToken,
        dedupeToken,
        rootSummaryToken,
        connectorRootSummaryToken);
  }

  public GlobalResult runReadySlice(String pageTokenIn) {
    var cfg = ConfigProvider.getConfig();
    final int pageSize =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.page-size", Integer.class).orElse(200);
    final int batchLimit =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.global-ready-batch-limit", Integer.class)
            .orElse(1000);
    final long staleReadyGraceMs =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.ready-stale-grace-ms", Long.class)
            .orElse(60_000L);
    final long nowMs = System.currentTimeMillis();

    int scanned = 0;
    int deleted = 0;
    int quarantined = 0;
    String token = pageTokenIn == null ? "" : pageTokenIn;

    while (scanned < batchLimit) {
      int limit = Math.min(pageSize, batchLimit - scanned);
      var readyPage = readyQueueBackend.scanAllReadyEntries(limit, token);
      token = readyPage.nextPageToken();
      if (readyPage.entries().isEmpty()) {
        break;
      }

      for (var ready : readyPage.entries()) {
        if (scanned >= batchLimit) {
          break;
        }
        scanned++;

        var canonical = jobIndexBackend.loadIndexEntry(ready.canonicalPointerKey()).orElse(null);
        if (canonical == null) {
          if (ready.dueAtMs() <= nowMs - staleReadyGraceMs
              && readyQueueBackend.deleteReadyEntry(ready.readyPointerKey())) {
            deleted++;
          }
          continue;
        }
        JsonNode record = readRecordByReference(canonical.blobUri());
        if (record == null) {
          quarantined++;
          continue;
        }

        String state = text(record, "state");
        if (ready.dueAtMs() <= nowMs - staleReadyGraceMs) {
          boolean deleteReady = !"JS_QUEUED".equals(state);
          if (!deleteReady) {
            java.util.LinkedHashSet<String> validReadyKeys =
                new java.util.LinkedHashSet<>(currentReadyPointerKeys(record));
            deleteReady = !validReadyKeys.contains(ready.readyPointerKey());
          }
          if (deleteReady && readyQueueBackend.deleteReadyEntry(ready.readyPointerKey())) {
            deleted++;
          }
        }
      }

      if (token.isBlank()) {
        break;
      }
    }

    return new GlobalResult(scanned, deleted, quarantined, token);
  }

  private JsonNode readRecordByCanonicalKey(String canonicalPointerKey) {
    if (canonicalPointerKey == null || canonicalPointerKey.isBlank()) {
      return null;
    }
    var canonical = jobIndexBackend.loadIndexEntry(canonicalPointerKey).orElse(null);
    return canonical == null ? null : readRecordByReference(canonical.blobUri());
  }

  private JsonNode readRecordByReference(String reference) {
    if (reference == null || reference.isBlank()) {
      return null;
    }
    if (reference.startsWith(INLINE_JOB_STATE_PREFIX)) {
      try {
        byte[] payload =
            Base64.getUrlDecoder().decode(reference.substring(INLINE_JOB_STATE_PREFIX.length()));
        return mapper.readTree(payload);
      } catch (Exception ignored) {
        return null;
      }
    }
    return null;
  }

  private ReconcileJobIndexStore.JobWritePlan<String> buildQuarantinedCanonicalDeletePlan(
      String accountId,
      String jobId,
      JobIndexEntrySnapshot canonical,
      long nowMs,
      long quarantineRetentionMs) {
    if (canonical == null || accountId == null || accountId.isBlank()) {
      return null;
    }
    String markerKey =
        Keys.reconcileCanonicalQuarantinePointer(accountId, hashValue(canonical.pointerKey()));
    long firstSeenMs = nowMs;
    Pointer marker = pointerStore == null ? null : pointerStore.get(markerKey).orElse(null);
    String markerPayload = quarantineMarkerPayload(canonical, nowMs);
    if (marker == null || !markerPayloadMatches(marker.getBlobUri(), canonical)) {
      if (pointerStore != null) {
        long expectedVersion = marker == null ? 0L : marker.getVersion();
        pointerStore.compareAndSet(
            markerKey,
            expectedVersion,
            PointerReferences.opaqueMarkerPointer(markerKey, markerPayload, expectedVersion + 1L));
      }
      return null;
    } else {
      firstSeenMs = quarantineMarkerFirstSeenMs(marker.getBlobUri(), nowMs);
      if (quarantineMarkerCanonicalKey(marker.getBlobUri()).isBlank() && pointerStore != null) {
        boolean migrated =
            pointerStore.compareAndSet(
                markerKey,
                marker.getVersion(),
                PointerReferences.opaqueMarkerPointer(
                    markerKey,
                    quarantineMarkerPayload(canonical, firstSeenMs),
                    marker.getVersion() + 1L));
        if (migrated) {
          return null;
        }
      }
    }
    if (firstSeenMs > nowMs - quarantineRetentionMs) {
      return null;
    }
    if (!jobIndexBackend.legacyCleanupMigrationComplete()) {
      LOG.debugf(
          "Retaining quarantined reconcile job while awaiting cleanup migration accountId=%s jobId=%s canonicalKey=%s",
          accountId, jobId, canonical.pointerKey());
      return null;
    }
    ReconcileJobIndexStore.JobIndexWriteBatch deleteBatch =
        jobIndexStore.buildJobDeleteBatch(
            new CanonicalPointerSnapshot(
                canonical.pointerKey(), canonical.blobUri(), canonical.version()));
    if (deleteBatch.writes().isEmpty()) {
      // Non-Dynamo backends can reconstruct legacy references without a table scan.
      ReconcileJobIndexCleanupManifest discovered =
          jobIndexBackend.discoverLegacyCleanupManifest(canonical.pointerKey());
      deleteBatch =
          jobIndexStore.buildDiscoveredLegacyJobDeleteBatch(
              new CanonicalPointerSnapshot(
                  canonical.pointerKey(), canonical.blobUri(), canonical.version()),
              discovered);
      if (!deleteBatch.writes().isEmpty()) {
        LOG.warnf(
            "Using reverse-reference cleanup fallback for unreadable legacy reconcile job accountId=%s jobId=%s indexPointers=%d readyPointers=%d",
            accountId,
            jobId,
            discovered.indexPointerKeys().size(),
            discovered.readyPointerKeys().size());
      }
    }
    if (deleteBatch.writes().isEmpty()) {
      return null;
    }
    List<PointerStore.CasOp> pointerDeletes = new ArrayList<>();
    if (marker != null) {
      pointerDeletes.add(new PointerStore.CasDelete(marker.getKey(), marker.getVersion()));
    }
    return new ReconcileJobIndexStore.JobWritePlan<>(jobId, deleteBatch, pointerDeletes);
  }

  private void clearCanonicalQuarantineMarkerIfReadable(Pointer marker) {
    if (marker == null || marker.getKey() == null || marker.getKey().isBlank()) {
      return;
    }
    String canonicalKey = quarantineMarkerCanonicalKey(marker.getBlobUri());
    if (!canonicalKey.isBlank() && readRecordByCanonicalKey(canonicalKey) != null) {
      pointerStore.compareAndDelete(marker.getKey(), marker.getVersion());
    }
  }

  private static String quarantineMarkerPayload(JobIndexEntrySnapshot canonical, long firstSeenMs) {
    return canonical.version()
        + "\n"
        + canonical.blobUri()
        + "\n"
        + firstSeenMs
        + "\n"
        + canonical.pointerKey();
  }

  private static boolean markerPayloadMatches(
      String markerPayload, JobIndexEntrySnapshot canonical) {
    if (markerPayload == null || canonical == null) {
      return false;
    }
    String[] parts = markerPayload.split("\n", 4);
    // The cleanup lock advances the canonical version without changing the corrupt payload. Treat
    // that as the same quarantined observation so a failed cleanup attempt cannot restart the
    // retention clock indefinitely.
    return parts.length >= 3
        && java.util.Objects.equals(parts[1], canonical.blobUri())
        && (parts.length < 4
            || parts[3].isBlank()
            || java.util.Objects.equals(parts[3], canonical.pointerKey()));
  }

  private static long quarantineMarkerFirstSeenMs(String markerPayload, long defaultValue) {
    if (markerPayload == null) {
      return defaultValue;
    }
    String[] parts = markerPayload.split("\n", 4);
    return parts.length >= 3 ? parseLong(parts[2], defaultValue) : defaultValue;
  }

  private static String quarantineMarkerCanonicalKey(String markerPayload) {
    if (markerPayload == null) {
      return "";
    }
    String[] parts = markerPayload.split("\n", 4);
    return parts.length == 4 ? parts[3] : "";
  }

  private static String text(JsonNode node, String field) {
    if (node == null || field == null) {
      return "";
    }
    JsonNode child = node.get(field);
    if (child == null || child.isNull()) {
      return "";
    }
    String value = child.asText("");
    return value == null ? "" : value;
  }

  private static long longValue(JsonNode node, String field, long defaultValue) {
    if (node == null || field == null) {
      return defaultValue;
    }
    JsonNode child = node.get(field);
    if (child == null || child.isNull()) {
      return defaultValue;
    }
    return child.asLong(defaultValue);
  }

  private RootSummaryDeleteResult deleteRootSummaryIfOrphan(
      String accountId, Pointer summaryPointer) {
    if (summaryPointer == null
        || summaryPointer.getKey() == null
        || summaryPointer.getKey().isBlank()
        || pointerStore == null) {
      return new RootSummaryDeleteResult(0, 0);
    }
    RootSummaryReadResult read = readRootSummary(summaryPointer.getBlobUri());
    if (read.status() != RootSummaryReadStatus.READABLE) {
      return new RootSummaryDeleteResult(0, 1);
    }
    StoredReconcileJobListSummary summary = read.summary();
    if (summary == null || summary.jobId() == null || summary.jobId().isBlank()) {
      return new RootSummaryDeleteResult(0, 1);
    }
    String canonicalKey = Keys.reconcileJobPointerById(accountId, summary.jobId());
    if (jobIndexBackend.loadIndexEntry(canonicalKey).isPresent()) {
      return new RootSummaryDeleteResult(0, 0);
    }
    var deletes = new java.util.LinkedHashMap<String, PointerStore.CasDelete>();
    deletes.put(
        summaryPointer.getKey(),
        new PointerStore.CasDelete(summaryPointer.getKey(), summaryPointer.getVersion()));
    appendPointerDeleteIfPresent(
        deletes, Keys.reconcileJobProjectionPointer(accountId, summary.jobId()));
    if (summary.connectorId() != null && !summary.connectorId().isBlank()) {
      String connectorSummaryKey =
          Keys.reconcileRootJobSummaryByConnectorPointer(
              accountId,
              summary.connectorId(),
              rootSummarySortableJobToken(summary.createdAtMs(), summary.jobId()));
      appendPointerDeleteIfPresent(deletes, connectorSummaryKey);
    }
    java.util.ArrayList<PointerStore.CasOp> ops = new java.util.ArrayList<>();
    ops.addAll(deletes.values());
    return new RootSummaryDeleteResult(
        jobIndexBackend.compareAndSetBatch(
                new ReconcileJobIndexStore.JobIndexWriteBatch(
                    List.of(new ReconcileJobIndexStore.JobIndexCheckAbsent(canonicalKey)),
                    ReconcileJobIndexStore.ReadyQueueMutation.empty()),
                ops)
            ? deletes.size()
            : 0,
        0);
  }

  private RootSummaryReadResult readRootSummary(String reference) {
    if (reference == null || !reference.startsWith(INLINE_JOB_LIST_SUMMARY_PREFIX)) {
      return new RootSummaryReadResult(RootSummaryReadStatus.UNREADABLE, null);
    }
    try {
      byte[] payload =
          Base64.getUrlDecoder()
              .decode(reference.substring(INLINE_JOB_LIST_SUMMARY_PREFIX.length()));
      return new RootSummaryReadResult(
          RootSummaryReadStatus.READABLE,
          mapper.readValue(payload, StoredReconcileJobListSummary.class));
    } catch (Exception ignored) {
      return new RootSummaryReadResult(RootSummaryReadStatus.UNREADABLE, null);
    }
  }

  private static String decodeJobId(String prefix, String canonicalKey) {
    if (canonicalKey == null || prefix == null || !canonicalKey.startsWith(prefix)) {
      return null;
    }
    String suffix = canonicalKey.substring(prefix.length());
    if (suffix.isBlank()) {
      return null;
    }
    return URLDecoder.decode(suffix, StandardCharsets.UTF_8);
  }

  private static String hashValue(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] payload = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest(payload));
    } catch (Exception e) {
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
    }
  }

  private ReconcileJobIndexStore.JobWritePlan<String> buildLockedCanonicalFootprintDeletePlan(
      String accountId, String jobId, JobIndexEntrySnapshot canonical) {
    if (canonical == null || !canonical.cleanupLocked() || jobId == null || jobId.isBlank()) {
      return null;
    }
    var pointerDeletes = new java.util.ArrayList<PointerStore.CasOp>();
    appendPointerDeleteIfPresent(
        pointerDeletes,
        Keys.reconcileCanonicalQuarantinePointer(accountId, hashValue(canonical.pointerKey())));
    String projectionPointerKey = Keys.reconcileJobProjectionPointer(accountId, jobId);
    appendPointerDeleteIfPresent(pointerDeletes, projectionPointerKey);
    ReconcileJobIndexCleanupManifest cleanupManifest =
        jobIndexBackend.loadCleanupManifest(canonical.pointerKey());
    for (String pointerKey : cleanupManifest.pointerKeys()) {
      if (!projectionPointerKey.equals(pointerKey)) {
        appendPointerDeleteIfPresent(pointerDeletes, pointerKey);
      }
    }
    JsonNode record = readRecordByReference(canonical.blobUri());
    if (record != null && cleanupManifest.pointerKeys().isEmpty()) {
      if (text(record, "parentJobId").isBlank()) {
        appendPointerDeleteIfPresent(
            pointerDeletes,
            Keys.reconcileRootJobSummaryByAccountPointer(
                accountId,
                rootSummarySortableJobToken(longValue(record, "createdAtMs", 0L), jobId)));
        String connectorId = text(record, "connectorId");
        if (!connectorId.isBlank()) {
          appendPointerDeleteIfPresent(
              pointerDeletes,
              Keys.reconcileRootJobSummaryByConnectorPointer(
                  accountId,
                  connectorId,
                  rootSummarySortableJobToken(longValue(record, "createdAtMs", 0L), jobId)));
        }
      }
    }
    ReconcileJobIndexStore.JobIndexWriteBatch deleteBatch =
        jobIndexStore.buildJobDeleteBatch(
            new CanonicalPointerSnapshot(
                canonical.pointerKey(), canonical.blobUri(), canonical.version()));
    if (deleteBatch.writes().isEmpty()) {
      LOG.warnf(
          "Deferring claimed reconcile-job cleanup until its manifest is available accountId=%s jobId=%s canonicalKey=%s",
          accountId, jobId, canonical.pointerKey());
      return null;
    }
    return new ReconcileJobIndexStore.JobWritePlan<>(jobId, deleteBatch, pointerDeletes);
  }

  private ReconcileJobIndexStore.JobWritePlan<String> buildCanonicalFootprintDeletePlan(
      String accountId, String jobId, JobIndexEntrySnapshot canonical, JsonNode record) {
    if (canonical == null || jobId == null || jobId.isBlank()) {
      return null;
    }
    if (!jobIndexBackend.legacyCleanupMigrationComplete()) {
      return null;
    }
    var pointerDeletes = new java.util.ArrayList<PointerStore.CasOp>();
    appendPointerDeleteIfPresent(
        pointerDeletes,
        Keys.reconcileCanonicalQuarantinePointer(accountId, hashValue(canonical.pointerKey())));
    if (record != null) {
      appendPointerDeleteIfPresent(
          pointerDeletes, Keys.reconcileJobProjectionPointer(accountId, jobId));
      if (text(record, "parentJobId").isBlank()) {
        appendPointerDeleteIfPresent(
            pointerDeletes,
            Keys.reconcileRootJobSummaryByAccountPointer(
                accountId,
                rootSummarySortableJobToken(longValue(record, "createdAtMs", 0L), jobId)));
        String connectorId = text(record, "connectorId");
        if (!connectorId.isBlank()) {
          appendPointerDeleteIfPresent(
              pointerDeletes,
              Keys.reconcileRootJobSummaryByConnectorPointer(
                  accountId,
                  connectorId,
                  rootSummarySortableJobToken(longValue(record, "createdAtMs", 0L), jobId)));
        }
      }
    }
    CanonicalPointerSnapshot snapshot =
        new CanonicalPointerSnapshot(
            canonical.pointerKey(), canonical.blobUri(), canonical.version());
    ReconcileJobIndexStore.JobIndexWriteBatch deleteBatch =
        jobIndexStore.buildJobDeleteBatch(snapshot);
    if (deleteBatch.writes().isEmpty() && record != null) {
      deleteBatch = jobIndexStore.buildReadableLegacyJobDeleteBatch(snapshot, storedJob(record));
      if (!deleteBatch.writes().isEmpty()) {
        LOG.infof(
            "Using readable legacy reconcile-job GC fallback accountId=%s jobId=%s",
            accountId, jobId);
      }
    }
    if (deleteBatch.writes().isEmpty()) {
      return null;
    }
    return new ReconcileJobIndexStore.JobWritePlan<>(jobId, deleteBatch, pointerDeletes);
  }

  private JobCleanupResult deleteCanonicalFootprints(
      String accountId,
      List<ReconcileJobIndexStore.JobWritePlan<String>> plans,
      long deadline,
      CleanupWriteBudget writeBudget) {
    int expired = 0;
    int ptrDeleted = 0;
    int blobDeleted = 0;
    List<ReconcileJobIndexStore.JobWritePlan<String>> regularPlans = new ArrayList<>();
    for (ReconcileJobIndexStore.JobWritePlan<String> plan : plans) {
      if (jobIndexStore.writeItemCount(plan.indexBatch(), plan.extraPointerOps())
          <= jobIndexStore.maxWriteItemsPerBatch()) {
        regularPlans.add(plan);
        continue;
      }
      boolean completed = true;
      for (ReconcileJobIndexStore.JobWriteChunk<String> phase :
          jobIndexStore.chunkOversizedJobDeletePlan(plan)) {
        if (!writeBudget.canAttempt(clock.getAsLong(), deadline)) {
          completed = false;
          break;
        }
        writeBudget.recordAttempt();
        if (!jobIndexBackend.compareAndSetBatch(phase.indexBatch(), phase.extraPointerOps())) {
          completed = false;
          break;
        }
      }
      if (completed) {
        expired++;
        ptrDeleted++;
        if (plan.subject() != null && !plan.subject().isBlank()) {
          blobDeleted += deleteJobBlobs(accountId, plan.subject());
        }
      }
    }
    regularChunks:
    for (ReconcileJobIndexStore.JobWriteChunk<String> chunk :
        jobIndexStore.chunkJobWritePlans(regularPlans)) {
      if (!writeBudget.canAttempt(clock.getAsLong(), deadline)) {
        break;
      }
      writeBudget.recordAttempt();
      if (jobIndexBackend.compareAndSetBatch(chunk.indexBatch(), chunk.extraPointerOps())) {
        for (ReconcileJobIndexStore.JobWritePlan<String> plan : chunk.plans()) {
          expired++;
          ptrDeleted++;
          if (plan.subject() != null && !plan.subject().isBlank()) {
            blobDeleted += deleteJobBlobs(accountId, plan.subject());
          }
        }
        continue;
      }
      for (ReconcileJobIndexStore.JobWritePlan<String> plan : chunk.plans()) {
        if (!writeBudget.canAttempt(clock.getAsLong(), deadline)) {
          break regularChunks;
        }
        writeBudget.recordAttempt();
        if (jobIndexBackend.compareAndSetBatch(plan.indexBatch(), plan.extraPointerOps())) {
          expired++;
          ptrDeleted++;
          if (plan.subject() != null && !plan.subject().isBlank()) {
            blobDeleted += deleteJobBlobs(accountId, plan.subject());
          }
        }
      }
    }
    return new JobCleanupResult(expired, ptrDeleted, blobDeleted, plans.size() - expired);
  }

  private static String rootSummarySortableJobToken(long createdAtMs, String jobId) {
    long created = Math.max(0L, createdAtMs);
    long reversedCreated = Long.MAX_VALUE - created;
    return String.format("%019d-%s", reversedCreated, jobId);
  }

  private void appendPointerDeleteIfPresent(
      java.util.List<PointerStore.CasOp> deletes, String pointerKey) {
    if (pointerKey == null || pointerKey.isBlank() || pointerStore == null) {
      return;
    }
    var existing = pointerStore.get(pointerKey).orElse(null);
    if (existing != null) {
      deletes.add(new PointerStore.CasDelete(existing.getKey(), existing.getVersion()));
    }
  }

  private void appendPointerDeleteIfPresent(
      java.util.LinkedHashMap<String, PointerStore.CasDelete> deletes, String pointerKey) {
    if (pointerKey == null || pointerKey.isBlank() || pointerStore == null) {
      return;
    }
    var existing = pointerStore.get(pointerKey).orElse(null);
    if (existing != null) {
      deletes.putIfAbsent(
          existing.getKey(), new PointerStore.CasDelete(existing.getKey(), existing.getVersion()));
    }
  }

  private boolean deleteJobIndexPointerIfOwned(String pointerKey, String expectedReference) {
    if (pointerKey == null
        || pointerKey.isBlank()
        || expectedReference == null
        || expectedReference.isBlank()) {
      return false;
    }
    var existing = jobIndexBackend.loadIndexEntry(pointerKey).orElse(null);
    if (existing == null || !expectedReference.equals(existing.blobUri())) {
      return false;
    }
    return jobIndexBackend.compareAndSetBatch(
        new ReconcileJobIndexStore.JobIndexWriteBatch(
            List.of(
                new ReconcileJobIndexStore.JobIndexDelete(
                    existing.pointerKey(),
                    existing.version(),
                    expectedReference,
                    existing.lookupStoragePartitionKey())),
            ReconcileJobIndexStore.ReadyQueueMutation.empty()));
  }

  private int deleteJobBlobs(String accountId, String jobId) {
    String prefix = Keys.reconcileJobBlobPrefix(accountId, jobId);
    boolean hadBlob = !blobStore.list(prefix, 1, "").keys().isEmpty();
    blobStore.deletePrefix(prefix);
    return hadBlob ? 1 : 0;
  }

  private StoredReconcileJob storedJob(JsonNode record) {
    if (record == null) {
      return null;
    }
    StoredReconcileJob stored = new StoredReconcileJob();
    stored.jobId = text(record, "jobId");
    stored.accountId = text(record, "accountId");
    stored.connectorId = text(record, "connectorId");
    stored.parentJobId = text(record, "parentJobId");
    stored.state = text(record, "state");
    stored.createdAtMs = longValue(record, "createdAtMs", 0L);
    stored.updatedAtMs = longValue(record, "updatedAtMs", 0L);
    stored.nextAttemptAtMs = longValue(record, "nextAttemptAtMs", 0L);
    stored.laneKey = text(record, "laneKey");
    stored.executionClass = text(record, "executionClass");
    stored.executionLane = text(record, "executionLane");
    stored.pinnedExecutorId = text(record, "pinnedExecutorId");
    stored.jobKind = text(record, "jobKind");
    stored.readyPointerKey = text(record, "readyPointerKey");
    stored.dedupeKeyHash = text(record, "dedupeKeyHash");
    return stored;
  }

  private List<String> currentReadyPointerKeys(JsonNode record) {
    if (record == null) {
      return List.of();
    }
    StoredReconcileJob stored = storedJob(record);
    if (stored == null || !"JS_QUEUED".equals(stored.state)) {
      return List.of();
    }
    if (stored.accountId == null
        || stored.accountId.isBlank()
        || stored.jobId == null
        || stored.jobId.isBlank()
        || stored.laneKey == null
        || stored.laneKey.isBlank()) {
      return List.of();
    }
    long dueAt = readyPointerDueAt(stored);
    if (dueAt == INVALID_ORDERED_POINTER_MS || dueAt <= 0L) {
      dueAt = System.currentTimeMillis();
    }
    return ReadyQueueKeys.readyPointerKeys(stored, dueAt, job -> "JS_QUEUED".equals(job.state));
  }

  private List<String> readyPointerKeysForCleanup(JsonNode record) {
    if (record == null) {
      return List.of();
    }
    StoredReconcileJob stored = storedJob(record);
    if (stored == null) {
      return List.of();
    }
    java.util.LinkedHashSet<String> readyKeys =
        new java.util.LinkedHashSet<>(currentReadyPointerKeys(record));
    boolean hasStoredReadyPointer =
        stored.readyPointerKey != null && !stored.readyPointerKey.isBlank();
    if (hasStoredReadyPointer) {
      readyKeys.add(stored.readyPointerKey);
    }
    boolean shouldReconstructHistoricalReadyKeys =
        hasStoredReadyPointer || !TERMINAL_STATES.contains(stored.state);
    if (shouldReconstructHistoricalReadyKeys) {
      long dueAt = readyPointerDueAt(stored);
      if (dueAt != INVALID_ORDERED_POINTER_MS && dueAt > 0L) {
        if (stored.accountId != null
            && !stored.accountId.isBlank()
            && stored.jobId != null
            && !stored.jobId.isBlank()
            && stored.laneKey != null
            && !stored.laneKey.isBlank()) {
          readyKeys.add(
              Keys.reconcileReadyPointerByDue(
                  dueAt, stored.accountId, stored.laneKey, stored.jobId));
        }
        if (stored.executionClass != null && !stored.executionClass.isBlank()) {
          readyKeys.add(
              Keys.reconcileReadyByExecutionClassPointerByDue(
                  dueAt, stored.executionClass, stored.accountId, stored.jobId));
        }
        String executionLane = stored.executionPolicy().lane();
        if (executionLane != null && !executionLane.isBlank()) {
          readyKeys.add(
              Keys.reconcileReadyByExecutionLanePointerByDue(
                  dueAt, executionLane, stored.accountId, stored.jobId));
        }
        if (stored.pinnedExecutorId != null && !stored.pinnedExecutorId.isBlank()) {
          readyKeys.add(
              Keys.reconcileReadyByPinnedExecutorPointerByDue(
                  dueAt, stored.pinnedExecutorId, stored.accountId, stored.jobId));
        }
        if (stored.jobKind != null && !stored.jobKind.isBlank()) {
          readyKeys.add(
              Keys.reconcileReadyByJobKindPointerByDue(
                  dueAt, stored.jobKind, stored.accountId, stored.jobId));
        }
      }
    }
    readyKeys.removeIf(readyKey -> readyKey == null || readyKey.isBlank());
    return List.copyOf(readyKeys);
  }

  private long readyPointerDueAt(StoredReconcileJob stored) {
    if (stored == null) {
      return INVALID_ORDERED_POINTER_MS;
    }
    if (stored.nextAttemptAtMs > 0L) {
      return stored.nextAttemptAtMs;
    }
    return parseDueMillis(stored.readyPointerKey);
  }

  private long parseDueMillis(String readyPointerKey) {
    return parseTimestampFromOrderedPointer(readyPointerKey, Keys.reconcileReadyPointerPrefix());
  }

  private static long parseLong(String value, long defaultValue) {
    try {
      return Long.parseLong(value);
    } catch (RuntimeException ignored) {
      return defaultValue;
    }
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
}
