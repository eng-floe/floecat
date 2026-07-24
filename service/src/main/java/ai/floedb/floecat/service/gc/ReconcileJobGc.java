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
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcileJobIndexes;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.CanonicalPointerSnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.JobIndexEntrySnapshot;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexCleanupManifest;
import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexStore;
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
  private static final Set<String> TERMINAL_STATES =
      Set.of("JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED");
  private static final List<String> TERMINAL_BACKFILL_STATES =
      List.of("JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED");
  private static final long INVALID_ORDERED_POINTER_MS = Long.MIN_VALUE;

  @Inject BlobStore blobStore;
  @Inject ObjectMapper mapper;
  @Inject ReconcilerSettingsStore settings;
  @Inject ReconcileJobIndexBackend jobIndexBackend;
  @Inject ReconcileJobIndexStore jobIndexStore;
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
      int readyDeleted,
      int canonicalQuarantined,
      String nextJobToken,
      String nextCanonicalQuarantineToken,
      int retentionScanned,
      int quarantineScanned,
      long retentionNanos,
      long quarantineNanos) {}

  public record LookupMigrationResult(
      int scanned, int migrated, int conflicted, int retryable, String nextToken) {}

  public record RetentionBackfillResult(
      int scanned, int indexed, int retryable, String nextToken, boolean complete) {}

  public boolean terminalRetentionBackfillComplete(String accountId) {
    return pointerStore != null
        && pointerStore.get(Keys.reconcileTerminalRetentionBackfillPointer(accountId)).isPresent();
  }

  public RetentionBackfillResult runTerminalRetentionBackfillSlice(
      String accountId, String pageTokenIn) {
    return runTerminalRetentionBackfillSlice(accountId, pageTokenIn, Long.MAX_VALUE);
  }

  public RetentionBackfillResult runTerminalRetentionBackfillSlice(
      String accountId, String pageTokenIn, long deadlineMs) {
    if (terminalRetentionBackfillComplete(accountId)) {
      return new RetentionBackfillResult(0, 0, 0, "", true);
    }
    int pageSize =
        ConfigProvider.getConfig()
            .getOptionalValue(
                "floecat.gc.reconcile-jobs.retention-backfill-page-size", Integer.class)
            .orElse(100);
    RetentionBackfillCursor cursor = decodeRetentionBackfillCursor(pageTokenIn);
    int scanned = 0;
    int indexed = 0;
    int retryable = 0;
    String nextToken = encodeRetentionBackfillCursor(cursor);
    boolean complete = false;
    while (cursor.stateIndex() < TERMINAL_BACKFILL_STATES.size()) {
      String pageStartToken = cursor.pageToken();
      var page =
          jobIndexBackend.listAccountStateEntries(
              accountId,
              TERMINAL_BACKFILL_STATES.get(cursor.stateIndex()),
              Math.max(1, pageSize),
              pageStartToken);
      String lastProcessedToken = pageStartToken;
      int pageScanned = 0;
      for (var stateEntry : page.entries()) {
        if (scanned >= Math.max(1, pageSize) || (scanned > 0 && clock.getAsLong() >= deadlineMs)) {
          break;
        }
        scanned++;
        pageScanned++;
        JobIndexEntrySnapshot canonical =
            jobIndexBackend.loadIndexEntry(stateEntry.blobUri()).orElse(null);
        if (canonical == null) {
          lastProcessedToken = stateEntry.pointerKey();
          continue;
        }
        JsonNode record = readRecordByReference(canonical.blobUri());
        if (record == null) {
          lastProcessedToken = stateEntry.pointerKey();
          continue;
        }
        StoredReconcileJob storedRecord;
        try {
          storedRecord = mapper.treeToValue(record, StoredReconcileJob.class);
        } catch (Exception invalidRecord) {
          lastProcessedToken = stateEntry.pointerKey();
          continue;
        }
        var result =
            jobIndexStore.backfillTerminalRetentionIndex(
                new CanonicalPointerSnapshot(
                    canonical.pointerKey(), canonical.blobUri(), canonical.version()),
                storedRecord);
        if (result.updated()) {
          indexed++;
        }
        if (result.retryable()) {
          retryable++;
          nextToken =
              encodeRetentionBackfillCursor(
                  new RetentionBackfillCursor(cursor.stateIndex(), pageStartToken));
          break;
        }
        lastProcessedToken = stateEntry.pointerKey();
      }
      if (retryable > 0) {
        break;
      }
      if (pageScanned < page.entries().size()) {
        nextToken =
            encodeRetentionBackfillCursor(
                new RetentionBackfillCursor(cursor.stateIndex(), lastProcessedToken));
        break;
      }
      String pageNextToken = page.nextPageToken() == null ? "" : page.nextPageToken();
      if (!pageNextToken.isBlank()) {
        nextToken =
            encodeRetentionBackfillCursor(
                new RetentionBackfillCursor(cursor.stateIndex(), pageNextToken));
        break;
      }
      cursor = new RetentionBackfillCursor(cursor.stateIndex() + 1, "");
      nextToken = encodeRetentionBackfillCursor(cursor);
      if (clock.getAsLong() >= deadlineMs
          && cursor.stateIndex() < TERMINAL_BACKFILL_STATES.size()) {
        break;
      }
    }
    complete =
        cursor.stateIndex() >= TERMINAL_BACKFILL_STATES.size()
            && retryable == 0
            && nextToken.isBlank();
    if (complete) {
      String markerKey = Keys.reconcileTerminalRetentionBackfillPointer(accountId);
      Pointer marker = PointerReferences.opaqueMarkerPointer(markerKey, "v1", 1L);
      complete =
          pointerStore.compareAndSet(markerKey, 0L, marker)
              || pointerStore.get(markerKey).isPresent();
      if (!complete) {
        nextToken = encodeRetentionBackfillCursor(cursor);
      }
    }
    return new RetentionBackfillResult(scanned, indexed, retryable, nextToken, complete);
  }

  private record RetentionBackfillCursor(int stateIndex, String pageToken) {}

  private static RetentionBackfillCursor decodeRetentionBackfillCursor(String token) {
    if (token == null || token.isBlank()) {
      return new RetentionBackfillCursor(0, "");
    }
    int separator = token.indexOf(':');
    if (separator <= 0) {
      return new RetentionBackfillCursor(0, token);
    }
    try {
      int stateIndex = Integer.parseInt(token.substring(0, separator));
      String pageToken =
          new String(
              Base64.getUrlDecoder().decode(token.substring(separator + 1)),
              StandardCharsets.UTF_8);
      return new RetentionBackfillCursor(
          Math.max(0, Math.min(stateIndex, TERMINAL_BACKFILL_STATES.size())), pageToken);
    } catch (RuntimeException invalidToken) {
      return new RetentionBackfillCursor(0, "");
    }
  }

  private static String encodeRetentionBackfillCursor(RetentionBackfillCursor cursor) {
    if (cursor == null || cursor.stateIndex() >= TERMINAL_BACKFILL_STATES.size()) {
      return "";
    }
    String encodedPageToken =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(cursor.pageToken().getBytes(StandardCharsets.UTF_8));
    return cursor.stateIndex() + ":" + encodedPageToken;
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

  private record JobCleanupResult(
      int expired, int ptrDeleted, int blobDeleted, int readyDeleted, int failed) {}

  private static final class CleanupWriteBudget {
    private boolean attempted;

    private boolean canAttempt(long nowMs, long deadline) {
      return !attempted || nowMs < deadline;
    }

    private void recordAttempt() {
      attempted = true;
    }
  }

  public AccountResult runAccountSlice(
      String accountId, String jobTokenIn, String canonicalQuarantineTokenIn) {
    return runAccountSlice(accountId, jobTokenIn, canonicalQuarantineTokenIn, Long.MAX_VALUE);
  }

  public AccountResult runAccountSlice(
      String accountId,
      String jobTokenIn,
      String canonicalQuarantineTokenIn,
      long absoluteDeadlineMs) {
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
    final long sliceDeadline =
        sliceMillis >= Long.MAX_VALUE - nowMs ? Long.MAX_VALUE : nowMs + sliceMillis;
    final long deadline =
        absoluteDeadlineMs <= 0L ? sliceDeadline : Math.min(sliceDeadline, absoluteDeadlineMs);

    String jobToken = jobTokenIn == null ? "" : jobTokenIn;
    String canonicalQuarantineToken =
        canonicalQuarantineTokenIn == null ? "" : canonicalQuarantineTokenIn;

    int scanned = 0;
    int expired = 0;
    int ptrDeleted = 0;
    int blobDeleted = 0;
    int readyDeleted = 0;
    int canonicalQuarantined = 0;
    int retentionScanned = 0;
    int quarantineScanned = 0;

    List<ReconcileJobIndexStore.JobWritePlan<String>> deletePlans = new ArrayList<>();
    List<ReconcileJobIndexStore.JobWritePlan<String>> quarantinedDeletePlans = new ArrayList<>();
    java.util.Set<String> quarantineMarkersCreatedThisSlice = new java.util.HashSet<>();
    String cleanupRetryToken = null;
    long retentionCutoffMs = nowMs - retentionMs;
    long retentionStartedNanos = System.nanoTime();
    while (scanned < batchLimit && clock.getAsLong() < deadline) {
      int limit = Math.min(pageSize, batchLimit - scanned);
      String jobPageStartToken = jobToken;
      var page =
          jobIndexBackend.listTerminalRetentionEntries(
              accountId, retentionCutoffMs, limit, jobToken);
      var pointers = page.entries();
      jobToken = page.nextPageToken();
      if (pointers.isEmpty()) {
        jobToken = "";
        break;
      }
      boolean partialPage = false;
      int preparedInPage = 0;
      String lastPreparedJobToken = jobPageStartToken;
      for (var retentionEntry : pointers) {
        long terminalAtMs =
            parseTimestampFromOrderedPointer(
                retentionEntry.pointerKey(),
                Keys.reconcileTerminalRetentionPointerPrefix(accountId));
        if (terminalAtMs == INVALID_ORDERED_POINTER_MS) {
          if (deleteJobIndexPointerIfOwned(retentionEntry.pointerKey(), retentionEntry.blobUri())) {
            ptrDeleted++;
          }
          continue;
        }
        if (terminalAtMs > retentionCutoffMs) {
          // The index is ordered by terminal time. No later row can be due, and the empty cursor
          // intentionally starts the next tick at the oldest entry again.
          jobToken = "";
          partialPage = true;
          break;
        }
        if (scanned >= batchLimit || (preparedInPage > 0 && clock.getAsLong() >= deadline)) {
          partialPage = true;
          break;
        }
        scanned++;
        retentionScanned++;
        preparedInPage++;
        lastPreparedJobToken = retentionEntry.pointerKey();
        var canonical = jobIndexBackend.loadIndexEntry(retentionEntry.blobUri()).orElse(null);
        if (canonical == null) {
          if (deleteJobIndexPointerIfOwned(retentionEntry.pointerKey(), retentionEntry.blobUri())) {
            ptrDeleted++;
          }
          continue;
        }
        String jobId =
            decodeJobId(Keys.reconcileJobPointerByIdPrefix(accountId), canonical.pointerKey());
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
            String markerKey =
                Keys.reconcileCanonicalQuarantinePointer(
                    accountId, hashValue(canonical.pointerKey()));
            boolean markerAlreadyExisted =
                pointerStore != null && pointerStore.get(markerKey).isPresent();
            ReconcileJobIndexStore.JobWritePlan<String> deletePlan =
                buildQuarantinedCanonicalDeletePlan(
                    accountId, jobId, canonical, nowMs, canonicalQuarantineRetentionMs);
            if (deletePlan == null && !markerAlreadyExisted) {
              quarantineMarkersCreatedThisSlice.add(markerKey);
              canonicalQuarantined++;
            }
            if (deletePlan != null) {
              quarantinedDeletePlans.add(deletePlan);
              if (cleanupRetryToken == null) {
                cleanupRetryToken = jobPageStartToken;
              }
            }
          } else {
            String state = text(record, "state");
            StoredReconcileJob stored = storedJob(record);
            String expectedRetentionKey =
                stored == null ? "" : jobIndexes.terminalRetentionPointerKey(stored);
            if (!TERMINAL_STATES.contains(state)
                || !retentionEntry.pointerKey().equals(expectedRetentionKey)) {
              if (deleteJobIndexPointerIfOwned(
                  retentionEntry.pointerKey(), retentionEntry.blobUri())) {
                ptrDeleted++;
              }
              continue;
            }
            ReconcileJobIndexStore.JobWritePlan<String> deletePlan =
                buildCanonicalFootprintDeletePlan(accountId, jobId, canonical, record);
            if (deletePlan != null) {
              deletePlans.add(deletePlan);
              if (cleanupRetryToken == null) {
                cleanupRetryToken = jobPageStartToken;
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
      if (partialPage || jobToken.isBlank()) {
        break;
      }
    }

    CleanupWriteBudget cleanupWriteBudget = new CleanupWriteBudget();
    JobCleanupResult cleanup =
        deleteCanonicalFootprints(accountId, deletePlans, deadline, cleanupWriteBudget);
    expired += cleanup.expired();
    ptrDeleted += cleanup.ptrDeleted();
    blobDeleted += cleanup.blobDeleted();
    readyDeleted += cleanup.readyDeleted();

    JobCleanupResult quarantinedCleanup =
        deleteCanonicalFootprints(accountId, quarantinedDeletePlans, deadline, cleanupWriteBudget);
    expired += quarantinedCleanup.expired();
    ptrDeleted += quarantinedCleanup.ptrDeleted();
    blobDeleted += quarantinedCleanup.blobDeleted();
    readyDeleted += quarantinedCleanup.readyDeleted();
    canonicalQuarantined += quarantinedCleanup.failed();

    if ((cleanup.failed() > 0 || quarantinedCleanup.failed() > 0) && cleanupRetryToken != null) {
      jobToken = cleanupRetryToken;
    }
    long retentionNanos = System.nanoTime() - retentionStartedNanos;

    long quarantineStartedNanos = System.nanoTime();
    List<ReconcileJobIndexStore.JobWritePlan<String>> quarantineMarkerDeletePlans =
        new ArrayList<>();
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
        quarantineScanned++;
        if (quarantineMarkersCreatedThisSlice.contains(marker.getKey())) {
          continue;
        }
        String canonicalKey = quarantineMarkerCanonicalKey(marker.getBlobUri());
        var canonical =
            canonicalKey.isBlank()
                ? null
                : jobIndexBackend.loadIndexEntry(canonicalKey).orElse(null);
        if (canonical == null) {
          pointerStore.compareAndDelete(marker.getKey(), marker.getVersion());
          continue;
        }
        if (readRecordByReference(canonical.blobUri()) != null
            && (!canonical.cleanupLocked()
                || !markerPayloadMatches(marker.getBlobUri(), canonical))) {
          pointerStore.compareAndDelete(marker.getKey(), marker.getVersion());
          continue;
        }
        String jobId =
            decodeJobId(Keys.reconcileJobPointerByIdPrefix(accountId), canonical.pointerKey());
        var deletePlan =
            canonical.cleanupLocked()
                ? buildLockedCanonicalFootprintDeletePlan(accountId, jobId, canonical)
                : buildQuarantinedCanonicalDeletePlan(
                    accountId, jobId, canonical, nowMs, canonicalQuarantineRetentionMs);
        if (deletePlan != null) {
          quarantineMarkerDeletePlans.add(deletePlan);
        } else {
          canonicalQuarantined++;
        }
      }
      if (canonicalQuarantineToken.isBlank()) {
        break;
      }
    }
    JobCleanupResult markerCleanup =
        deleteCanonicalFootprints(
            accountId, quarantineMarkerDeletePlans, deadline, cleanupWriteBudget);
    expired += markerCleanup.expired();
    ptrDeleted += markerCleanup.ptrDeleted();
    blobDeleted += markerCleanup.blobDeleted();
    readyDeleted += markerCleanup.readyDeleted();
    canonicalQuarantined += markerCleanup.failed();
    long quarantineNanos = System.nanoTime() - quarantineStartedNanos;

    return new AccountResult(
        scanned,
        expired,
        ptrDeleted,
        blobDeleted,
        readyDeleted,
        canonicalQuarantined,
        jobToken,
        canonicalQuarantineToken,
        retentionScanned,
        quarantineScanned,
        retentionNanos,
        quarantineNanos);
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
    ReconcileJobIndexStore.JobIndexWriteBatch deleteBatch =
        jobIndexStore.buildJobDeleteBatch(
            new CanonicalPointerSnapshot(
                canonical.pointerKey(), canonical.blobUri(), canonical.version()));
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
    int readyDeleted = 0;
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
        readyDeleted += plan.indexBatch().readyMutation().deletes().size();
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
          readyDeleted += plan.indexBatch().readyMutation().deletes().size();
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
          readyDeleted += plan.indexBatch().readyMutation().deletes().size();
          if (plan.subject() != null && !plan.subject().isBlank()) {
            blobDeleted += deleteJobBlobs(accountId, plan.subject());
          }
        }
      }
    }
    return new JobCleanupResult(
        expired, ptrDeleted, blobDeleted, readyDeleted, plans.size() - expired);
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
