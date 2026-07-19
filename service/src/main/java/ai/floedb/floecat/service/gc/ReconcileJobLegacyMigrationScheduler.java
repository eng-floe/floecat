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

import ai.floedb.floecat.service.reconciler.jobs.durable.store.ReconcileJobIndexBackend;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.helpers.GcMetrics;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import java.time.Duration;
import java.util.UUID;
import java.util.function.LongSupplier;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobLegacyMigrationScheduler {
  private static final Logger LOG = Logger.getLogger(ReconcileJobLegacyMigrationScheduler.class);

  @Inject Provider<ReconcileJobGc> reconcileJobGc;
  @Inject Observability observability;

  @ConfigProperty(
      name = "floecat.gc.reconcile-jobs.legacy-migration.lease-duration",
      defaultValue = "PT5M")
  Duration leaseDuration = Duration.ofMinutes(5);

  private GcMetrics metrics;
  private volatile boolean cleanupComplete;
  private volatile boolean lookupComplete;
  String ownerId = UUID.randomUUID().toString();
  LongSupplier currentTimeMillis = System::currentTimeMillis;

  @PostConstruct
  void initMeters() {
    metrics =
        new GcMetrics(
            observability,
            "service",
            "gc.reconcile-job-legacy-migration",
            "reconcile-job-legacy-migration");
  }

  @Scheduled(
      every = "{floecat.gc.reconcile-jobs.legacy-migration.tick-every}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
      skipExecutionIf = ReconcileJobGcScheduler.DisabledOrStopping.class)
  void tick() {
    final ReconcileJobGc gc;
    try {
      gc = reconcileJobGc.get();
    } catch (Throwable t) {
      if (metrics != null) {
        metrics.recordError(1, Tag.of(TagKey.RESULT, "gc-resolution-failed"));
      }
      LOG.warnf(t, "reconcile job legacy migration could not resolve reconcile job GC");
      return;
    }

    long started = System.nanoTime();
    try {
      // Keep the small lookup migration moving even if a cleanup page is expensive.
      try {
        runLookupSlice(gc);
      } catch (Throwable t) {
        metrics.recordError(1, Tag.of(TagKey.RESULT, "lookup-failed"));
        LOG.warnf(t, "reconcile job legacy lookup migration tick failed");
      }
      try {
        runCleanupSlice(gc);
      } catch (Throwable t) {
        metrics.recordError(1, Tag.of(TagKey.RESULT, "cleanup-failed"));
        LOG.warnf(t, "reconcile job legacy cleanup migration tick failed");
      }
    } finally {
      metrics.recordPause(
          Duration.ofNanos(System.nanoTime() - started), Tag.of(TagKey.RESULT, "tick"));
    }
  }

  private void runCleanupSlice(ReconcileJobGc gc) {
    if (cleanupComplete) {
      return;
    }
    if (!gc.legacyMigrationComplete(ReconcileJobIndexBackend.LegacyMigration.LOOKUP)) {
      return;
    }
    if (gc.legacyMigrationComplete(ReconcileJobIndexBackend.LegacyMigration.CLEANUP)) {
      cleanupComplete = true;
      return;
    }
    long nowMs = currentTimeMillis.getAsLong();
    long leaseDurationMs = Math.max(1L, leaseDuration.toMillis());
    var lease =
        gc.acquireLegacyMigrationLease(
                ReconcileJobIndexBackend.LegacyMigration.CLEANUP, ownerId, nowMs, leaseDurationMs)
            .orElse(null);
    if (lease == null) {
      return;
    }
    var stored = lease.progress();
    if (stored.quietPassComplete()) {
      cleanupComplete =
          gc.completeLegacyMigration(
              ReconcileJobIndexBackend.LegacyMigration.CLEANUP,
              ownerId,
              lease.fence(),
              currentTimeMillis.getAsLong());
      if (cleanupComplete) {
        recordCleanupResiduals(stored.unresolvable(), stored.conflicted());
      }
      return;
    }

    // Persist a conservative change before doing side effects. If the slice succeeds, the
    // checkpoint below replaces it with the real count. If the process or lease is lost, the
    // sentinel survives and forces a full quiet verification pass.
    var inFlight =
        new ReconcileJobIndexBackend.LegacyMigrationProgress(
            stored.pageToken(),
            Math.max(1, stored.changed()),
            stored.unresolvable(),
            stored.conflicted(),
            stored.retryable(),
            false);
    if (!gc.checkpointLegacyMigration(
        ReconcileJobIndexBackend.LegacyMigration.CLEANUP,
        ownerId,
        lease.fence(),
        inFlight,
        currentTimeMillis.getAsLong(),
        leaseDurationMs)) {
      return;
    }

    var result = gc.runLegacyCleanupMigrationSlice(stored.pageToken());
    int passUnresolvable = add(stored.unresolvable(), result.unresolvable());
    int passConflicted = add(stored.conflicted(), result.conflicted());
    int passRetryable = add(stored.retryable(), result.retryable());
    int passChanged =
        add(stored.changed(), add(result.manifestsUpdated(), result.indexesBackfilled()));
    String nextToken = blankToEmpty(result.nextToken());
    metrics.recordCollection(result.scanned(), Tag.of(TagKey.RESULT, "cleanup-scanned"));
    metrics.recordCollection(
        result.manifestsUpdated(), Tag.of(TagKey.RESULT, "cleanup-manifests-updated"));
    metrics.recordCollection(
        result.indexesBackfilled(), Tag.of(TagKey.RESULT, "cleanup-indexes-backfilled"));
    metrics.recordCollection(result.unresolvable(), Tag.of(TagKey.RESULT, "cleanup-unresolvable"));
    metrics.recordCollection(result.conflicted(), Tag.of(TagKey.RESULT, "cleanup-conflicted"));
    metrics.recordCollection(result.retryable(), Tag.of(TagKey.RESULT, "cleanup-retryable"));
    boolean passEnded = nextToken.isBlank();
    boolean quietPass = passEnded && passRetryable == 0 && passChanged == 0;
    var checkpoint =
        passEnded && !quietPass
            ? ReconcileJobIndexBackend.LegacyMigrationProgress.empty()
            : new ReconcileJobIndexBackend.LegacyMigrationProgress(
                nextToken, passChanged, passUnresolvable, passConflicted, passRetryable, quietPass);
    boolean checkpointed =
        gc.checkpointLegacyMigration(
            ReconcileJobIndexBackend.LegacyMigration.CLEANUP,
            ownerId,
            lease.fence(),
            checkpoint,
            currentTimeMillis.getAsLong(),
            leaseDurationMs);
    if (!checkpointed) {
      return;
    }
    if (quietPass) {
      cleanupComplete =
          gc.completeLegacyMigration(
              ReconcileJobIndexBackend.LegacyMigration.CLEANUP,
              ownerId,
              lease.fence(),
              currentTimeMillis.getAsLong());
      if (cleanupComplete) {
        recordCleanupResiduals(passUnresolvable, passConflicted);
      }
    }
    if (!passEnded) {
      return;
    }
    if (passRetryable > 0) {
      LOG.warnf(
          "reconcile job legacy cleanup migration pass will retry retryable=%d conflicted=%d",
          passRetryable, passConflicted);
    } else if (passChanged > 0) {
      LOG.infof(
          "reconcile job legacy cleanup migration changed rows=%d; running a quiet verification pass",
          passChanged);
    }
  }

  private void recordCleanupResiduals(int unresolvable, int conflicted) {
    if (unresolvable > 0) {
      metrics.recordCollection(
          unresolvable, Tag.of(TagKey.RESULT, "cleanup-completed-unresolvable"));
      LOG.warnf(
          "reconcile job legacy cleanup migration completed with unresolvable rows=%d; affected rows require operator review",
          unresolvable);
    }
    if (conflicted > 0) {
      metrics.recordCollection(conflicted, Tag.of(TagKey.RESULT, "cleanup-completed-conflicted"));
      LOG.warnf(
          "reconcile job legacy cleanup migration completed with conflicts=%d; affected legacy rows require operator review",
          conflicted);
    }
  }

  private void runLookupSlice(ReconcileJobGc gc) {
    if (lookupComplete) {
      return;
    }
    if (gc.legacyMigrationComplete(ReconcileJobIndexBackend.LegacyMigration.LOOKUP)) {
      lookupComplete = true;
      return;
    }
    long nowMs = currentTimeMillis.getAsLong();
    long leaseDurationMs = Math.max(1L, leaseDuration.toMillis());
    var lease =
        gc.acquireLegacyMigrationLease(
                ReconcileJobIndexBackend.LegacyMigration.LOOKUP, ownerId, nowMs, leaseDurationMs)
            .orElse(null);
    if (lease == null) {
      return;
    }
    var stored = lease.progress();
    if (stored.quietPassComplete()) {
      lookupComplete =
          gc.completeLegacyMigration(
              ReconcileJobIndexBackend.LegacyMigration.LOOKUP,
              ownerId,
              lease.fence(),
              currentTimeMillis.getAsLong());
      if (lookupComplete && stored.conflicted() > 0) {
        recordLookupConflicts(stored.conflicted());
      }
      return;
    }

    // See the cleanup path above. This closes the mutation-succeeded/checkpoint-lost window.
    var inFlight =
        new ReconcileJobIndexBackend.LegacyMigrationProgress(
            stored.pageToken(),
            Math.max(1, stored.changed()),
            0,
            stored.conflicted(),
            stored.retryable(),
            false);
    if (!gc.checkpointLegacyMigration(
        ReconcileJobIndexBackend.LegacyMigration.LOOKUP,
        ownerId,
        lease.fence(),
        inFlight,
        currentTimeMillis.getAsLong(),
        leaseDurationMs)) {
      return;
    }

    var result = gc.runLegacyLookupMigrationSlice(stored.pageToken());
    int passMigrated = add(stored.changed(), result.migrated());
    int passConflicted = add(stored.conflicted(), result.conflicted());
    int passRetryable = add(stored.retryable(), result.retryable());
    String nextToken = blankToEmpty(result.nextToken());
    metrics.recordCollection(result.scanned(), Tag.of(TagKey.RESULT, "lookup-scanned"));
    metrics.recordCollection(result.migrated(), Tag.of(TagKey.RESULT, "lookup-migrated"));
    metrics.recordCollection(result.conflicted(), Tag.of(TagKey.RESULT, "lookup-conflicted"));
    metrics.recordCollection(result.retryable(), Tag.of(TagKey.RESULT, "lookup-retryable"));
    boolean passEnded = nextToken.isBlank();
    boolean quietPass = passEnded && passMigrated == 0 && passRetryable == 0;
    var checkpoint =
        passEnded && !quietPass
            ? ReconcileJobIndexBackend.LegacyMigrationProgress.empty()
            : new ReconcileJobIndexBackend.LegacyMigrationProgress(
                nextToken, passMigrated, 0, passConflicted, passRetryable, quietPass);
    boolean checkpointed =
        gc.checkpointLegacyMigration(
            ReconcileJobIndexBackend.LegacyMigration.LOOKUP,
            ownerId,
            lease.fence(),
            checkpoint,
            currentTimeMillis.getAsLong(),
            leaseDurationMs);
    if (!checkpointed) {
      return;
    }
    if (quietPass) {
      lookupComplete =
          gc.completeLegacyMigration(
              ReconcileJobIndexBackend.LegacyMigration.LOOKUP,
              ownerId,
              lease.fence(),
              currentTimeMillis.getAsLong());
    }
    if (lookupComplete && passConflicted > 0) {
      recordLookupConflicts(passConflicted);
    }
  }

  private void recordLookupConflicts(int conflicts) {
    metrics.recordCollection(conflicts, Tag.of(TagKey.RESULT, "lookup-completed-conflicted"));
    LOG.warnf(
        "reconcile job legacy lookup migration completed with residual conflicts=%d; affected legacy rows require operator review",
        conflicts);
  }

  private static int add(int left, int right) {
    long sum = (long) left + right;
    return sum >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) Math.max(0L, sum);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
