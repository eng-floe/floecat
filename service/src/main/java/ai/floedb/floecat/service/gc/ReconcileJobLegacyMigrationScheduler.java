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
    } finally {
      metrics.recordPause(
          Duration.ofNanos(System.nanoTime() - started), Tag.of(TagKey.RESULT, "tick"));
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
      recordCheckpointRejected(
          ReconcileJobIndexBackend.LegacyMigration.LOOKUP,
          "pre-slice",
          lease.fence(),
          leaseDurationMs);
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
      recordCheckpointRejected(
          ReconcileJobIndexBackend.LegacyMigration.LOOKUP,
          "post-slice",
          lease.fence(),
          leaseDurationMs);
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

  private void recordCheckpointRejected(
      ReconcileJobIndexBackend.LegacyMigration migration,
      String phase,
      long fence,
      long leaseDurationMs) {
    String migrationName = "lookup";
    metrics.recordError(
        1, Tag.of(TagKey.RESULT, migrationName + "-" + phase + "-checkpoint-rejected"));
    LOG.warnf(
        "reconcile job legacy %s migration %s checkpoint was rejected owner=%s fence=%d lease-duration-ms=%d; migration work will be retried; if this repeats, increase the lease duration or reduce the migration page size",
        migrationName, phase, ownerId, fence, leaseDurationMs);
  }

  private static int add(int left, int right) {
    long sum = (long) left + right;
    return sum >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) Math.max(0L, sum);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
