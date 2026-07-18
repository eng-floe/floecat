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
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobLegacyMigrationScheduler {
  private static final Logger LOG = Logger.getLogger(ReconcileJobLegacyMigrationScheduler.class);

  @Inject Provider<ReconcileJobGc> reconcileJobGc;
  @Inject Observability observability;

  private GcMetrics metrics;
  private volatile String cleanupToken = "";
  private volatile int cleanupConflictedThisPass;
  private volatile int cleanupRetryableThisPass;
  private volatile boolean cleanupComplete;
  private volatile String lookupToken = "";
  private volatile int lookupMigratedThisPass;
  private volatile int lookupConflictedThisPass;
  private volatile int lookupRetryableThisPass;
  private volatile boolean lookupComplete;

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
    } catch (Throwable ignored) {
      return;
    }

    long started = System.nanoTime();
    try {
      // Keep the small lookup migration moving even if a cleanup page is expensive.
      runLookupSlice(gc);
      runCleanupSlice(gc);
    } catch (Throwable t) {
      metrics.recordError(1, Tag.of(TagKey.RESULT, "tick-failed"));
      LOG.warnf(t, "reconcile job legacy migration tick failed");
    } finally {
      metrics.recordPause(
          Duration.ofNanos(System.nanoTime() - started), Tag.of(TagKey.RESULT, "tick"));
    }
  }

  private void runCleanupSlice(ReconcileJobGc gc) {
    if (cleanupComplete) {
      return;
    }
    var result = gc.runLegacyCleanupMigrationSlice(cleanupToken);
    cleanupConflictedThisPass += result.conflicted();
    cleanupRetryableThisPass += result.retryable();
    cleanupToken = blankToEmpty(result.nextToken());
    metrics.recordCollection(result.scanned(), Tag.of(TagKey.RESULT, "cleanup-scanned"));
    metrics.recordCollection(
        result.manifestsUpdated(), Tag.of(TagKey.RESULT, "cleanup-manifests-updated"));
    metrics.recordCollection(result.conflicted(), Tag.of(TagKey.RESULT, "cleanup-conflicted"));
    metrics.recordCollection(result.retryable(), Tag.of(TagKey.RESULT, "cleanup-retryable"));
    if (!cleanupToken.isBlank()) {
      return;
    }

    int passConflicted = cleanupConflictedThisPass;
    int passRetryable = cleanupRetryableThisPass;
    cleanupConflictedThisPass = 0;
    cleanupRetryableThisPass = 0;
    if (passRetryable == 0 && passConflicted == 0) {
      cleanupComplete = gc.completeLegacyCleanupMigration();
    }
    if (passConflicted > 0) {
      metrics.recordCollection(passConflicted, Tag.of(TagKey.RESULT, "cleanup-blocked-conflicted"));
      LOG.warnf(
          "reconcile job legacy cleanup migration completion blocked by conflicts=%d; affected legacy rows require operator review",
          passConflicted);
    } else if (passRetryable > 0) {
      LOG.warnf(
          "reconcile job legacy cleanup migration pass will retry retryable=%d conflicted=%d",
          passRetryable, passConflicted);
    }
  }

  private void runLookupSlice(ReconcileJobGc gc) {
    if (lookupComplete) {
      return;
    }
    var result = gc.runLegacyLookupMigrationSlice(lookupToken);
    lookupMigratedThisPass += result.migrated();
    lookupConflictedThisPass += result.conflicted();
    lookupRetryableThisPass += result.retryable();
    lookupToken = blankToEmpty(result.nextToken());
    metrics.recordCollection(result.scanned(), Tag.of(TagKey.RESULT, "lookup-scanned"));
    metrics.recordCollection(result.migrated(), Tag.of(TagKey.RESULT, "lookup-migrated"));
    metrics.recordCollection(result.conflicted(), Tag.of(TagKey.RESULT, "lookup-conflicted"));
    metrics.recordCollection(result.retryable(), Tag.of(TagKey.RESULT, "lookup-retryable"));
    if (!lookupToken.isBlank()) {
      return;
    }

    int passMigrated = lookupMigratedThisPass;
    int passConflicted = lookupConflictedThisPass;
    int passRetryable = lookupRetryableThisPass;
    boolean passComplete = result.scanned() == 0 || (passMigrated == 0 && passRetryable == 0);
    lookupMigratedThisPass = 0;
    lookupConflictedThisPass = 0;
    lookupRetryableThisPass = 0;
    lookupComplete = passComplete && gc.completeLegacyLookupMigration();
    if (lookupComplete && passConflicted > 0) {
      metrics.recordCollection(
          passConflicted, Tag.of(TagKey.RESULT, "lookup-completed-conflicted"));
      LOG.warnf(
          "reconcile job legacy lookup migration completed with residual conflicts=%d; affected legacy rows require operator review",
          passConflicted);
    }
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
