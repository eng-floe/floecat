/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.reconciler.impl;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SnapshotFinalizePublicationSchedulerTest {
  @Test
  void saturatedPageResumesFromTheSameToken() throws Exception {
    var scheduler = new SnapshotFinalizePublicationScheduler();
    scheduler.jobs = mock(ReconcileJobStore.class);
    scheduler.publicationService = mock(LeasedSnapshotFinalizeExecutionService.class);
    List<Runnable> scheduled = new ArrayList<>();
    set(scheduler, "executor", (java.util.concurrent.Executor) scheduled::add);
    set(scheduler, "pageSize", 3);
    set(scheduler, "maxParallelism", 1);
    var first = intent("first");
    var second = intent("second");
    var third = intent("third");
    when(scheduler.jobs.pendingSnapshotFinalizeCommits(3, ""))
        .thenReturn(
            new ReconcileJobStore.SnapshotFinalizeCommitPage(
                List.of(first, second, third), "next-page"),
            new ReconcileJobStore.SnapshotFinalizeCommitPage(List.of(second, third), "next-page"));
    when(scheduler.publicationService.publishAcceptedSnapshotFinalize("first")).thenReturn(true);
    when(scheduler.publicationService.publishAcceptedSnapshotFinalize("second")).thenReturn(true);

    scheduler.tick();
    scheduled.removeFirst().run();
    scheduler.tick();
    scheduled.removeFirst().run();

    verify(scheduler.jobs, times(2)).pendingSnapshotFinalizeCommits(3, "");
    verify(scheduler.publicationService).publishAcceptedSnapshotFinalize("first");
    verify(scheduler.publicationService).publishAcceptedSnapshotFinalize("second");
  }

  @Test
  void transientPublicationFailuresKeepTheAcceptedIntentAndRetry() throws Exception {
    var scheduler = new SnapshotFinalizePublicationScheduler();
    scheduler.jobs = mock(ReconcileJobStore.class);
    scheduler.publicationService = mock(LeasedSnapshotFinalizeExecutionService.class);
    List<Runnable> scheduled = new ArrayList<>();
    set(scheduler, "executor", (java.util.concurrent.Executor) scheduled::add);
    set(scheduler, "pageSize", 1);
    set(scheduler, "maxParallelism", 1);
    var only = intent("only");
    when(scheduler.jobs.pendingSnapshotFinalizeCommits(1, ""))
        .thenReturn(new ReconcileJobStore.SnapshotFinalizeCommitPage(List.of(only), ""));
    when(scheduler.publicationService.publishAcceptedSnapshotFinalize("only"))
        .thenThrow(new IllegalStateException("storage unavailable"));

    // Far more attempts than the previous local retry budget, all transient.
    for (int attempt = 0; attempt < 12; attempt++) {
      scheduler.tick();
      while (!scheduled.isEmpty()) {
        scheduled.removeFirst().run();
      }
      expireRetryBackoff(scheduler);
    }

    verify(scheduler.publicationService, times(12)).publishAcceptedSnapshotFinalize("only");
    verify(scheduler.jobs, never())
        .markFailed(
            anyString(),
            anyString(),
            anyLong(),
            anyString(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong());
    verify(scheduler.jobs, never())
        .markFailedTerminal(
            anyString(),
            anyString(),
            anyLong(),
            anyString(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong());
  }

  @Test
  void invalidPayloadsStillFailTerminally() throws Exception {
    var scheduler = new SnapshotFinalizePublicationScheduler();
    scheduler.jobs = mock(ReconcileJobStore.class);
    scheduler.publicationService = mock(LeasedSnapshotFinalizeExecutionService.class);
    List<Runnable> scheduled = new ArrayList<>();
    set(scheduler, "executor", (java.util.concurrent.Executor) scheduled::add);
    set(scheduler, "pageSize", 1);
    set(scheduler, "maxParallelism", 1);
    when(scheduler.jobs.pendingSnapshotFinalizeCommits(1, ""))
        .thenReturn(new ReconcileJobStore.SnapshotFinalizeCommitPage(List.of(intent("bad")), ""));
    when(scheduler.publicationService.publishAcceptedSnapshotFinalize("bad"))
        .thenThrow(new IllegalArgumentException("manifest mismatch"));

    scheduler.tick();
    scheduled.removeFirst().run();

    verify(scheduler.jobs)
        .markFailedTerminal(
            eq("bad"),
            anyString(),
            anyLong(),
            contains("manifest mismatch"),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong());
  }

  @SuppressWarnings("unchecked")
  private static void expireRetryBackoff(SnapshotFinalizePublicationScheduler scheduler)
      throws Exception {
    Field field = scheduler.getClass().getDeclaredField("retries");
    field.setAccessible(true);
    ((java.util.Map<String, Object>) field.get(scheduler)).clear();
  }

  private static ReconcileJobStore.SnapshotFinalizeCommitIntent intent(String jobId) {
    return new ReconcileJobStore.SnapshotFinalizeCommitIntent(
        jobId, "lease", "result", "/manifest", 1L, "00".repeat(32), 0, 0, 0L, 0L);
  }

  private static void set(Object target, String fieldName, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
