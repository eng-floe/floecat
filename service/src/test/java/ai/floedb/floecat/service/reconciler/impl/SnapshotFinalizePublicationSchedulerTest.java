/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.reconciler.impl;

import static org.mockito.Mockito.mock;
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
