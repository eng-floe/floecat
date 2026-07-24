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

package ai.floedb.floecat.service.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import java.util.List;
import org.junit.jupiter.api.Test;

class SnapshotFinalizeChildStateServiceTest {

  @Test
  void compactStateReadsTheChildIndexOnce() {
    ReconcileJobStore jobs = mock(ReconcileJobStore.class);
    SnapshotFinalizeChildStateService service = new SnapshotFinalizeChildStateService();
    service.jobs = jobs;
    when(jobs.childJobStatesPage("acct", "parent", 200, ""))
        .thenReturn(new ReconcileJobStore.ChildJobStatePage(List.of(), ""));

    var state = service.compactChildState("acct", "parent", "finalizer", 0);

    assertEquals(0, state.completedGroups());
    verify(jobs).childJobStatesPage("acct", "parent", 200, "");
    verify(jobs, never()).childJobsPage("acct", "parent", 200, "");
    verify(jobs, never()).childFileGroupResultDescriptorsPage("acct", "parent", 200, "");
  }
}
