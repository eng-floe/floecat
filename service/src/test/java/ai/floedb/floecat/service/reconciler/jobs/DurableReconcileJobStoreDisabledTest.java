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

package ai.floedb.floecat.service.reconciler.jobs;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobQueue;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.reconciler.ReconcileJobQueueTestScope;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

class DurableReconcileJobStoreDisabledTest {
  @Test
  @ResourceLock(ReconcileJobQueueTestScope.LOCK_NAME)
  void initStillReadsStoreConfigurationWhenQueueIsDisabled() {
    try (var ignored = ReconcileJobQueueTestScope.disabled()) {
      var store = new DurableReconcileJobStore();
      store.config = mock(Config.class);
      RuntimeException sentinel = new RuntimeException("config was read");
      when(store.config.getOptionalValue(
              "floecat.reconciler.job-store.max-attempts", Integer.class))
          .thenThrow(sentinel);

      assertSame(sentinel, assertThrows(RuntimeException.class, store::init));
    }
  }

  @Test
  @ResourceLock(ReconcileJobQueueTestScope.LOCK_NAME)
  void enqueueRejectsWorkBeforeAccessingDurableStorage() {
    try (var ignored = ReconcileJobQueueTestScope.disabled()) {
      var store = new DurableReconcileJobStore();

      assertThrows(
          ReconcileJobQueue.DisabledException.class,
          () ->
              store.enqueue(
                  "acct", "conn", false, CaptureMode.METADATA_AND_CAPTURE, ReconcileScope.empty()));
    }
  }
}
