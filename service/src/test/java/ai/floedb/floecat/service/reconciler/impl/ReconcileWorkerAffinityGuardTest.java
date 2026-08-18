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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileWorkerAffinity;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReconcileWorkerAffinityGuardTest {
  private ReconcileWorkerAffinityGuard guard;

  @BeforeEach
  void setUp() {
    guard = new ReconcileWorkerAffinityGuard();
    guard.jobs = mock(ReconcileJobStore.class);
    guard.configuredAffinity = "reconciler-v1";
  }

  @Test
  void acceptsMatchingLeaseRequest() {
    assertDoesNotThrow(() -> guard.requireLeaseRequestAffinity("reconciler-v1"));
  }

  @Test
  void rejectsMismatchedLeaseRequest() {
    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class, () -> guard.requireLeaseRequestAffinity("reconciler-v2"));

    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
  }

  @Test
  void rejectsUnversionedLeaseRequest() {
    StatusRuntimeException error =
        assertThrows(StatusRuntimeException.class, () -> guard.requireLeaseRequestAffinity(""));

    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
  }

  @Test
  void rejectsLeaseBoundCallRoutedThroughAnotherCohort() {
    when(guard.jobs.getCompactLeaseView("job-1"))
        .thenReturn(
            Optional.of(
                job(
                    ReconcileWorkerAffinity.of("reconciler-v2")
                        .applyTo(ReconcileExecutionPolicy.defaults()))));

    StatusRuntimeException error =
        assertThrows(StatusRuntimeException.class, () -> guard.requireJobAffinity("job-1"));

    assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
  }

  @Test
  void acceptsLeaseBoundCallForThisCohort() {
    when(guard.jobs.getCompactLeaseView("job-1"))
        .thenReturn(
            Optional.of(
                job(
                    ReconcileWorkerAffinity.of("reconciler-v1")
                        .applyTo(ReconcileExecutionPolicy.defaults()))));

    assertDoesNotThrow(() -> guard.requireJobAffinity("job-1"));
  }

  private static ReconcileJobStore.ReconcileJob job(ReconcileExecutionPolicy policy) {
    return new ReconcileJobStore.ReconcileJob(
        "job-1",
        "acct",
        "connector",
        "JS_LEASED",
        "",
        0L,
        0L,
        0L,
        0L,
        0L,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        ReconcileScope.empty(),
        policy,
        "floescan_ingest");
  }
}
