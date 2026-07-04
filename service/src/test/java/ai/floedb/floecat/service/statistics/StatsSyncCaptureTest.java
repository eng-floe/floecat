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

package ai.floedb.floecat.service.statistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.stats.spi.StatsSyncOutcome;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StatsSyncCaptureTest {

  private static final ReconcileScope SCOPE =
      ReconcileScope.of(java.util.List.of(), "table-1", java.util.List.of(), null);

  @Test
  void returnsCapturedWhenJobSucceeds() {
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-1");
    when(jobStore.get("acct", "job-1")).thenReturn(Optional.of(job("JS_SUCCEEDED")));

    StatsSyncCapture capture = new StatsSyncCapture(jobStore);
    StatsSyncOutcome outcome = capture.capture("acct", "conn-1", SCOPE, Duration.ofSeconds(5));

    assertThat(outcome).isEqualTo(StatsSyncOutcome.CAPTURED);
  }

  @Test
  void returnsFailedWhenJobFails() {
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-1");
    when(jobStore.get("acct", "job-1")).thenReturn(Optional.of(job("JS_FAILED")));

    StatsSyncCapture capture = new StatsSyncCapture(jobStore);
    StatsSyncOutcome outcome = capture.capture("acct", "conn-1", SCOPE, Duration.ofSeconds(5));

    assertThat(outcome).isEqualTo(StatsSyncOutcome.FAILED);
  }

  @Test
  void returnsFailedWhenJobCancelled() {
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-1");
    when(jobStore.get("acct", "job-1")).thenReturn(Optional.of(job("JS_CANCELLED")));

    StatsSyncCapture capture = new StatsSyncCapture(jobStore);
    StatsSyncOutcome outcome = capture.capture("acct", "conn-1", SCOPE, Duration.ofSeconds(5));

    assertThat(outcome).isEqualTo(StatsSyncOutcome.FAILED);
  }

  @Test
  void waitsForTerminalCancellationBeforeFailing() {
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-1");
    when(jobStore.get("acct", "job-1"))
        .thenReturn(Optional.of(job("JS_CANCELLING")))
        .thenReturn(Optional.of(job("JS_CANCELLED")));

    StatsSyncCapture capture = new StatsSyncCapture(jobStore);
    StatsSyncOutcome outcome = capture.capture("acct", "conn-1", SCOPE, Duration.ofMillis(250));

    assertThat(outcome).isEqualTo(StatsSyncOutcome.FAILED);
    verify(jobStore, times(2)).get("acct", "job-1");
  }

  @Test
  void returnsFailedWhenEnqueueThrows() {
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenThrow(new RuntimeException("store unavailable"));

    StatsSyncCapture capture = new StatsSyncCapture(jobStore);
    StatsSyncOutcome outcome = capture.capture("acct", "conn-1", SCOPE, Duration.ofSeconds(5));

    assertThat(outcome).isEqualTo(StatsSyncOutcome.FAILED);
  }

  @Test
  void returnsFailedWhenJobDisappears() {
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-1");
    when(jobStore.get("acct", "job-1")).thenReturn(Optional.empty());

    StatsSyncCapture capture = new StatsSyncCapture(jobStore);
    StatsSyncOutcome outcome = capture.capture("acct", "conn-1", SCOPE, Duration.ofSeconds(5));

    assertThat(outcome).isEqualTo(StatsSyncOutcome.FAILED);
  }

  @Test
  void returnsTimeoutWhenBudgetExhaustedBeforeTerminal() {
    ReconcileJobStore jobStore = Mockito.mock(ReconcileJobStore.class);
    when(jobStore.enqueue(anyString(), anyString(), anyBoolean(), any(), any()))
        .thenReturn("job-1");
    // Job stays running indefinitely.
    when(jobStore.get("acct", "job-1")).thenReturn(Optional.of(job("JS_RUNNING")));

    StatsSyncCapture capture = new StatsSyncCapture(jobStore);
    // Tiny budget so it times out immediately.
    StatsSyncOutcome outcome = capture.capture("acct", "conn-1", SCOPE, Duration.ofNanos(1));

    assertThat(outcome).isEqualTo(StatsSyncOutcome.TIMEOUT);
  }

  private static ReconcileJobStore.ReconcileJob job(String state) {
    return new ReconcileJobStore.ReconcileJob(
        "job-1",
        "acct",
        "conn-1",
        state,
        "",
        0L,
        0L,
        0L,
        0L,
        0L,
        false,
        CaptureMode.CAPTURE_ONLY,
        0L,
        0L,
        null,
        null,
        "");
  }
}
