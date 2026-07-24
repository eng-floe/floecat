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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifestDescriptor;
import ai.floedb.floecat.service.catalog.impl.CurrentSnapshotPointerService;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LeasedSnapshotFinalizeExecutionServiceTest {
  private static final String ACCOUNT_ID = "acct";
  private static final String FINALIZE_JOB_ID = "finalize-job";
  private static final String LEASE_EPOCH = "lease-1";
  private static final String TABLE_ID = "table-1";
  private static final long SNAPSHOT_ID = 55L;

  private LeasedSnapshotFinalizeExecutionService service;
  private ReconcileJobStore jobs;
  private BlobStore blobs;
  private CurrentSnapshotPointerService currentSnapshotPointerService;
  private PrincipalContext principal;

  @BeforeEach
  void setUp() {
    service = new LeasedSnapshotFinalizeExecutionService();
    jobs = mock(ReconcileJobStore.class);
    blobs = mock(BlobStore.class);
    currentSnapshotPointerService = mock(CurrentSnapshotPointerService.class);
    principal = mock(PrincipalContext.class);
    service.jobs = jobs;
    service.blobStore = blobs;
    service.childStateService = mock(SnapshotFinalizeChildStateService.class);
    service.currentSnapshotPointerService = currentSnapshotPointerService;
    service.idempotencyStore = mock(IdempotencyRepository.class);
    when(principal.getCorrelationId()).thenReturn("corr");
    when(principal.getAccountId()).thenReturn(ACCOUNT_ID);
    when(service.idempotencyStore.get(anyString())).thenReturn(Optional.empty());
    when(service.idempotencyStore.createPending(
            anyString(), anyString(), anyString(), anyString(), any(), any()))
        .thenReturn(true);
    when(jobs.renewLease(FINALIZE_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID)).thenReturn(Optional.of(finalizeJobView()));
    when(jobs.applyLeaseOutcome(
            eq(FINALIZE_JOB_ID),
            eq(LEASE_EPOCH),
            eq(ReconcileJobStore.CompletionKind.SUCCEEDED),
            anyLong(),
            anyString(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong()))
        .thenReturn(true);
  }

  @Test
  void successRegistersManifestWithoutReadingIt() {
    SnapshotCaptureManifestDescriptor descriptor = descriptor(manifestUri());
    when(blobs.head(manifestUri()))
        .thenReturn(Optional.of(BlobHeader.newBuilder().setContentLength(123).build()));

    service.persistSuccess(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor);

    verify(blobs, never()).get(anyString());
    verify(currentSnapshotPointerService)
        .publishCaptureManifest(
            any(),
            eq(SNAPSHOT_ID),
            eq(
                BlobRef.newBuilder()
                    .setUri(manifestUri())
                    .setVersion("0000000000000000000000000000000000000000000000000000000000000000")
                    .build()),
            eq(FINALIZE_JOB_ID));
  }

  @Test
  void successRejectsManifestOutsideFencedLocation() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            service.persistSuccess(
                principal,
                FINALIZE_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                descriptor("s3://other/manifest.pb")));

    verify(blobs, never()).head(anyString());
    verify(currentSnapshotPointerService, never())
        .publishCaptureManifest(any(), anyLong(), any(), anyString());
  }

  private static SnapshotCaptureManifestDescriptor descriptor(String uri) {
    return SnapshotCaptureManifestDescriptor.newBuilder()
        .setFormatVersion(1)
        .setAccountId(ACCOUNT_ID)
        .setConnectorId("connector")
        .setParentJobId("parent-job")
        .setFinalizeJobId(FINALIZE_JOB_ID)
        .setTableId(TABLE_ID)
        .setSnapshotId(SNAPSHOT_ID)
        .setLeaseEpoch(LEASE_EPOCH)
        .setResultId("result-1")
        .setManifestUri(uri)
        .setManifestBytes(123)
        .setManifestSha256(ByteString.copyFrom(new byte[32]))
        .setFileGroupCount(0)
        .setSourceFileCount(0)
        .setStatsRecordCount(2)
        .build();
  }

  private static String manifestUri() {
    return Keys.reconcileSnapshotCaptureManifestUri(
        ACCOUNT_ID, "parent-job", FINALIZE_JOB_ID, LEASE_EPOCH);
  }

  private static ReconcileJobStore.ReconcileJob finalizeJobView() {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            TABLE_ID,
            SNAPSHOT_ID,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/snapshot-plan.pb",
            0);
    return new ReconcileJobStore.ReconcileJob(
        FINALIZE_JOB_ID,
        ACCOUNT_ID,
        "connector",
        "JS_RUNNING",
        "",
        0L,
        0L,
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
        ReconcileExecutionPolicy.defaults(),
        "",
        "",
        ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        snapshotTask,
        ReconcileFileGroupTask.empty(),
        "parent-job");
  }
}
