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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRegistry;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

@ApplicationScoped
public class StandaloneJavaFileGroupExecutionRunner {
  @Inject CaptureEngineRegistry captureEngineRegistry;

  public CaptureEngineResult execute(StandaloneFileGroupExecutionPayload payload) {
    if (payload == null
        || payload.tableId() == null
        || payload.sourceConnector() == null
        || payload.sourceConnector().equals(payload.sourceConnector().getDefaultInstance())
        || payload.snapshotId() < 0
        || payload.plannedFilePaths().isEmpty()) {
      return CaptureEngineResult.empty();
    }
    CaptureEngineResult capture =
        captureEngineRegistry.capture(
            new CaptureEngineRequest(
                payload.sourceConnector(),
                payload.sourceNamespace(),
                payload.sourceTable(),
                payload.tableId(),
                payload.snapshotId(),
                payload.planId(),
                payload.groupId(),
                payload.plannedFilePaths(),
                payload.statsColumns(),
                payload.indexColumns(),
                FileGroupExecutionSupport.requestedStatsTargetKinds(payload.capturePolicy()),
                payload.capturePageIndex()));
    if (!payload.capturePageIndex() || !capture.stagedIndexArtifacts().isEmpty()) {
      return capture;
    }
    return CaptureEngineResult.of(
        capture.statsRecords(),
        List.of(),
        FileGroupIndexArtifactStager.stage(
            payload.tableId(),
            payload.snapshotId(),
            payload.plannedFilePaths(),
            capture.statsRecords(),
            capture.pageIndexEntries()));
  }

  public record PersistableResult(
      List<ai.floedb.floecat.catalog.rpc.TargetStatsRecord> statsRecords,
      List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts) {
    public PersistableResult {
      statsRecords = statsRecords == null ? List.of() : List.copyOf(statsRecords);
      stagedIndexArtifacts =
          stagedIndexArtifacts == null ? List.of() : List.copyOf(stagedIndexArtifacts);
    }

    public static PersistableResult of(CaptureEngineResult capture) {
      CaptureEngineResult effective = capture == null ? CaptureEngineResult.empty() : capture;
      return new PersistableResult(effective.statsRecords(), effective.stagedIndexArtifacts());
    }
  }
}
