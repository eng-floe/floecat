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

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.auth.ReconcileWorkerAuthProvider;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRegistry;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class StandaloneJavaFileGroupExecutionRunner {
  @Inject CaptureEngineRegistry captureEngineRegistry;
  @Inject ReconcileWorkerAuthProvider reconcileWorkerAuthProvider;

  @ConfigProperty(name = "floecat.reconciler.worker.auth.required", defaultValue = "true")
  boolean workerAuthRequired = true;

  public CaptureEngineResult execute(
      StandaloneFileGroupExecutionPayload payload,
      BooleanSupplier shouldStop,
      Consumer<TargetStatsRecord> fileStatsPublisher) {
    if (payload == null
        || payload.tableId() == null
        || payload.sourceConnector() == null
        || payload.sourceConnector().equals(payload.sourceConnector().getDefaultInstance())
        || payload.snapshotId() < 0
        || payload.plannedFilePaths().isEmpty()) {
      return CaptureEngineResult.empty();
    }
    BooleanSupplier stop = shouldStop == null ? () -> false : shouldStop;
    throwIfCancellationRequested(stop);
    var requestedStatsKinds =
        FileGroupExecutionSupport.requestedFileGroupStatsTargetKinds(payload.capturePolicy());
    java.util.Optional<String> authorizationHeader =
        workerAuthorizationHeader(payload.tableId().getAccountId());
    Consumer<TargetStatsRecord> publisher =
        java.util.Objects.requireNonNull(fileStatsPublisher, "fileStatsPublisher");
    List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts = new ArrayList<>();
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
                FileGroupExecutionSupport.columnSelectorPolicy(payload.capturePolicy()),
                requestedStatsKinds,
                payload.capturePageIndex(),
                java.util.Optional.of(payload.storageLocation())
                    .filter(location -> !location.isBlank()),
                authorizationHeader,
                java.util.Optional.of(payload.jobId()),
                java.util.Optional.of(payload.leaseEpoch()),
                stop),
            (fileStats, pageIndexEntries) -> {
              List<TargetStatsRecord> completedFileStats =
                  fileStats == null ? List.of() : fileStats;
              List<ai.floedb.floecat.connector.spi.FloecatConnector.ParquetPageIndexEntry>
                  completedPageIndexEntries =
                      pageIndexEntries == null ? List.of() : pageIndexEntries;
              throwIfCancellationRequested(stop);
              for (TargetStatsRecord fileStat : completedFileStats) {
                throwIfCancellationRequested(stop);
                publisher.accept(fileStat);
              }
              if (payload.capturePageIndex()) {
                stagedIndexArtifacts.addAll(
                    FileGroupIndexArtifactStager.stage(
                        payload.tableId(),
                        payload.snapshotId(),
                        completedFilePaths(completedFileStats, completedPageIndexEntries),
                        completedFileStats,
                        completedPageIndexEntries));
              }
              throwIfCancellationRequested(stop);
            });
    throwIfCancellationRequested(stop);
    stagedIndexArtifacts.addAll(capture.stagedIndexArtifacts());
    return CaptureEngineResult.of(capture.statsRecords(), List.of(), stagedIndexArtifacts);
  }

  private static List<String> completedFilePaths(
      List<TargetStatsRecord> fileStats,
      List<ai.floedb.floecat.connector.spi.FloecatConnector.ParquetPageIndexEntry>
          pageIndexEntries) {
    LinkedHashSet<String> paths = new LinkedHashSet<>();
    for (TargetStatsRecord record : fileStats) {
      if (record != null && record.hasFile() && !record.getFile().getFilePath().isBlank()) {
        paths.add(record.getFile().getFilePath());
      }
    }
    for (var entry : pageIndexEntries) {
      if (entry != null && entry.filePath() != null && !entry.filePath().isBlank()) {
        paths.add(entry.filePath());
      }
    }
    return List.copyOf(paths);
  }

  private static void throwIfCancellationRequested(BooleanSupplier shouldStop) {
    if (shouldStop.getAsBoolean()) {
      throw new CancellationException("file-group execution cancelled");
    }
  }

  private java.util.Optional<String> workerAuthorizationHeader(String accountId) {
    if (!workerAuthRequired) {
      return java.util.Optional.empty();
    }
    return reconcileWorkerAuthProvider.authorizationHeader(accountId);
  }
}
