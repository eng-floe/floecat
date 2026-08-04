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

import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileIndexArtifactResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class FileGroupExecutionSupport {
  private FileGroupExecutionSupport() {}

  public static ReconcileCapturePolicy effectiveCapturePolicy(ReconcileJobStore.LeasedJob lease) {
    if (lease != null && lease.scope != null && lease.scope.hasCapturePolicy()) {
      return lease.scope.capturePolicy();
    }
    if (lease != null && lease.captureMode == ReconcilerService.CaptureMode.METADATA_ONLY) {
      return ReconcileCapturePolicy.empty();
    }
    throw new IllegalArgumentException("capture policy is required for capture reconcile modes");
  }

  public static Set<ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind>
      requestedStatsTargetKinds(ReconcileCapturePolicy capturePolicy) {
    EnumSet<ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind> kinds =
        EnumSet.noneOf(ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.class);
    if (capturePolicy.outputs().contains(ReconcileCapturePolicy.Output.TABLE_STATS)) {
      kinds.add(ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.TABLE);
    }
    if (capturePolicy.outputs().contains(ReconcileCapturePolicy.Output.FILE_STATS)) {
      kinds.add(ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.FILE);
    }
    if (capturePolicy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)) {
      kinds.add(ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.COLUMN);
    }
    return kinds;
  }

  public static Set<ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind>
      requestedFileGroupStatsTargetKinds(ReconcileCapturePolicy capturePolicy) {
    EnumSet<ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind> kinds =
        EnumSet.noneOf(ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.class);
    if (capturePolicy.outputs().contains(ReconcileCapturePolicy.Output.TABLE_STATS)) {
      kinds.add(ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.TABLE);
      kinds.add(ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.FILE);
    }
    if (capturePolicy.outputs().contains(ReconcileCapturePolicy.Output.FILE_STATS)) {
      kinds.add(ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.FILE);
    }
    if (capturePolicy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)) {
      kinds.add(ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.COLUMN);
      kinds.add(ai.floedb.floecat.connector.spi.FloecatConnector.StatsTargetKind.FILE);
    }
    return kinds;
  }

  public static ai.floedb.floecat.connector.spi.FloecatConnector.ColumnSelectorPolicy
      columnSelectorPolicy(ReconcileCapturePolicy capturePolicy) {
    ReconcileCapturePolicy effective =
        capturePolicy == null ? ReconcileCapturePolicy.empty() : capturePolicy;
    return new ai.floedb.floecat.connector.spi.FloecatConnector.ColumnSelectorPolicy(
        switch (effective.defaultColumnScope()) {
          case ALL -> ai.floedb.floecat.connector.spi.FloecatConnector.DefaultColumnScope.ALL;
          case EXPLICIT_ONLY ->
              ai.floedb.floecat.connector.spi.FloecatConnector.DefaultColumnScope.EXPLICIT_ONLY;
          case FIRST_N ->
              ai.floedb.floecat.connector.spi.FloecatConnector.DefaultColumnScope.FIRST_N;
        },
        effective.maxDefaultColumns());
  }

  public static List<ReconcileFileResult> fileResultsForSuccess(
      ReconcileFileGroupTask plannedTask,
      List<StatsObjectDescriptor> fileStats,
      List<ReconcilerBackend.StagedIndexArtifact> artifacts) {
    LinkedHashMap<String, Long> statsByFile = new LinkedHashMap<>();
    HashMap<String, String> fileByStatsTarget = new HashMap<>();
    HashMap<String, ReconcileIndexArtifactResult> artifactsByFile = new HashMap<>();
    for (String filePath : plannedTask.filePaths()) {
      statsByFile.put(filePath, 0L);
      fileByStatsTarget.put(
          StatsTargetIdentity.storageId(StatsTargetIdentity.fileTarget(filePath)), filePath);
    }
    for (ReconcilerBackend.StagedIndexArtifact artifact : artifacts) {
      if (artifact == null || artifact.record() == null) {
        continue;
      }
      var record = artifact.record();
      if (!record.hasTarget() || !record.getTarget().hasFile()) {
        continue;
      }
      String filePath = record.getTarget().getFile().getFilePath();
      if (filePath == null || filePath.isBlank()) {
        continue;
      }
      artifactsByFile.put(
          filePath,
          ReconcileIndexArtifactResult.of(
              record.getArtifactUri(),
              record.getArtifactFormat(),
              record.getArtifactFormatVersion()));
    }
    for (StatsObjectDescriptor descriptor : fileStats) {
      if (descriptor == null) {
        continue;
      }
      String filePath = fileByStatsTarget.get(descriptor.getTargetStorageId());
      if (filePath == null) {
        continue;
      }
      statsByFile.computeIfPresent(filePath, (ignored, count) -> count + 1L);
    }
    return statsByFile.entrySet().stream()
        .map(
            entry ->
                ReconcileFileResult.succeeded(
                    entry.getKey(),
                    entry.getValue(),
                    artifactsByFile.getOrDefault(
                        entry.getKey(), ReconcileIndexArtifactResult.empty())))
        .toList();
  }

  public static List<ReconcileFileResult> fileResultsForFailure(
      ReconcileFileGroupTask plannedTask, String message) {
    String effectiveMessage = message == null ? "" : message;
    return plannedTask.filePaths().stream()
        .map(filePath -> ReconcileFileResult.failed(filePath, effectiveMessage))
        .toList();
  }

  public static List<String> missingIndexArtifactFiles(
      ReconcileFileGroupTask plannedTask, List<ReconcilerBackend.StagedIndexArtifact> artifacts) {
    LinkedHashSet<String> plannedFiles = new LinkedHashSet<>(plannedTask.filePaths());
    LinkedHashSet<String> artifactFiles = new LinkedHashSet<>();
    for (ReconcilerBackend.StagedIndexArtifact artifact : artifacts) {
      if (artifact == null || artifact.record() == null) {
        continue;
      }
      var record = artifact.record();
      if (!record.hasTarget() || !record.getTarget().hasFile()) {
        continue;
      }
      String filePath = record.getTarget().getFile().getFilePath();
      if (filePath != null && !filePath.isBlank()) {
        artifactFiles.add(filePath);
      }
    }
    plannedFiles.removeAll(artifactFiles);
    return List.copyOf(plannedFiles);
  }
}
