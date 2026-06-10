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

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.impl.SnapshotPlanBlobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

@ApplicationScoped
public class SnapshotFinalizeCoverageService {
  @Inject SnapshotPlanBlobStore snapshotPlanBlobStore;

  public ExpectedCoverage expectedCoverage(ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (!effective.fileGroupPlanRecorded()) {
      return new ExpectedCoverage(
          PlannedCoverageState.UNKNOWN,
          List.of(),
          List.of(),
          "snapshot finalization requires explicit snapshot coverage metadata");
    }
    if (effective.completionMode() == ReconcileSnapshotTask.CompletionMode.DIRECT_STATS) {
      return new ExpectedCoverage(PlannedCoverageState.DIRECT_STATS, List.of(), List.of(), "");
    }
    List<ReconcileFileGroupTask> plannedGroups = plannedFileGroups(effective);
    LinkedHashSet<String> expectedFiles = new LinkedHashSet<>();
    for (ReconcileFileGroupTask fileGroup : plannedGroups) {
      if (fileGroup == null) {
        continue;
      }
      expectedFiles.addAll(fileGroup.filePaths());
    }
    List<ReconcileFileGroupTask> expectedGroups =
        plannedGroups.stream().filter(group -> group != null && !group.isEmpty()).toList();
    if (expectedGroups.isEmpty()) {
      return new ExpectedCoverage(
          PlannedCoverageState.EXPLICIT_EMPTY, List.of(), List.copyOf(expectedFiles), "");
    }
    return new ExpectedCoverage(
        PlannedCoverageState.NON_EMPTY,
        List.copyOf(expectedGroups),
        List.copyOf(expectedFiles),
        "");
  }

  public List<ReconcileFileGroupTask> plannedFileGroups(ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (effective.fileGroups() != null && !effective.fileGroups().isEmpty()) {
      return effective.fileGroups();
    }
    if (!effective.fileGroupPlanRecorded()
        || effective.fileGroupCount() <= 0L
        || effective.fileGroupPlanBlobUri().isBlank()) {
      return List.of();
    }
    return snapshotPlanBlobStore.loadFileGroups(effective);
  }

  public CoverageValidation validateCoverage(
      List<String> expectedFiles, List<TargetStatsRecord> fileStats) {
    LinkedHashSet<String> expected =
        new LinkedHashSet<>(expectedFiles == null ? List.of() : expectedFiles);
    LinkedHashMap<String, Integer> actualCounts = new LinkedHashMap<>();
    for (TargetStatsRecord record : fileStats == null ? List.<TargetStatsRecord>of() : fileStats) {
      if (record == null || !record.hasFile()) {
        continue;
      }
      String filePath = record.getFile().getFilePath();
      if (filePath == null || filePath.isBlank()) {
        continue;
      }
      actualCounts.merge(filePath, 1, Integer::sum);
    }
    LinkedHashSet<String> actual = new LinkedHashSet<>(actualCounts.keySet());
    LinkedHashSet<String> missing = new LinkedHashSet<>(expected);
    missing.removeAll(actual);
    LinkedHashSet<String> unexpected = new LinkedHashSet<>(actual);
    unexpected.removeAll(expected);
    LinkedHashSet<String> duplicates = new LinkedHashSet<>();
    for (var entry : actualCounts.entrySet()) {
      if (entry.getValue() > 1) {
        duplicates.add(entry.getKey());
      }
    }
    if (missing.isEmpty() && unexpected.isEmpty() && duplicates.isEmpty()) {
      return new CoverageValidation(true, List.of(), List.of(), List.of(), "");
    }

    StringBuilder message = new StringBuilder("Snapshot finalization coverage mismatch");
    if (!missing.isEmpty()) {
      message.append(" missing=").append(missing);
    }
    if (!unexpected.isEmpty()) {
      message.append(" unexpected=").append(unexpected);
    }
    if (!duplicates.isEmpty()) {
      message.append(" duplicates=").append(duplicates);
    }
    return new CoverageValidation(
        false,
        List.copyOf(missing),
        List.copyOf(unexpected),
        List.copyOf(duplicates),
        message.toString());
  }

  public enum PlannedCoverageState {
    UNKNOWN,
    DIRECT_STATS,
    EXPLICIT_EMPTY,
    NON_EMPTY
  }

  public record ExpectedCoverage(
      PlannedCoverageState state,
      List<ReconcileFileGroupTask> expectedGroups,
      List<String> expectedFiles,
      String message) {}

  public record CoverageValidation(
      boolean valid,
      List<String> missingFiles,
      List<String> unexpectedFiles,
      List<String> duplicateFiles,
      String message) {}
}
