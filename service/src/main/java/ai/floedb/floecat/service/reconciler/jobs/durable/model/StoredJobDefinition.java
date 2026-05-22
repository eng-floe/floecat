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

package ai.floedb.floecat.service.reconciler.jobs.durable.model;

import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import java.util.List;

public class StoredJobDefinition {
  public String sourceNamespace;
  public String sourceTable;
  public String taskMode;
  public String taskDestinationNamespaceId;
  public String taskDestinationTableId;
  public String taskDestinationTableDisplayName;
  public String sourceView;
  public String taskDestinationViewId;
  public String taskDestinationViewDisplayName;
  public String destinationTableId;
  public String destinationViewId;
  public List<String> destinationNamespaceIds = List.of();
  public List<ReconcileScope.ScopedCaptureRequest> destinationCaptureRequests = List.of();
  public List<ReconcileCapturePolicy.Column> capturePolicyColumns = List.of();
  public List<String> capturePolicyOutputs = List.of();
  public String capturePolicyDefaultColumnScope;
  public int capturePolicyMaxDefaultColumns;
  public String snapshotSelectionKind;
  public List<Long> snapshotSelectionSnapshotIds = List.of();
  public int snapshotSelectionLatestN;

  public static StoredJobDefinition of(
      ReconcileScope scope, ReconcileTableTask tableTask, ReconcileViewTask viewTask) {
    ReconcileScope effectiveScope = scope == null ? ReconcileScope.empty() : scope;
    ReconcileTableTask effectiveTableTask =
        tableTask == null ? ReconcileTableTask.empty() : tableTask;
    ReconcileViewTask effectiveViewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
    StoredJobDefinition definition = new StoredJobDefinition();
    definition.sourceNamespace = effectiveTableTask.sourceNamespace();
    definition.sourceTable = effectiveTableTask.sourceTable();
    definition.taskMode = effectiveTableTask.mode().name();
    definition.taskDestinationNamespaceId = effectiveTableTask.destinationNamespaceId();
    definition.taskDestinationTableId = blankToEmpty(effectiveTableTask.destinationTableId());
    definition.taskDestinationTableDisplayName = effectiveTableTask.destinationTableDisplayName();
    definition.sourceView = effectiveViewTask.sourceView();
    if (!effectiveViewTask.isEmpty()) {
      definition.sourceNamespace = effectiveViewTask.sourceNamespace();
      definition.taskMode = effectiveViewTask.mode().name();
      definition.taskDestinationNamespaceId = effectiveViewTask.destinationNamespaceId();
    }
    definition.taskDestinationViewId = blankToEmpty(effectiveViewTask.destinationViewId());
    definition.taskDestinationViewDisplayName = effectiveViewTask.destinationViewDisplayName();
    definition.destinationNamespaceIds = effectiveScope.destinationNamespaceIds();
    definition.destinationTableId = effectiveScope.destinationTableId();
    definition.destinationViewId = effectiveScope.destinationViewId();
    definition.destinationCaptureRequests = effectiveScope.destinationCaptureRequests();
    definition.capturePolicyColumns = effectiveScope.capturePolicy().columns();
    definition.capturePolicyOutputs =
        effectiveScope.capturePolicy().outputs().stream().map(Enum::name).toList();
    definition.capturePolicyDefaultColumnScope =
        effectiveScope.capturePolicy().defaultColumnScope().name();
    definition.capturePolicyMaxDefaultColumns = effectiveScope.capturePolicy().maxDefaultColumns();
    ReconcileSnapshotSelection snapshotSelection = effectiveScope.snapshotSelection();
    definition.snapshotSelectionKind = snapshotSelection.kind().name();
    definition.snapshotSelectionSnapshotIds = snapshotSelection.snapshotIds();
    definition.snapshotSelectionLatestN = snapshotSelection.latestN();
    return definition;
  }

  public ReconcileScope toScope() {
    ReconcileSnapshotSelection snapshotSelection =
        switch (blankToEmpty(snapshotSelectionKind)) {
          case "CURRENT" -> ReconcileSnapshotSelection.current();
          case "LATEST_N" -> ReconcileSnapshotSelection.latestN(snapshotSelectionLatestN);
          case "EXPLICIT" ->
              ReconcileSnapshotSelection.explicit(
                  snapshotSelectionSnapshotIds == null ? List.of() : snapshotSelectionSnapshotIds);
          case "ALL" -> ReconcileSnapshotSelection.all();
          default -> ReconcileSnapshotSelection.unspecified();
        };
    return ReconcileScope.of(
        destinationNamespaceIds,
        destinationTableId,
        destinationViewId,
        destinationCaptureRequests,
        ReconcileCapturePolicy.of(
            capturePolicyColumns,
            capturePolicyOutputs.stream()
                .map(ReconcileCapturePolicy.Output::valueOf)
                .collect(java.util.stream.Collectors.toSet()),
            blankToEmpty(capturePolicyDefaultColumnScope).isBlank()
                ? ReconcileCapturePolicy.DefaultColumnScope.FIRST_N
                : ReconcileCapturePolicy.DefaultColumnScope.valueOf(
                    capturePolicyDefaultColumnScope),
            capturePolicyMaxDefaultColumns),
        snapshotSelection);
  }

  public ReconcileTableTask tableTask() {
    if (ReconcileTableTask.Mode.DISCOVERY.name().equals(taskMode)) {
      return ReconcileTableTask.discovery(
          sourceNamespace,
          sourceTable,
          taskDestinationNamespaceId,
          blankToNull(taskDestinationTableId),
          taskDestinationTableDisplayName);
    }
    return ReconcileTableTask.of(
        sourceNamespace,
        sourceTable,
        taskDestinationNamespaceId,
        taskDestinationTableId,
        taskDestinationTableDisplayName);
  }

  public ReconcileViewTask viewTask() {
    if (ReconcileViewTask.Mode.DISCOVERY.name().equals(taskMode)) {
      return ReconcileViewTask.discovery(
          sourceNamespace,
          sourceView,
          taskDestinationNamespaceId,
          blankToNull(taskDestinationViewId),
          taskDestinationViewDisplayName);
    }
    return ReconcileViewTask.of(
        sourceNamespace,
        sourceView,
        taskDestinationNamespaceId,
        taskDestinationViewId,
        taskDestinationViewDisplayName);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static String blankToNull(String value) {
    String normalized = blankToEmpty(value);
    return normalized.isBlank() ? null : normalized;
  }
}
