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

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import java.util.Map;

public record StoredReconcileJobListSummary(
    String accountId,
    String jobId,
    String connectorId,
    String state,
    String message,
    long startedAtMs,
    long finishedAtMs,
    long tablesScanned,
    long tablesChanged,
    long viewsScanned,
    long viewsChanged,
    long errors,
    boolean fullRescan,
    CaptureMode captureMode,
    long snapshotsProcessed,
    long statsProcessed,
    long indexesProcessed,
    String executorId,
    ReconcileExecutionClass executionClass,
    String executionLane,
    Map<String, String> executionAttributes,
    ReconcileJobKind jobKind,
    long plannedFileGroups,
    long plannedFiles,
    long completedFileGroups,
    long failedFileGroups,
    long completedFiles,
    long failedFiles,
    long createdAtMs) {}
