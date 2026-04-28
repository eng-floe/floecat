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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import java.util.List;
import java.util.Set;

public record StandaloneFileGroupExecutionPayload(
    String jobId,
    String leaseEpoch,
    String parentJobId,
    Connector sourceConnector,
    String sourceNamespace,
    String sourceTable,
    ResourceId tableId,
    long snapshotId,
    String planId,
    String groupId,
    List<String> plannedFilePaths,
    ReconcileCapturePolicy capturePolicy) {
  public StandaloneFileGroupExecutionPayload {
    jobId = jobId == null ? "" : jobId.trim();
    leaseEpoch = leaseEpoch == null ? "" : leaseEpoch.trim();
    parentJobId = parentJobId == null ? "" : parentJobId.trim();
    sourceConnector = sourceConnector == null ? Connector.getDefaultInstance() : sourceConnector;
    sourceNamespace = sourceNamespace == null ? "" : sourceNamespace.trim();
    sourceTable = sourceTable == null ? "" : sourceTable.trim();
    planId = planId == null ? "" : planId.trim();
    groupId = groupId == null ? "" : groupId.trim();
    plannedFilePaths =
        plannedFilePaths == null
            ? List.of()
            : plannedFilePaths.stream()
                .filter(path -> path != null && !path.isBlank())
                .map(String::trim)
                .toList();
    capturePolicy = capturePolicy == null ? ReconcileCapturePolicy.empty() : capturePolicy;
  }

  public Set<String> statsColumns() {
    return capturePolicy.selectorsForStats();
  }

  public Set<String> indexColumns() {
    return capturePolicy.selectorsForIndex();
  }

  public boolean requestsStats() {
    return !FileGroupExecutionSupport.requestedStatsTargetKinds(capturePolicy).isEmpty();
  }

  public boolean capturePageIndex() {
    return capturePolicy.requestsIndexes();
  }
}
