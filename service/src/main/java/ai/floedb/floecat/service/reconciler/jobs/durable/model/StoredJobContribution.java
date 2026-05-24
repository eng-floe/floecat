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

public class StoredJobContribution {
  public String accountId;
  public String parentJobId;
  public String childJobId;
  public String state;
  public String message;
  public long startedAtMs;
  public long finishedAtMs;
  public long tablesScanned;
  public long tablesChanged;
  public long viewsScanned;
  public long viewsChanged;
  public long errors;
  public long snapshotsProcessed;
  public long statsProcessed;
  public long indexesProcessed;
  public long plannedFileGroups;
  public long plannedFiles;
  public long completedFileGroups;
  public long failedFileGroups;
  public long completedFiles;
  public long failedFiles;
  public String executorId;
  public long updatedAtMs;

  public static StoredJobContribution of(
      String accountId,
      String parentJobId,
      String childJobId,
      String state,
      String message,
      long startedAtMs,
      long finishedAtMs,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      long indexesProcessed,
      long plannedFileGroups,
      long plannedFiles,
      long completedFileGroups,
      long failedFileGroups,
      long completedFiles,
      long failedFiles,
      String executorId,
      long updatedAtMs) {
    StoredJobContribution contribution = new StoredJobContribution();
    contribution.accountId = blankToEmpty(accountId);
    contribution.parentJobId = blankToEmpty(parentJobId);
    contribution.childJobId = blankToEmpty(childJobId);
    contribution.state = blankToEmpty(state);
    contribution.message = blankToEmpty(message);
    contribution.startedAtMs = startedAtMs;
    contribution.finishedAtMs = finishedAtMs;
    contribution.tablesScanned = tablesScanned;
    contribution.tablesChanged = tablesChanged;
    contribution.viewsScanned = viewsScanned;
    contribution.viewsChanged = viewsChanged;
    contribution.errors = errors;
    contribution.snapshotsProcessed = snapshotsProcessed;
    contribution.statsProcessed = statsProcessed;
    contribution.indexesProcessed = indexesProcessed;
    contribution.plannedFileGroups = plannedFileGroups;
    contribution.plannedFiles = plannedFiles;
    contribution.completedFileGroups = completedFileGroups;
    contribution.failedFileGroups = failedFileGroups;
    contribution.completedFiles = completedFiles;
    contribution.failedFiles = failedFiles;
    contribution.executorId = blankToEmpty(executorId);
    contribution.updatedAtMs = updatedAtMs;
    return contribution;
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }
}
