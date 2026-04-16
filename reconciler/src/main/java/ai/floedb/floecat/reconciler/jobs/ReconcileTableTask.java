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

package ai.floedb.floecat.reconciler.jobs;

public record ReconcileTableTask(
    String sourceNamespace, String sourceTable, String destinationTableDisplayName) {
  public ReconcileTableTask {
    sourceNamespace = sourceNamespace == null ? "" : sourceNamespace.trim();
    sourceTable = sourceTable == null ? "" : sourceTable.trim();
    destinationTableDisplayName =
        destinationTableDisplayName == null ? "" : destinationTableDisplayName.trim();
  }

  public static ReconcileTableTask of(String sourceNamespace, String sourceTable) {
    return of(sourceNamespace, sourceTable, "");
  }

  public static ReconcileTableTask of(
      String sourceNamespace, String sourceTable, String destinationTableDisplayName) {
    if ((sourceNamespace == null || sourceNamespace.isBlank())
        && (sourceTable == null || sourceTable.isBlank())
        && (destinationTableDisplayName == null || destinationTableDisplayName.isBlank())) {
      return empty();
    }
    return new ReconcileTableTask(sourceNamespace, sourceTable, destinationTableDisplayName);
  }

  public static ReconcileTableTask empty() {
    return new ReconcileTableTask("", "", "");
  }

  public boolean isEmpty() {
    return sourceNamespace.isBlank()
        && sourceTable.isBlank()
        && destinationTableDisplayName.isBlank();
  }
}
