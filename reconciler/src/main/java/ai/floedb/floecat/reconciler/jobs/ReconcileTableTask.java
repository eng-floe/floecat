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
    Mode mode,
    String sourceNamespace,
    String sourceTable,
    String destinationNamespaceId,
    String destinationTableId,
    String destinationTableDisplayName) {
  public enum Mode {
    STRICT,
    DISCOVERY
  }

  public ReconcileTableTask {
    mode = mode == null ? Mode.STRICT : mode;
    sourceNamespace = sourceNamespace == null ? "" : sourceNamespace.trim();
    sourceTable = sourceTable == null ? "" : sourceTable.trim();
    destinationNamespaceId = destinationNamespaceId == null ? "" : destinationNamespaceId.trim();
    destinationTableId =
        mode == Mode.DISCOVERY && destinationTableId == null
            ? null
            : blankToEmpty(destinationTableId);
    destinationTableDisplayName =
        destinationTableDisplayName == null ? "" : destinationTableDisplayName.trim();
  }

  public static ReconcileTableTask of(String sourceNamespace, String sourceTable) {
    return of(sourceNamespace, sourceTable, "", "");
  }

  public static ReconcileTableTask of(
      String sourceNamespace,
      String sourceTable,
      String destinationTableId,
      String destinationTableDisplayName) {
    return of(sourceNamespace, sourceTable, "", destinationTableId, destinationTableDisplayName);
  }

  public static ReconcileTableTask of(
      String sourceNamespace,
      String sourceTable,
      String destinationNamespaceId,
      String destinationTableId,
      String destinationTableDisplayName) {
    if ((sourceNamespace == null || sourceNamespace.isBlank())
        && (sourceTable == null || sourceTable.isBlank())
        && (destinationNamespaceId == null || destinationNamespaceId.isBlank())
        && (destinationTableId == null || destinationTableId.isBlank())
        && (destinationTableDisplayName == null || destinationTableDisplayName.isBlank())) {
      return empty();
    }
    return new ReconcileTableTask(
        Mode.STRICT,
        sourceNamespace,
        sourceTable,
        destinationNamespaceId,
        destinationTableId,
        destinationTableDisplayName);
  }

  public static ReconcileTableTask discovery(
      String sourceNamespace,
      String sourceTable,
      String destinationNamespaceId,
      String destinationTableDisplayName) {
    return discovery(
        sourceNamespace, sourceTable, destinationNamespaceId, null, destinationTableDisplayName);
  }

  public static ReconcileTableTask discovery(
      String sourceNamespace,
      String sourceTable,
      String destinationNamespaceId,
      String destinationTableId,
      String destinationTableDisplayName) {
    return new ReconcileTableTask(
        Mode.DISCOVERY,
        sourceNamespace,
        sourceTable,
        destinationNamespaceId,
        destinationTableId,
        destinationTableDisplayName);
  }

  public static ReconcileTableTask empty() {
    return new ReconcileTableTask(Mode.STRICT, "", "", "", "", "");
  }

  public boolean isEmpty() {
    return sourceNamespace.isBlank()
        && sourceTable.isBlank()
        && destinationNamespaceId.isBlank()
        && blank(destinationTableId)
        && destinationTableDisplayName.isBlank();
  }

  public boolean strict() {
    return !isEmpty() && mode == Mode.STRICT;
  }

  public boolean discoveryMode() {
    return !isEmpty() && mode == Mode.DISCOVERY;
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
