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

public record ReconcileViewTask(
    Mode mode,
    String sourceNamespace,
    String sourceView,
    String destinationNamespaceId,
    String destinationViewId,
    String destinationViewDisplayName) {
  public enum Mode {
    STRICT,
    DISCOVERY
  }

  public ReconcileViewTask {
    mode = mode == null ? Mode.STRICT : mode;
    sourceNamespace = sourceNamespace == null ? "" : sourceNamespace.trim();
    sourceView = sourceView == null ? "" : sourceView.trim();
    destinationNamespaceId = destinationNamespaceId == null ? "" : destinationNamespaceId.trim();
    destinationViewId =
        mode == Mode.DISCOVERY && destinationViewId == null
            ? null
            : blankToEmpty(destinationViewId);
    destinationViewDisplayName =
        destinationViewDisplayName == null ? "" : destinationViewDisplayName.trim();
  }

  public static ReconcileViewTask of(
      String sourceNamespace,
      String sourceView,
      String destinationNamespaceId,
      String destinationViewId) {
    return of(sourceNamespace, sourceView, destinationNamespaceId, destinationViewId, "");
  }

  public static ReconcileViewTask of(
      String sourceNamespace,
      String sourceView,
      String destinationNamespaceId,
      String destinationViewId,
      String destinationViewDisplayName) {
    if ((sourceNamespace == null || sourceNamespace.isBlank())
        && (sourceView == null || sourceView.isBlank())
        && (destinationNamespaceId == null || destinationNamespaceId.isBlank())
        && (destinationViewId == null || destinationViewId.isBlank())
        && (destinationViewDisplayName == null || destinationViewDisplayName.isBlank())) {
      return empty();
    }
    return new ReconcileViewTask(
        Mode.STRICT,
        sourceNamespace,
        sourceView,
        destinationNamespaceId,
        destinationViewId,
        destinationViewDisplayName);
  }

  public static ReconcileViewTask discovery(
      String sourceNamespace,
      String sourceView,
      String destinationNamespaceId,
      String destinationViewDisplayName) {
    return discovery(
        sourceNamespace, sourceView, destinationNamespaceId, null, destinationViewDisplayName);
  }

  public static ReconcileViewTask discovery(
      String sourceNamespace,
      String sourceView,
      String destinationNamespaceId,
      String destinationViewId,
      String destinationViewDisplayName) {
    return new ReconcileViewTask(
        Mode.DISCOVERY,
        sourceNamespace,
        sourceView,
        destinationNamespaceId,
        destinationViewId,
        destinationViewDisplayName);
  }

  public static ReconcileViewTask empty() {
    return new ReconcileViewTask(Mode.STRICT, "", "", "", "", "");
  }

  public boolean isEmpty() {
    return sourceNamespace.isBlank()
        && sourceView.isBlank()
        && destinationNamespaceId.isBlank()
        && blank(destinationViewId)
        && destinationViewDisplayName.isBlank();
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
