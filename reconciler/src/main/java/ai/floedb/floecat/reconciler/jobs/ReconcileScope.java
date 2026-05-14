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

import java.util.List;
import java.util.Objects;

/** Scope constraints for a reconcile job (namespaces, tables, views, and capture-target hints). */
public final class ReconcileScope {
  private static final ReconcileScope EMPTY =
      new ReconcileScope(
          List.of(),
          null,
          null,
          List.of(),
          ReconcileCapturePolicy.empty(),
          ReconcileSnapshotSelection.unspecified());

  private final List<String> destinationNamespaceIds;
  private final String destinationTableId;
  private final String destinationViewId;
  private final List<ScopedCaptureRequest> destinationCaptureRequests;
  private final ReconcileCapturePolicy capturePolicy;
  private final ReconcileSnapshotSelection snapshotSelection;

  public record ScopedCaptureRequest(
      String tableId, long snapshotId, String targetSpec, List<String> columnSelectors) {
    public ScopedCaptureRequest {
      tableId = tableId == null ? "" : tableId.trim();
      targetSpec = targetSpec == null ? "" : targetSpec.trim();
      columnSelectors =
          columnSelectors == null
              ? List.of()
              : columnSelectors.stream()
                  .filter(selector -> selector != null && !selector.isBlank())
                  .map(String::trim)
                  .sorted()
                  .toList();
    }
  }

  private ReconcileScope(
      List<String> destinationNamespaceIds,
      String destinationTableId,
      String destinationViewId,
      List<ScopedCaptureRequest> destinationCaptureRequests,
      ReconcileCapturePolicy capturePolicy,
      ReconcileSnapshotSelection snapshotSelection) {
    if (destinationNamespaceIds == null || destinationNamespaceIds.isEmpty()) {
      this.destinationNamespaceIds = List.of();
    } else {
      this.destinationNamespaceIds =
          destinationNamespaceIds.stream()
              .filter(namespaceId -> namespaceId != null && !namespaceId.isBlank())
              .map(String::trim)
              .distinct()
              .toList();
    }

    this.destinationTableId =
        (destinationTableId == null || destinationTableId.isBlank())
            ? null
            : destinationTableId.trim();
    this.destinationViewId =
        (destinationViewId == null || destinationViewId.isBlank())
            ? null
            : destinationViewId.trim();

    this.destinationCaptureRequests =
        destinationCaptureRequests == null
            ? List.of()
            : destinationCaptureRequests.stream()
                .filter(Objects::nonNull)
                .map(
                    request ->
                        new ScopedCaptureRequest(
                            request.tableId(),
                            request.snapshotId(),
                            request.targetSpec(),
                            request.columnSelectors()))
                .distinct()
                .toList();
    this.capturePolicy = capturePolicy == null ? ReconcileCapturePolicy.empty() : capturePolicy;
    this.snapshotSelection =
        snapshotSelection == null ? ReconcileSnapshotSelection.unspecified() : snapshotSelection;

    if (this.destinationTableId != null && !this.destinationNamespaceIds.isEmpty()) {
      throw new IllegalArgumentException(
          "destinationTableId cannot be combined with destinationNamespaceIds");
    }
    if (this.destinationViewId != null && !this.destinationNamespaceIds.isEmpty()) {
      throw new IllegalArgumentException(
          "destinationViewId cannot be combined with destinationNamespaceIds");
    }
    if (this.destinationTableId != null && this.destinationViewId != null) {
      throw new IllegalArgumentException(
          "destinationTableId cannot be combined with destinationViewId");
    }
    if (this.destinationViewId != null && !this.destinationCaptureRequests.isEmpty()) {
      throw new IllegalArgumentException(
          "destinationViewId cannot be combined with destinationCaptureRequests");
    }
    if (this.destinationTableId != null) {
      for (ScopedCaptureRequest request : this.destinationCaptureRequests) {
        if (request == null || request.tableId() == null || request.tableId().isBlank()) {
          continue;
        }
        if (!this.destinationTableId.equals(request.tableId())) {
          throw new IllegalArgumentException(
              "destinationCaptureRequests tableId must match destinationTableId");
        }
      }
    }
  }

  public static ReconcileScope empty() {
    return EMPTY;
  }

  public static ReconcileScope of(List<String> destinationNamespaceIds, String destinationTableId) {
    return of(
        destinationNamespaceIds,
        destinationTableId,
        null,
        List.of(),
        ReconcileCapturePolicy.empty(),
        ReconcileSnapshotSelection.unspecified());
  }

  public static ReconcileScope ofView(
      List<String> destinationNamespaceIds, String destinationViewId) {
    return of(
        destinationNamespaceIds,
        null,
        destinationViewId,
        List.of(),
        ReconcileCapturePolicy.empty(),
        ReconcileSnapshotSelection.unspecified());
  }

  public static ReconcileScope of(
      List<String> destinationNamespaceIds,
      String destinationTableId,
      List<ScopedCaptureRequest> destinationCaptureRequests) {
    return of(
        destinationNamespaceIds,
        destinationTableId,
        null,
        destinationCaptureRequests,
        ReconcileCapturePolicy.empty(),
        ReconcileSnapshotSelection.unspecified());
  }

  public static ReconcileScope of(
      List<String> destinationNamespaceIds,
      String destinationTableId,
      List<ScopedCaptureRequest> destinationCaptureRequests,
      ReconcileCapturePolicy capturePolicy) {
    return of(
        destinationNamespaceIds,
        destinationTableId,
        null,
        destinationCaptureRequests,
        capturePolicy,
        ReconcileSnapshotSelection.unspecified());
  }

  public static ReconcileScope of(
      List<String> destinationNamespaceIds,
      String destinationTableId,
      String destinationViewId,
      List<ScopedCaptureRequest> destinationCaptureRequests) {
    return of(
        destinationNamespaceIds,
        destinationTableId,
        destinationViewId,
        destinationCaptureRequests,
        ReconcileCapturePolicy.empty(),
        ReconcileSnapshotSelection.unspecified());
  }

  public static ReconcileScope of(
      List<String> destinationNamespaceIds,
      String destinationTableId,
      String destinationViewId,
      List<ScopedCaptureRequest> destinationCaptureRequests,
      ReconcileCapturePolicy capturePolicy) {
    return of(
        destinationNamespaceIds,
        destinationTableId,
        destinationViewId,
        destinationCaptureRequests,
        capturePolicy,
        ReconcileSnapshotSelection.unspecified());
  }

  public static ReconcileScope of(
      List<String> destinationNamespaceIds,
      String destinationTableId,
      String destinationViewId,
      List<ScopedCaptureRequest> destinationCaptureRequests,
      ReconcileCapturePolicy capturePolicy,
      ReconcileSnapshotSelection snapshotSelection) {
    if ((destinationNamespaceIds == null || destinationNamespaceIds.isEmpty())
        && (destinationTableId == null || destinationTableId.isBlank())
        && (destinationViewId == null || destinationViewId.isBlank())
        && (destinationCaptureRequests == null || destinationCaptureRequests.isEmpty())
        && (capturePolicy == null || capturePolicy.isEmpty())
        && (snapshotSelection == null || !snapshotSelection.isSpecified())) {
      return EMPTY;
    }
    return new ReconcileScope(
        destinationNamespaceIds,
        destinationTableId,
        destinationViewId,
        destinationCaptureRequests,
        capturePolicy,
        snapshotSelection);
  }

  public List<String> destinationNamespaceIds() {
    return destinationNamespaceIds;
  }

  public String destinationTableId() {
    return destinationTableId;
  }

  public String destinationViewId() {
    return destinationViewId;
  }

  public List<ScopedCaptureRequest> destinationCaptureRequests() {
    return destinationCaptureRequests;
  }

  public ReconcileCapturePolicy capturePolicy() {
    return capturePolicy;
  }

  public ReconcileSnapshotSelection snapshotSelection() {
    return snapshotSelection;
  }

  public boolean hasNamespaceFilter() {
    return !destinationNamespaceIds.isEmpty();
  }

  public boolean hasTableFilter() {
    return destinationTableId != null;
  }

  public boolean hasViewFilter() {
    return destinationViewId != null;
  }

  public boolean matchesNamespaceId(String namespaceId) {
    if (!hasNamespaceFilter()) {
      return true;
    }
    if (namespaceId == null || namespaceId.isBlank()) {
      return false;
    }
    return destinationNamespaceIds.contains(namespaceId);
  }

  public boolean acceptsTable(String namespaceId, String tableId) {
    if (!matchesNamespaceId(namespaceId)) {
      return false;
    }
    if (!hasTableFilter()) {
      return true;
    }
    return destinationTableId.equals(tableId);
  }

  public boolean acceptsView(String namespaceId, String viewId) {
    if (!matchesNamespaceId(namespaceId)) {
      return false;
    }
    if (!hasViewFilter()) {
      return true;
    }
    return destinationViewId.equals(viewId);
  }

  public boolean hasCaptureRequestFilter() {
    return !destinationCaptureRequests.isEmpty();
  }

  public boolean hasCapturePolicy() {
    return !capturePolicy.isEmpty();
  }

  public boolean hasSnapshotSelection() {
    return snapshotSelection.isSpecified();
  }
}
