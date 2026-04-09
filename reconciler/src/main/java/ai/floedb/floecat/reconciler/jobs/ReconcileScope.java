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

import java.util.Collections;
import java.util.List;

/** Scope constraints for a reconcile job (namespaces, tables, optional column hints). */
public final class ReconcileScope {
  private static final ReconcileScope EMPTY = new ReconcileScope(List.of(), null, List.of());

  private final List<List<String>> destinationNamespacePaths;
  private final String destinationTableDisplayName;
  private final List<String> destinationTableColumns;

  private ReconcileScope(
      List<List<String>> destinationNamespacePaths,
      String destinationTableDisplayName,
      List<String> destinationTableColumns) {
    if (destinationNamespacePaths == null || destinationNamespacePaths.isEmpty()) {
      this.destinationNamespacePaths = List.of();
    } else {
      this.destinationNamespacePaths =
          destinationNamespacePaths.stream()
              .filter(path -> path != null && !path.isEmpty())
              .map(List::copyOf)
              .toList();
    }

    this.destinationTableDisplayName =
        (destinationTableDisplayName == null || destinationTableDisplayName.isBlank())
            ? null
            : destinationTableDisplayName;

    this.destinationTableColumns =
        destinationTableColumns == null ? List.of() : List.copyOf(destinationTableColumns);
  }

  public static ReconcileScope empty() {
    return EMPTY;
  }

  public static ReconcileScope of(
      List<List<String>> destinationNamespacePaths,
      String destinationTableDisplayName,
      List<String> destinationTableColumns) {
    if ((destinationNamespacePaths == null || destinationNamespacePaths.isEmpty())
        && (destinationTableDisplayName == null || destinationTableDisplayName.isBlank())
        && (destinationTableColumns == null || destinationTableColumns.isEmpty())) {
      return EMPTY;
    }
    return new ReconcileScope(
        destinationNamespacePaths, destinationTableDisplayName, destinationTableColumns);
  }

  public List<List<String>> destinationNamespacePaths() {
    return destinationNamespacePaths;
  }

  public String destinationTableDisplayName() {
    return destinationTableDisplayName;
  }

  public List<String> destinationTableColumns() {
    return destinationTableColumns;
  }

  public boolean hasNamespaceFilter() {
    return !destinationNamespacePaths.isEmpty();
  }

  public boolean hasTableFilter() {
    return destinationTableDisplayName != null;
  }

  public boolean matchesNamespace(String namespaceFq) {
    if (!hasNamespaceFilter()) {
      return true;
    }
    if (namespaceFq == null || namespaceFq.isBlank()) {
      return false;
    }
    for (List<String> path : destinationNamespacePaths) {
      if (String.join(".", path).equals(namespaceFq)) {
        return true;
      }
    }
    return false;
  }

  public boolean acceptsTable(String namespaceFq, String tableDisplayName) {
    if (!matchesNamespace(namespaceFq)) {
      return false;
    }
    if (!hasTableFilter()) {
      return true;
    }
    return destinationTableDisplayName.equals(tableDisplayName);
  }

  public boolean hasColumnFilter() {
    return !destinationTableColumns.isEmpty();
  }

  public List<String> columnsOrEmpty() {
    return destinationTableColumns.isEmpty() ? Collections.emptyList() : destinationTableColumns;
  }
}
