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

package ai.floedb.floecat.connector.common;

import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class ConnectorPlanningSupport {
  private ConnectorPlanningSupport() {}

  public static List<FloecatConnector.PlannedTableTask> planTableTasks(
      FloecatConnector.TablePlanningRequest request, Function<String, List<String>> listTables) {
    if (request == null) {
      return List.of();
    }

    String sourceNamespaceFq = requireNonBlank(request.sourceNamespaceFq(), "source namespace");
    String destinationNamespaceFq = blankToNull(request.destinationNamespaceFq());
    if (!matchesNamespaceFilter(destinationNamespaceFq, request.destinationNamespacePaths())) {
      throw new IllegalArgumentException(
          "Connector destination namespace "
              + destinationNamespaceFq
              + " does not match requested scope");
    }

    List<String> sourceTables =
        blankToNull(request.sourceTable()) != null
            ? List.of(request.sourceTable())
            : listTables.apply(sourceNamespaceFq);
    String destinationTableDisplayHint = blankToNull(request.destinationTableDisplayHint());
    String destinationTableFilter = blankToNull(request.destinationTableDisplayName());

    List<FloecatConnector.PlannedTableTask> planned = new ArrayList<>();
    for (String sourceTable : sourceTables) {
      String destinationTableDisplayName =
          destinationTableDisplayHint != null ? destinationTableDisplayHint : sourceTable;
      if (destinationTableFilter != null
          && !destinationTableFilter.equals(destinationTableDisplayName)) {
        continue;
      }
      planned.add(
          new FloecatConnector.PlannedTableTask(
              sourceNamespaceFq, sourceTable, destinationTableDisplayName));
    }
    return List.copyOf(planned);
  }

  private static boolean matchesNamespaceFilter(
      String namespaceFq, List<List<String>> destinationNamespacePaths) {
    if (destinationNamespacePaths == null || destinationNamespacePaths.isEmpty()) {
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

  private static String requireNonBlank(String value, String fieldName) {
    String normalized = blankToNull(value);
    if (normalized == null) {
      throw new IllegalArgumentException(fieldName + " is required");
    }
    return normalized;
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }
}
