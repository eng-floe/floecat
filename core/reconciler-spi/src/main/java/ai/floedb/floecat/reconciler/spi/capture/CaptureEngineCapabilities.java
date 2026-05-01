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

package ai.floedb.floecat.reconciler.spi.capture;

import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public record CaptureEngineCapabilities(
    Set<FloecatConnector.StatsTargetKind> statsTargetKinds,
    boolean pageIndex,
    boolean expressionTargets,
    boolean columnSelectors,
    ExecutionScope executionScope,
    ResultContract resultContract,
    ExecutionRuntime executionRuntime) {
  public enum ExecutionScope {
    FILE_GROUP_ONLY
  }

  public enum ResultContract {
    COMPLETE_FILE_GROUP_OUTPUTS
  }

  public enum ExecutionRuntime {
    LOCAL_ONLY,
    REMOTE_ONLY,
    LOCAL_OR_REMOTE
  }

  public CaptureEngineCapabilities {
    statsTargetKinds =
        statsTargetKinds == null ? Set.of() : Set.copyOf(new LinkedHashSet<>(statsTargetKinds));
    executionScope = executionScope == null ? ExecutionScope.FILE_GROUP_ONLY : executionScope;
    resultContract =
        resultContract == null ? ResultContract.COMPLETE_FILE_GROUP_OUTPUTS : resultContract;
    executionRuntime = executionRuntime == null ? ExecutionRuntime.LOCAL_ONLY : executionRuntime;
  }

  public static CaptureEngineCapabilities of(
      Set<FloecatConnector.StatsTargetKind> statsTargetKinds, boolean pageIndex) {
    return new CaptureEngineCapabilities(
        statsTargetKinds,
        pageIndex,
        false,
        true,
        ExecutionScope.FILE_GROUP_ONLY,
        ResultContract.COMPLETE_FILE_GROUP_OUTPUTS,
        ExecutionRuntime.LOCAL_ONLY);
  }

  public static CaptureEngineCapabilities of(
      Set<FloecatConnector.StatsTargetKind> statsTargetKinds,
      boolean pageIndex,
      boolean expressionTargets,
      boolean columnSelectors,
      ExecutionScope executionScope,
      ResultContract resultContract,
      ExecutionRuntime executionRuntime) {
    return new CaptureEngineCapabilities(
        statsTargetKinds,
        pageIndex,
        expressionTargets,
        columnSelectors,
        executionScope,
        resultContract,
        executionRuntime);
  }

  /**
   * Placeholder seam for future unified expression-target capture.
   *
   * <p>The current file-group capture contract does not execute expression targets, but the target
   * kind remains part of the broader stats schema. A follow-up PR can turn this into a real
   * advertised capability without changing the rest of the SPI shape.
   */
  public boolean supportsExpressionTargets() {
    return expressionTargets;
  }

  public List<String> validationErrors(CaptureEngineRequest request) {
    List<String> errors = new ArrayList<>();
    if (request == null) {
      errors.add("capture request is required");
      return List.copyOf(errors);
    }
    if (!request.hasOutputs()) {
      errors.add("capture request must include at least one output");
    }
    if (!request.hasSourceContext()) {
      errors.add(
          "capture request must include source connector, source table, destination table, and snapshot");
    }
    if (!statsTargetKinds.containsAll(request.requestedStatsTargetKinds())) {
      errors.add(
          "requested stats target kinds are not supported: " + request.requestedStatsTargetKinds());
    }
    if (request.capturePageIndex() && !pageIndex) {
      errors.add("page-index output is not supported");
    }
    if (!columnSelectors && request.hasColumnSelectors()) {
      errors.add("column selectors are not supported");
    }
    if (executionScope == ExecutionScope.FILE_GROUP_ONLY && !request.isFileGroupScoped()) {
      errors.add("request does not satisfy file-group execution scope");
    }
    if (resultContract == ResultContract.COMPLETE_FILE_GROUP_OUTPUTS
        && !request.expectsCompleteFileGroupOutputs()) {
      errors.add("request does not satisfy complete file-group output contract");
    }
    return List.copyOf(errors);
  }

  public Optional<String> firstValidationError(CaptureEngineRequest request) {
    return validationErrors(request).stream().findFirst();
  }

  public boolean supports(CaptureEngineRequest request) {
    return validationErrors(request).isEmpty();
  }
}
