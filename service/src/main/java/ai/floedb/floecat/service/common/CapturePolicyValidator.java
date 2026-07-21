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

package ai.floedb.floecat.service.common;

import ai.floedb.floecat.capture.rpc.CaptureOutput;
import ai.floedb.floecat.capture.rpc.CapturePolicy;
import ai.floedb.floecat.capture.rpc.DefaultColumnScope;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.util.Map;

public final class CapturePolicyValidator {
  private CapturePolicyValidator() {}

  public static void validate(CapturePolicy policy, String correlationId, String fieldName) {
    if (policy == null) {
      return;
    }
    if (policy.getOutputsCount() == 0) {
      throw GrpcErrors.invalidArgument(
          correlationId, null, Map.of("field", fieldName + ".outputs"));
    }
    for (int i = 0; i < policy.getOutputsCount(); i++) {
      var output = policy.getOutputs(i);
      if (output == CaptureOutput.CO_UNSPECIFIED || output == CaptureOutput.UNRECOGNIZED) {
        throw GrpcErrors.invalidArgument(
            correlationId, null, Map.of("field", fieldName + ".outputs[" + i + "]"));
      }
    }
    if (policy.getMaxDefaultColumns() < 0) {
      throw GrpcErrors.invalidArgument(
          correlationId, null, Map.of("field", fieldName + ".max_default_columns"));
    }
    if (policy.getDefaultColumnScope() == DefaultColumnScope.UNRECOGNIZED) {
      throw GrpcErrors.invalidArgument(
          correlationId, null, Map.of("field", fieldName + ".default_column_scope"));
    }
    for (int i = 0; i < policy.getColumnsCount(); i++) {
      var column = policy.getColumns(i);
      String columnField = fieldName + ".columns[" + i + "]";
      if (column.getSelector().isBlank()) {
        throw GrpcErrors.invalidArgument(
            correlationId, null, Map.of("field", columnField + ".selector"));
      }
      if (!column.getCaptureStats() && !column.getCaptureIndex()) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            null,
            Map.of("field", columnField, "reason", "column capture policy has no enabled outputs"));
      }
    }
  }
}
