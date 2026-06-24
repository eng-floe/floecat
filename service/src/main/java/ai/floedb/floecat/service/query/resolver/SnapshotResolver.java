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

package ai.floedb.floecat.service.query.resolver;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.*;

/**
 * Uses existing QueryContext snapshotSet to decide effective pins.
 *
 * <p>Override hierarchy: 1. explicit snapshot_id 2. explicit as_of timestamp 3. pinned snapshot in
 * QueryContext
 */
@ApplicationScoped
public class SnapshotResolver {

  public List<SnapshotPin> resolvePins(
      String correlationId, QueryContext ctx, List<QueryInput> inputs) {
    return resolvePins(correlationId, ctx, inputs, PhaseDiagnostics.NOOP);
  }

  public List<SnapshotPin> resolvePins(
      String correlationId, QueryContext ctx, List<QueryInput> inputs, PhaseDiagnostics diagnostics) {
    PhaseDiagnostics safeDiagnostics =
        diagnostics == null ? PhaseDiagnostics.NOOP : diagnostics;
    safeDiagnostics.add("snapshot_inputs", inputs == null ? 0 : inputs.size());

    List<SnapshotPin> out = new ArrayList<>(inputs.size());

    for (QueryInput in : inputs) {

      if (!in.hasTableId()) {
        if (in.hasViewId()) {
          safeDiagnostics.count("snapshot_view_inputs");
          out.add(viewPin(in.getViewId()));
          continue;
        }
        throw GrpcErrors.invalidArgument(correlationId, QUERY_INPUT_NOT_TABLE, Map.of());
      }

      safeDiagnostics.count("snapshot_table_inputs");
      ResourceId tableId = in.getTableId();
      SnapshotRef override = in.hasSnapshot() ? in.getSnapshot() : null;

      if (override == null || override.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
        safeDiagnostics.count("snapshot_context_pins");
        out.add(ctx.requireSnapshotPin(tableId, correlationId));
        continue;
      }

      switch (override.getWhichCase()) {
        case SNAPSHOT_ID -> {
          safeDiagnostics.count("snapshot_id_overrides");
          out.add(
              SnapshotPin.newBuilder()
                  .setTableId(tableId)
                  .setSnapshotId(override.getSnapshotId())
                  .build());
        }

        case AS_OF -> {
          safeDiagnostics.count("snapshot_asof_overrides");
          out.add(
              SnapshotPin.newBuilder().setTableId(tableId).setAsOf(override.getAsOf()).build());
        }

        case SPECIAL -> {
          // SS_CURRENT means "no override": use pinned snapshot for this query context.
          // Any other special value should be rejected to avoid silently mis-resolving.
          if (override.getSpecial() != SpecialSnapshot.SS_CURRENT) {
            throw GrpcErrors.invalidArgument(correlationId, SNAPSHOT_SPECIAL_MISSING, Map.of());
          }
          safeDiagnostics.count("snapshot_special_current");
          out.add(ctx.requireSnapshotPin(tableId, correlationId));
        }

        default -> throw GrpcErrors.invalidArgument(correlationId, SNAPSHOT_MISSING, Map.of());
      }
    }

    return out;
  }

  private SnapshotPin viewPin(ResourceId viewId) {
    // NOTE: SnapshotPin is table-oriented; views don't participate in snapshot pinning.
    // We return a "dummy" pin keyed by the view id to preserve output cardinality.
    return SnapshotPin.newBuilder().setTableId(viewId).build();
  }
}
