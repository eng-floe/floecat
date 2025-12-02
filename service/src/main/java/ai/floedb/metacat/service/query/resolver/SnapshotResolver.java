package ai.floedb.metacat.service.query.resolve;

import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.query.rpc.QueryInput;
import ai.floedb.metacat.query.rpc.SnapshotPin;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.query.impl.QueryContext;
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

    List<SnapshotPin> out = new ArrayList<>(inputs.size());

    for (QueryInput in : inputs) {

      if (!in.hasTableId()) {
        if (in.hasViewId()) {
          out.add(emptyViewPin(in.getViewId()));
          continue;
        }
        throw GrpcErrors.invalidArgument(correlationId, "query.input.not_table", Map.of());
      }

      ResourceId tableId = in.getTableId();
      SnapshotRef override = in.getSnapshot();

      // case 1: snapshot_id override
      if (override != null && override.hasSnapshotId()) {
        out.add(
            SnapshotPin.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(override.getSnapshotId())
                .build());
        continue;
      }

      // case 2: as_of override
      if (override != null && override.hasAsOf()) {
        out.add(SnapshotPin.newBuilder().setTableId(tableId).setAsOf(override.getAsOf()).build());
        continue;
      }

      // case 3: fallback to pinned snapshot
      SnapshotPin pinned = ctx.requireSnapshotPin(tableId, correlationId);
      out.add(pinned);
    }

    return out;
  }

  private SnapshotPin emptyViewPin(ResourceId viewId) {
    return SnapshotPin.newBuilder().setTableId(viewId).build();
  }
}
