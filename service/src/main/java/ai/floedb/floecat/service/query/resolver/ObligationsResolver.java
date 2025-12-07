package ai.floedb.floecat.service.query.resolver;

import ai.floedb.floecat.query.rpc.SnapshotPin;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;

/**
 * Resolves governance obligations (masks, row filters). For now, DescribeInputs does not return
 * obligations, but they are stored in the QueryContext for planner use.
 */
@ApplicationScoped
public class ObligationsResolver {

  public byte[] resolveObligations(String correlationId, List<SnapshotPin> pins) {
    // TODO: load obligations from governance service
    return new byte[0];
  }
}
