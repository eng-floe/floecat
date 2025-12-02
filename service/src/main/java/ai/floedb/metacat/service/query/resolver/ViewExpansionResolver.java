package ai.floedb.metacat.service.query.resolve;

import ai.floedb.metacat.query.rpc.QueryInput;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;

/**
 * Computes view expansion maps for tables + views.
 *
 * <p>For DescribeInputs (external connectors): - Typically returns empty expansion (stubs).
 *
 * <p>For internal planner: - Will expand views fully.
 */
@ApplicationScoped
public class ViewExpansionResolver {

  public byte[] computeExpansion(String correlationId, List<QueryInput> inputs) {
    // TODO: full view expansion logic for internal planner
    return new byte[0];
  }
}
