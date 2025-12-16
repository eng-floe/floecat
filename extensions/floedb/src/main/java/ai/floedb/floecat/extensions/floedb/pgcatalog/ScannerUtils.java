package ai.floedb.floecat.extensions.floedb.pgcatalog;

import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;

public class ScannerUtils {
  public static int getNodeOid(GraphNode node) {
    // TODO: Replace with snowflake or other stable unique ID generator
    return Math.abs(node.id().hashCode());
  }

  public static int defaultOwnerOid(GraphNode ns) {
    // Deterministic to default system onwer in floedb
    return 10;
  }

  public static SchemaColumn col(String name, String type) {
    return SchemaColumn.newBuilder().setName(name).setLogicalType(type).setNullable(false).build();
  }
}
