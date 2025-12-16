package ai.floedb.floecat.metagraph.model;

import ai.floedb.floecat.common.rpc.ResourceId;

public interface TableNode extends GraphNode {

  ResourceId namespaceId();

  String displayName();

  @Override
  default GraphNodeKind kind() {
    return GraphNodeKind.TABLE;
  }
}
