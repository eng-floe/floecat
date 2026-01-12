package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.systemcatalog.spi.scanner.MetadataResolutionContext;
import java.util.Objects;

/** Mutable holder describing a namespace (catalog/schema) during bundle decoration. */
public final class NamespaceDecoration extends AbstractDecoration {

  private final NameRef name;
  private final ResourceId namespaceId; // nullable if you don't have stable namespace ids
  private final GraphNode node; // optional

  public NamespaceDecoration(
      NameRef name,
      ResourceId namespaceId,
      GraphNode node,
      MetadataResolutionContext resolutionContext) {
    super(resolutionContext);
    this.name = Objects.requireNonNull(name, "name");
    this.namespaceId = namespaceId;
    this.node = node;
  }

  public NameRef name() {
    return name;
  }

  public ResourceId namespaceId() {
    return namespaceId;
  }

  public GraphNode node() {
    return node;
  }
}
