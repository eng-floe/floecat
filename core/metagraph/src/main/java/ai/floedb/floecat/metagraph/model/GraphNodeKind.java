package ai.floedb.floecat.metagraph.model;

/**
 * Local classification of graph nodes.
 *
 * <p>Using an internal enum avoids leaking proto dependencies into the cache layer while still
 * allowing RPC mappers to translate into protobuf enums when needed.
 */
public enum GraphNodeKind {
  CATALOG,
  NAMESPACE,
  TABLE,
  VIEW,
  FUNCTION,
  OPERATOR,
  TYPE,
  CAST,
  COLLATION,
  AGGREGATE
}
