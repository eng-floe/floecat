package ai.floedb.metacat.service.query.graph.model;

/**
 * Local classification of relation nodes.
 *
 * <p>Using an internal enum avoids leaking proto dependencies into the cache layer while still
 * allowing RPC mappers to translate into protobuf enums when needed.
 */
public enum RelationNodeKind {
  CATALOG,
  NAMESPACE,
  TABLE,
  VIEW,
  SYSTEM_VIEW,
  BUILTIN_FUNCTION,
  BUILTIN_OPERATOR,
  BUILTIN_TYPE,
  BUILTIN_CAST,
  BUILTIN_COLLATION,
  BUILTIN_AGGREGATE
}
