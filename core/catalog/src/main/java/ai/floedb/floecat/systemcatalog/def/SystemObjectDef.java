package ai.floedb.floecat.systemcatalog.def;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import java.util.List;

/**
 * Common contract for all builtin catalog definitions (functions, operators, types, casts,
 * collations, aggregates).
 *
 * <p>This interface exists so that generic code (hint providers, registries, matching logic) can
 * operate uniformly across all builtin object kinds without switches or instanceof chains.
 *
 * <p>NOTE: Only shared semantic fields are included here. Kind-specific attributes (argument lists,
 * element types, etc.) remain in the concrete records.
 */
public interface SystemObjectDef {

  /** Returns the fully scoped builtin name (path + simple name). */
  NameRef name();

  /** Returns all engine-specific rules attached to this builtin object. */
  List<EngineSpecificRule> engineSpecific();

  /** Returns the builtin kind enumeration so generic code can branch without instanceof. */
  ResourceKind kind();
}
