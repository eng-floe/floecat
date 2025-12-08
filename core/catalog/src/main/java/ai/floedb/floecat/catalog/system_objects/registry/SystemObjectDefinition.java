package ai.floedb.floecat.catalog.system_objects.registry;

import ai.floedb.floecat.catalog.common.engine.EngineSpecificRule;
import ai.floedb.floecat.catalog.common.util.NameRefUtil;
import ai.floedb.floecat.catalog.system_objects.spi.SystemObjectColumnSet;
import ai.floedb.floecat.common.rpc.NameRef;
import java.util.List;

/** Immutable definition of a system object. */
public record SystemObjectDefinition(
    NameRef name,
    SystemObjectColumnSet columns,
    String scannerId,
    List<EngineSpecificRule> engineSpecificRules) {

  public SystemObjectDefinition {
    engineSpecificRules =
        List.copyOf(engineSpecificRules == null ? List.of() : engineSpecificRules);
  }

  /** Canonical path representation used as registry key. */
  public String canonicalName() {
    return NameRefUtil.canonical(name);
  }
}
