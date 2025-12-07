package ai.floedb.floecat.catalog.builtin;

import ai.floedb.floecat.query.rpc.FloeAggregateSpecific;
import ai.floedb.floecat.query.rpc.FloeCastSpecific;
import ai.floedb.floecat.query.rpc.FloeCollationSpecific;
import ai.floedb.floecat.query.rpc.FloeFunctionSpecific;
import ai.floedb.floecat.query.rpc.FloeOperatorSpecific;
import ai.floedb.floecat.query.rpc.FloeTypeSpecific;
import java.util.Map;

/** Engine-specific applicability window for a builtin object. */
public record EngineSpecificRule(
    String engineKind,
    String minVersion,
    String maxVersion,
    FloeFunctionSpecific floeFunction,
    FloeOperatorSpecific floeOperator,
    FloeCastSpecific floeCast,
    FloeTypeSpecific floeType,
    FloeAggregateSpecific floeAggregate,
    FloeCollationSpecific floeCollation,
    Map<String, String> properties) {

  public EngineSpecificRule {
    engineKind = engineKind == null ? "" : engineKind.trim();
    minVersion = minVersion == null ? "" : minVersion.trim();
    maxVersion = maxVersion == null ? "" : maxVersion.trim();

    // Floe-specific fields left as null when not present — correct semantic behavior.

    properties = Map.copyOf(properties == null ? Map.of() : properties);
  }

  public boolean hasEngineKind() {
    return !engineKind.isBlank();
  }

  public boolean hasMinVersion() {
    return !minVersion.isBlank();
  }

  public boolean hasMaxVersion() {
    return !maxVersion.isBlank();
  }
}
