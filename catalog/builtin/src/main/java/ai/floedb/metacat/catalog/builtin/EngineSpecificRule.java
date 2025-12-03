package ai.floedb.metacat.catalog.builtin;

import java.util.Map;

/** Engine-specific applicability window for a builtin object. */
public record EngineSpecificRule(
    String engineKind, String minVersion, String maxVersion, Map<String, String> properties) {

  public EngineSpecificRule {
    engineKind = engineKind == null ? "" : engineKind.trim();
    minVersion = minVersion == null ? "" : minVersion.trim();
    maxVersion = maxVersion == null ? "" : maxVersion.trim();
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
