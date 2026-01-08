/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.systemcatalog.util;

import java.util.Objects;

/** Shared representation of the engine context (kind + version + normalized helpers). */
public final class EngineContext {

  private static final EngineContext EMPTY =
      new EngineContext("", "", EngineCatalogNames.FLOECAT_DEFAULT_CATALOG, "", false);

  private final String engineKind;
  private final String engineVersion;
  private final String normalizedKind;
  private final String normalizedVersion;
  private final boolean hasEngineKind;

  private EngineContext(
      String engineKind,
      String engineVersion,
      String normalizedKind,
      String normalizedVersion,
      boolean hasEngineKind) {
    this.engineKind = Objects.requireNonNull(engineKind, "engineKind");
    this.engineVersion = Objects.requireNonNull(engineVersion, "engineVersion");
    this.normalizedKind = Objects.requireNonNull(normalizedKind, "normalizedKind");
    this.normalizedVersion = Objects.requireNonNull(normalizedVersion, "normalizedVersion");
    this.hasEngineKind = hasEngineKind;
  }

  public static EngineContext of(String engineKind, String engineVersion) {
    String kind = engineKind == null ? "" : engineKind.trim();
    String version = engineVersion == null ? "" : engineVersion.trim();
    boolean hasKind = !kind.isEmpty();
    String normalizedKind =
        hasKind
            ? EngineContextNormalizer.normalizeEngineKind(kind)
            : EngineCatalogNames.FLOECAT_DEFAULT_CATALOG;
    if (!hasKind) {
      version = "";
    }
    String normalizedVersion = EngineContextNormalizer.normalizeEngineVersion(version);
    if (!hasKind && version.isEmpty()) {
      return EMPTY;
    }
    return new EngineContext(kind, version, normalizedKind, normalizedVersion, hasKind);
  }

  public static EngineContext empty() {
    return EMPTY;
  }

  public String engineKind() {
    return engineKind;
  }

  public String engineVersion() {
    return engineVersion;
  }

  public String normalizedKind() {
    return normalizedKind;
  }

  public String normalizedVersion() {
    return normalizedVersion;
  }

  public boolean hasEngineKind() {
    return hasEngineKind;
  }
}
