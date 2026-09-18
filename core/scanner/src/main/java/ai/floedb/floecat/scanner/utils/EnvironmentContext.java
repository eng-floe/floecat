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

package ai.floedb.floecat.scanner.utils;

import ai.floedb.floecat.engine.util.EngineIdentityNormalizer;
import java.util.Objects;

/** Identity of the catalog environment presented to a client. */
public final class EnvironmentContext {

  private static final EnvironmentContext EMPTY = new EnvironmentContext("", "", "", "", false);

  private final String environmentKind;
  private final String environmentVersion;
  private final String normalizedKind;
  private final String normalizedVersion;
  private final boolean hasEnvironmentKind;

  private EnvironmentContext(
      String environmentKind,
      String environmentVersion,
      String normalizedKind,
      String normalizedVersion,
      boolean hasEnvironmentKind) {
    this.environmentKind = Objects.requireNonNull(environmentKind, "environmentKind");
    this.environmentVersion = Objects.requireNonNull(environmentVersion, "environmentVersion");
    this.normalizedKind = Objects.requireNonNull(normalizedKind, "normalizedKind");
    this.normalizedVersion = Objects.requireNonNull(normalizedVersion, "normalizedVersion");
    this.hasEnvironmentKind = hasEnvironmentKind;
  }

  public static EnvironmentContext of(String environmentKind, String environmentVersion) {
    String kind = environmentKind == null ? "" : environmentKind.trim();
    String version = environmentVersion == null ? "" : environmentVersion.trim();
    boolean hasKind = !kind.isEmpty();
    if (!hasKind) {
      return EMPTY;
    }
    return new EnvironmentContext(
        kind,
        version,
        EngineIdentityNormalizer.normalizeEngineKind(kind),
        EngineIdentityNormalizer.normalizeEngineVersion(version),
        true);
  }

  public static EnvironmentContext empty() {
    return EMPTY;
  }

  public String environmentKind() {
    return environmentKind;
  }

  public String environmentVersion() {
    return environmentVersion;
  }

  public String normalizedKind() {
    return normalizedKind;
  }

  public String normalizedVersion() {
    return normalizedVersion;
  }

  public boolean hasEnvironmentKind() {
    return hasEnvironmentKind;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EnvironmentContext other)) {
      return false;
    }
    return hasEnvironmentKind == other.hasEnvironmentKind
        && environmentKind.equals(other.environmentKind)
        && environmentVersion.equals(other.environmentVersion)
        && normalizedKind.equals(other.normalizedKind)
        && normalizedVersion.equals(other.normalizedVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        environmentKind, environmentVersion, normalizedKind, normalizedVersion, hasEnvironmentKind);
  }
}
