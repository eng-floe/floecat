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

import java.util.Objects;
import java.util.function.Function;

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

  /**
   * Builds an {@link EngineContext} from an arbitrary header source.
   *
   * <p>The {@code headerReader} is called with the plain header name (e.g. {@code "x-engine-kind"})
   * and must return the raw header value, or {@code null} when the header is absent. Trimming and
   * blank-handling are applied internally by {@link #of}.
   *
   * <p>This is the single point of truth for header-name constants and the construction logic,
   * shared by both the gRPC path ({@code InboundContextInterceptor}) and the Arrow Flight path
   * ({@code InboundContextFlightMiddleware}).
   *
   * <p>Usage:
   *
   * <pre>
   *   // gRPC (Metadata)
   *   EngineContext.fromHeaders(name ->
   *       headers.get(Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER)));
   *
   *   // Arrow Flight (CallHeaders)
   *   EngineContext.fromHeaders(flightHeaders::get);
   * </pre>
   */
  public static EngineContext fromHeaders(Function<String, String> headerReader) {
    String kind = headerReader.apply("x-engine-kind");
    String version = headerReader.apply("x-engine-version");
    return of(kind, version);
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

  /**
   * Returns the engine ID used for catalog resolution.
   *
   * <p>If the request omitted engine headers, this returns {@code floecat_internal}; otherwise it
   * returns the normalized engine kind.
   */
  public String effectiveEngineKind() {
    return hasEngineKind ? normalizedKind : EngineCatalogNames.FLOECAT_DEFAULT_CATALOG;
  }

  /**
   * Returns {@code true} when an engine plugin overlay should be applied.
   *
   * <p>Only engine-specific plugins (i.e., non-`floecat_internal`) trigger this.
   */
  public boolean enginePluginOverlaysEnabled() {
    return hasEngineKind && !EngineCatalogNames.FLOECAT_DEFAULT_CATALOG.equals(normalizedKind);
  }

  /** Returns {@code true} when the request included engine headers. */
  public boolean hasEngineHeaders() {
    return hasEngineKind;
  }
}
