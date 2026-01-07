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

package ai.floedb.floecat.systemcatalog.engine;

import java.util.Map;

/** Engine-specific applicability window for a builtin object. */
public record EngineSpecificRule(
    String engineKind,
    String minVersion,
    String maxVersion,
    // Opaque, engine-specific payload (optional; interpreted by plugins only)
    String payloadType,
    byte[] extensionPayload,
    // Generic key/value metadata (Spark, Trino, etc.)
    Map<String, String> properties) {

  public EngineSpecificRule {
    engineKind = engineKind == null ? "" : engineKind.trim();
    minVersion = minVersion == null ? "" : minVersion.trim();
    maxVersion = maxVersion == null ? "" : maxVersion.trim();

    // Payload can be null; normalize to empty to simplify equality / hashing
    payloadType = payloadType == null ? "" : payloadType.trim();
    extensionPayload =
        (extensionPayload == null || extensionPayload.length == 0)
            ? new byte[0]
            : extensionPayload.clone();

    properties = Map.copyOf(properties == null ? Map.of() : properties);
  }

  public static EngineSpecificRule exact(String engine, String version) {
    return new EngineSpecificRule(engine, version, version, "", null, Map.of());
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

  public boolean hasExtensionPayload() {
    return extensionPayload.length > 0;
  }
}
