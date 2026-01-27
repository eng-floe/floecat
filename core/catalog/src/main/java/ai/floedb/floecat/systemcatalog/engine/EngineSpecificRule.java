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

import java.util.Arrays;
import java.util.Map;

/** Engine-specific applicability window for a builtin object. */
public record EngineSpecificRule(
    String engineKind,
    String minVersion,
    String maxVersion,
    // Engine hint name that describes this payload.
    String payloadType,
    // Decoded payload bytes (from EngineSpecific.payload at runtime, or PBtxt extension blocks at
    // build time).
    byte[] extensionPayload,
    // Generic key/value metadata (Spark, Trino, etc.)
    Map<String, String> properties) {

  public EngineSpecificRule {
    engineKind = engineKind == null ? "" : engineKind.trim();
    minVersion = minVersion == null ? "" : minVersion.trim();
    maxVersion = maxVersion == null ? "" : maxVersion.trim();

    // Payload can be null; normalize to empty to simplify equality / hashing
    payloadType = normalizePayloadType(payloadType);
    extensionPayload =
        (extensionPayload == null || extensionPayload.length == 0)
            ? new byte[0]
            : extensionPayload.clone();

    properties = Map.copyOf(properties == null ? Map.of() : properties);
  }

  @Override
  public byte[] extensionPayload() {
    return extensionPayload.clone();
  }

  public boolean hasPayloadType() {
    return !payloadType.isBlank();
  }

  public boolean payloadTypeEquals(String expected) {
    return payloadType.equals(normalizePayloadType(expected));
  }

  private static String normalizePayloadType(String payloadType) {
    if (payloadType == null) {
      return "";
    }
    String trimmed = payloadType.trim();
    return trimmed.isBlank() ? "" : trimmed;
  }

  public static EngineSpecificRule exact(String engine, String version, String payloadType) {
    return new EngineSpecificRule(engine, version, version, payloadType, null, Map.of());
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof EngineSpecificRule other)) return false;
    return engineKind.equals(other.engineKind)
        && minVersion.equals(other.minVersion)
        && maxVersion.equals(other.maxVersion)
        && payloadType.equals(other.payloadType)
        && Arrays.equals(extensionPayload, other.extensionPayload)
        && properties.equals(other.properties);
  }

  @Override
  public int hashCode() {
    int result = engineKind.hashCode();
    result = 31 * result + minVersion.hashCode();
    result = 31 * result + maxVersion.hashCode();
    result = 31 * result + payloadType.hashCode();
    result = 31 * result + Arrays.hashCode(extensionPayload);
    result = 31 * result + properties.hashCode();
    return result;
  }
}
