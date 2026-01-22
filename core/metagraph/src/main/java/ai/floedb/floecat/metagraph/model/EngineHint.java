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

package ai.floedb.floecat.metagraph.model;

import java.util.Map;
import java.util.Objects;

/**
 * Engine-specific payload attached to a {@link GraphNode}.
 *
 * <p>Hints carry opaque binary blobs plus optional metadata describing the payload. The {@code
 * payloadType} string indicates which typed descriptor or decoder can decode the payload; it
 * typically mirrors the payloadType/payload_type that was used when the hint was written. Engine
 * kind/ version scoping is handled by {@link EngineHintKey} outside the hint object.
 */
public record EngineHint(
    String payloadType, byte[] payload, long sizeBytes, Map<String, String> metadata) {

  /** Shortcut when hint payloadType matches descriptor + we don't track size/metadata. */
  public EngineHint(String payloadType, byte[] payload) {
    this(payloadType, payload, payload == null ? 0 : payload.length, Map.of());
  }

  public EngineHint {
    Objects.requireNonNull(payloadType, "payloadType");
    if (payloadType.isBlank()) {
      throw new IllegalArgumentException("payloadType cannot be blank");
    }
    Objects.requireNonNull(payload, "payload");
    sizeBytes = sizeBytes <= 0 ? payload.length : sizeBytes;
    metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
  }
}
