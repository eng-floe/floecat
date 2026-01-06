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
 * <p>Hints carry opaque binary blobs plus optional metadata describing the payload. Engine kind /
 * version scoping is handled outside the hint object (via {@link EngineKey}).
 */
public record EngineHint(
    String contentType, byte[] payload, long sizeBytes, Map<String, String> metadata) {

  public EngineHint(String contentType, byte[] payload) {
    this(contentType, payload, payload == null ? 0 : payload.length, Map.of());
  }

  public EngineHint {
    Objects.requireNonNull(payload, "payload");
    contentType =
        contentType == null || contentType.isBlank() ? "application/octet-stream" : contentType;
    sizeBytes = sizeBytes <= 0 ? payload.length : sizeBytes;
    metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
  }
}
