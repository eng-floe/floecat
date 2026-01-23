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

import java.util.Objects;

/** Compound key for engine hints that includes a payload type. */
public record EngineHintKey(String engineKind, String engineVersion, String payloadType) {

  public EngineHintKey {
    Objects.requireNonNull(engineKind, "engineKind");
    Objects.requireNonNull(engineVersion, "engineVersion");
    Objects.requireNonNull(payloadType, "payloadType");
    if (engineKind.isBlank()) {
      throw new IllegalArgumentException("engineKind cannot be blank");
    }
    if (payloadType.isBlank()) {
      throw new IllegalArgumentException("payloadType cannot be blank");
    }
  }
}
