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

package ai.floedb.floecat.extensions.floedb.engine.oid;

import ai.floedb.floecat.common.rpc.ResourceId;

/** Generates stable engine-specific object identifiers (OIDs) for metadata hints. */
public interface EngineOidGenerator {

  /**
   * Derives an OID for the requested object/hint pair.
   *
   * @param id canonical resource identifier for the object being decorated
   * @param payloadType engine hint payload type (e.g., {@code floe.relation+proto})
   * @return a positive, non-zero integer suitable for PG-style OIDs
   */
  default int generate(ResourceId id, String payloadType) {
    return generate(id, payloadType, "");
  }

  /**
   * Derives an OID for the requested object/hint pair, incorporating a caller-provided salt.
   *
   * <p>Callers that retry on collision must persist the chosen salt (or retry tag) alongside the
   * object.
   *
   * <p>Determinism is guaranteed only if the same salt is used again for the same object/hint.
   *
   * @param id canonical resource identifier for the object being decorated
   * @param payloadType engine hint payload type (e.g., {@code floe.relation+proto})
   * @param salt optional salt (can be "", but should be stable if you want stable OIDs)
   * @return a positive, non-zero integer suitable for PG-style OIDs
   */
  int generate(ResourceId id, String payloadType, String salt);
}
