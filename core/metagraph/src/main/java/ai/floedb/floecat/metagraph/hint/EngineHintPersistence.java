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

package ai.floedb.floecat.metagraph.hint;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.List;

/** SPI that persists engine-specific payloads when decorators generate them. */
public interface EngineHintPersistence {

  EngineHintPersistence NOOP =
      new EngineHintPersistence() {
        @Override
        public void persistRelationHint(
            ResourceId relationId,
            String payloadType,
            String engineKind,
            String engineVersion,
            byte[] payload) {}

        @Override
        public void persistColumnHint(
            ResourceId relationId,
            long columnId,
            String payloadType,
            String engineKind,
            String engineVersion,
            byte[] payload) {}

        @Override
        public void persistColumnHints(
            ResourceId relationId,
            String engineKind,
            String engineVersion,
            List<ColumnHint> hints) {}
      };

  void persistRelationHint(
      ResourceId relationId,
      String payloadType,
      String engineKind,
      String engineVersion,
      byte[] payload);

  void persistColumnHint(
      ResourceId relationId,
      long columnId,
      String payloadType,
      String engineKind,
      String engineVersion,
      byte[] payload);

  default void persistColumnHints(
      ResourceId relationId, String engineKind, String engineVersion, List<ColumnHint> hints) {
    if (hints == null || hints.isEmpty()) {
      return;
    }
    for (ColumnHint hint : hints) {
      persistColumnHint(
          relationId,
          hint.columnId(),
          hint.payloadType(),
          engineKind,
          engineVersion,
          hint.payload());
    }
  }

  record ColumnHint(String payloadType, long columnId, byte[] payload) {}
}
