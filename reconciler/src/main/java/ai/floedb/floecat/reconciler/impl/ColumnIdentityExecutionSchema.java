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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Base64;

/** Carries snapshot identity through the existing execution-schema capture contract. */
final class ColumnIdentityExecutionSchema {
  private static final String MAP_FIELD = "_floecat_column_identity_map";
  private static final String FINGERPRINT_FIELD = "_floecat_column_identity_fingerprint";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ColumnIdentityExecutionSchema() {}

  /** Attaches the map so capture signatures and remote execution share the same identity. */
  static String attach(String schemaJson, ColumnIdentityMap identityMap) {
    if (identityMap == null || identityMap.equals(ColumnIdentityMap.getDefaultInstance())) {
      return schemaJson == null ? "" : schemaJson;
    }
    if (identityMap.getFingerprint().isBlank()) {
      throw new IllegalArgumentException("Column identity map fingerprint is required");
    }
    try {
      var parsed = MAPPER.readTree(schemaJson);
      if (!(parsed instanceof ObjectNode schema)) {
        throw new IllegalArgumentException("Execution schema must be a JSON object");
      }
      schema.put(MAP_FIELD, Base64.getEncoder().encodeToString(identityMap.toByteArray()));
      schema.put(FINGERPRINT_FIELD, identityMap.getFingerprint());
      return MAPPER.writeValueAsString(schema);
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to attach column identity to execution schema", e);
    }
  }

  /** Extracts and validates the identity map carried by an execution schema. */
  static ColumnIdentityMap identityMap(String executionSchemaJson) {
    if (executionSchemaJson == null || executionSchemaJson.isBlank()) {
      return ColumnIdentityMap.getDefaultInstance();
    }
    try {
      var schema = MAPPER.readTree(executionSchemaJson);
      String encoded = schema.path(MAP_FIELD).asText("");
      if (encoded.isBlank()) {
        return ColumnIdentityMap.getDefaultInstance();
      }
      ColumnIdentityMap identityMap =
          ColumnIdentityMap.parseFrom(Base64.getDecoder().decode(encoded));
      String fingerprint = schema.path(FINGERPRINT_FIELD).asText("");
      if (fingerprint.isBlank() || !fingerprint.equals(identityMap.getFingerprint())) {
        throw new IllegalArgumentException("Execution schema identity fingerprint mismatch");
      }
      return identityMap;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to read execution schema column identity", e);
    }
  }
}
