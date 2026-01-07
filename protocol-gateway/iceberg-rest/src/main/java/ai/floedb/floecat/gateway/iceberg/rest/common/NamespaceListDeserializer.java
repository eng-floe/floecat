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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class NamespaceListDeserializer extends JsonDeserializer<List<String>> {
  @Override
  public List<String> deserialize(JsonParser parser, DeserializationContext ctxt)
      throws IOException {
    JsonNode node = parser.getCodec().readTree(parser);
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isArray()) {
      List<String> parts = new ArrayList<>();
      for (JsonNode child : node) {
        parts.add(child.isNull() ? null : child.asText());
      }
      return parts;
    }
    if (node.isTextual()) {
      String text = node.asText();
      return text == null ? null : NamespacePaths.split(text);
    }
    throw JsonMappingException.from(
        parser,
        String.format(
            "Namespace value must be an array of strings or a string, found: %s",
            node.getNodeType()));
  }
}
