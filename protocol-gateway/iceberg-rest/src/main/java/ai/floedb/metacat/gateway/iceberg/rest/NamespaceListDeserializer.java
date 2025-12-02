package ai.floedb.metacat.gateway.iceberg.rest;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

final class NamespaceListDeserializer extends JsonDeserializer<List<String>> {
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
