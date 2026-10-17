/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.client.unity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Locale;

/** Converts Unity's SQL/catalog-string column types to Delta schema JSON type nodes. */
public final class UnityCatalogTypeText {
  private static final ObjectMapper JSON = new ObjectMapper();

  private UnityCatalogTypeText() {}

  /**
   * Parses one complete {@code type_text} value.
   *
   * <p>This is intentionally scoped to Unity metadata. Delta log schemas continue to be parsed
   * exclusively by Delta Kernel; this adapter only handles the alternate SQL spelling exposed by
   * OSS Unity Catalog when {@code type_json} is absent.
   */
  public static JsonNode toDeltaType(String typeText) {
    if (typeText == null || typeText.isBlank()) {
      throw new IllegalArgumentException("Unity column type_text is blank");
    }
    Parser parser = new Parser(typeText);
    JsonNode type = parser.parseType();
    parser.skipWhitespace();
    if (!parser.atEnd()) {
      throw parser.failure("unexpected trailing input");
    }
    return type;
  }

  private static final class Parser {
    private final String input;
    private int offset;

    private Parser(String input) {
      this.input = input;
    }

    private JsonNode parseType() {
      String name = identifier("type").toLowerCase(Locale.ROOT);
      return switch (name) {
        case "array" -> parseArray();
        case "map" -> parseMap();
        case "struct" -> parseStruct();
        case "decimal", "dec", "numeric" -> parseDecimal();
        case "char", "varchar" -> parseCharacterType();
        case "boolean" -> text("boolean");
        case "byte", "tinyint" -> text("byte");
        case "short", "smallint" -> text("short");
        case "integer", "int" -> text("integer");
        case "long", "bigint" -> text("long");
        case "float", "real" -> text("float");
        case "double" -> text("double");
        case "string" -> text("string");
        case "binary" -> text("binary");
        case "date" -> text("date");
        case "timestamp" -> text("timestamp");
        case "timestamp_ntz" -> text("timestamp_ntz");
        case "variant" -> text("variant");
        default -> throw failure("unsupported Unity column type '" + name + "'");
      };
    }

    private JsonNode parseArray() {
      expect('<');
      JsonNode element = parseType();
      expect('>');
      ObjectNode array = JSON.createObjectNode();
      array.put("type", "array");
      array.set("elementType", element);
      array.put("containsNull", true);
      return array;
    }

    private JsonNode parseMap() {
      expect('<');
      JsonNode key = parseType();
      expect(',');
      JsonNode value = parseType();
      expect('>');
      ObjectNode map = JSON.createObjectNode();
      map.put("type", "map");
      map.set("keyType", key);
      map.set("valueType", value);
      map.put("valueContainsNull", true);
      return map;
    }

    private JsonNode parseStruct() {
      expect('<');
      var fields = JSON.createArrayNode();
      skipWhitespace();
      if (take('>')) {
        return struct(fields);
      }
      while (true) {
        String fieldName = fieldName();
        expect(':');
        ObjectNode field = JSON.createObjectNode();
        field.put("name", fieldName);
        field.set("type", parseType());
        field.put("nullable", true);
        field.set("metadata", JSON.createObjectNode());
        fields.add(field);
        skipWhitespace();
        if (take('>')) {
          return struct(fields);
        }
        expect(',');
      }
    }

    private JsonNode parseDecimal() {
      expect('(');
      int precision = integer("decimal precision");
      expect(',');
      int scale = integer("decimal scale");
      expect(')');
      if (precision < 1 || precision > 38 || scale < 0 || scale > precision) {
        throw failure("invalid decimal precision or scale");
      }
      return text("decimal(" + precision + "," + scale + ")");
    }

    private JsonNode parseCharacterType() {
      skipWhitespace();
      if (take('(')) {
        int length = integer("character length");
        expect(')');
        if (length < 1) {
          throw failure("character length must be positive");
        }
      }
      // Delta persists CHAR/VARCHAR columns as strings; their length is a SQL constraint rather
      // than a distinct Delta physical type.
      return text("string");
    }

    private static ObjectNode struct(com.fasterxml.jackson.databind.node.ArrayNode fields) {
      ObjectNode struct = JSON.createObjectNode();
      struct.put("type", "struct");
      struct.set("fields", fields);
      return struct;
    }

    private static JsonNode text(String value) {
      return JSON.getNodeFactory().textNode(value);
    }

    private String fieldName() {
      skipWhitespace();
      if (take('`')) {
        StringBuilder result = new StringBuilder();
        while (!atEnd()) {
          char current = input.charAt(offset++);
          if (current != '`') {
            result.append(current);
          } else if (!atEnd() && input.charAt(offset) == '`') {
            result.append('`');
            offset++;
          } else {
            return result.toString();
          }
        }
        throw failure("unterminated quoted field name");
      }
      return identifier("struct field name");
    }

    private String identifier(String description) {
      skipWhitespace();
      int start = offset;
      while (!atEnd()) {
        char current = input.charAt(offset);
        if (!Character.isLetterOrDigit(current) && current != '_') {
          break;
        }
        offset++;
      }
      if (start == offset) {
        throw failure("expected " + description);
      }
      return input.substring(start, offset);
    }

    private int integer(String description) {
      String value = identifier(description);
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException failure) {
        throw failure("invalid " + description);
      }
    }

    private void expect(char expected) {
      skipWhitespace();
      if (!take(expected)) {
        throw failure("expected '" + expected + "'");
      }
    }

    private boolean take(char expected) {
      if (!atEnd() && input.charAt(offset) == expected) {
        offset++;
        return true;
      }
      return false;
    }

    private void skipWhitespace() {
      while (!atEnd() && Character.isWhitespace(input.charAt(offset))) {
        offset++;
      }
    }

    private boolean atEnd() {
      return offset == input.length();
    }

    private IllegalArgumentException failure(String message) {
      return new IllegalArgumentException(
          "Invalid Unity type_text at offset " + offset + ": " + message);
    }
  }
}
