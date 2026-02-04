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

import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Helper for encoding/decoding persisted engine hint metadata inside relation properties.
 *
 * <p>Properties follow the pattern: `engine.hint.<payloadType> =
 * engineKind=<engineKind>;engineVersion=<engineVersion>;payload=<base64>` for relation-level hints
 * and `engine.hint.column.<payloadType>.<columnId>` for column hints. `encodeValue` centralizes
 * this format so `persistHint`/`NodeLoader` can read/write consistently.
 */
public final class EngineHintMetadata {

  private static final String PARTITION = ";";
  private static final String ENGINE_KIND_KV = "engineKind=";
  private static final String ENGINE_VERSION_KV = "engineVersion=";
  private static final String PAYLOAD_KV = "payload=";
  public static final String COLUMN_HINT_COLUMN_KEY = "columnId";

  private EngineHintMetadata() {}

  public static String tableHintKey(String payloadType) {
    return HintType.RELATION.keyFor(payloadType, null);
  }

  public static String columnHintKey(String payloadType, long columnId) {
    return HintType.COLUMN.keyFor(payloadType, Long.toString(columnId));
  }

  public static String encodeValue(String engineKind, String engineVersion, byte[] payload) {
    Objects.requireNonNull(engineKind, "engineKind");
    Objects.requireNonNull(engineVersion, "engineVersion");
    Objects.requireNonNull(payload, "payload");
    String encoded = Base64.getEncoder().encodeToString(payload);
    return ENGINE_KIND_KV
        + engineKind
        + PARTITION
        + ENGINE_VERSION_KV
        + engineVersion
        + PARTITION
        + PAYLOAD_KV
        + encoded;
  }

  public static Optional<DecodedValue> decodeValue(String value) {
    if (value == null || value.isBlank()) {
      return Optional.empty();
    }
    String[] segments = value.split(PARTITION, -1);
    if (segments.length < 3) {
      return Optional.empty();
    }
    String engineKind = null;
    String engineVersion = null;
    String payloadValue = null;
    for (String segment : segments) {
      if (segment.startsWith(ENGINE_KIND_KV)) {
        engineKind = segment.substring(ENGINE_KIND_KV.length()).trim();
      } else if (segment.startsWith(ENGINE_VERSION_KV)) {
        engineVersion = segment.substring(ENGINE_VERSION_KV.length()).trim();
      } else if (segment.startsWith(PAYLOAD_KV)) {
        payloadValue = segment.substring(PAYLOAD_KV.length());
      }
    }
    if (engineKind == null || engineVersion == null || payloadValue == null) {
      return Optional.empty();
    }
    try {
      byte[] payload = Base64.getDecoder().decode(payloadValue);
      return Optional.of(new DecodedValue(engineKind, engineVersion, payload));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  /**
   * Extracts relation-level hints from the arbitrary properties map.
   *
   * <p>The map contains entries keyed by `engine.hint.<payloadType>` and `engine.hint.column...`.
   * We reuse {@link #visitProperties} so every hint goes through the same parsing/decoding path,
   * then we ignore anything that is not a relation hint.
   */
  public static Map<EngineHintKey, EngineHint> hintsFromProperties(Map<String, String> properties) {
    Map<EngineHintKey, EngineHint> hints = new LinkedHashMap<>();
    visitProperties(
        properties,
        (header, decoded) -> {
          if (header.type.kind != HintKind.RELATION) {
            return;
          }
          EngineHintKey hintKey =
              new EngineHintKey(decoded.engineKind(), decoded.engineVersion(), header.payloadType);
          hints.put(hintKey, new EngineHint(header.payloadType, decoded.payload()));
        });
    return Map.copyOf(hints);
  }

  /**
   * Extracts column hints grouped by column id.
   *
   * <p>We again share the decoding logic through {@link #visitProperties} and only process keys
   * whose hint type reports {@link HintKind#COLUMN}. The resulting map is nested so callers can
   * quickly look up the hints for a specific column ordinal.
   */
  public static Map<String, Map<EngineHintKey, EngineHint>> columnHints(
      Map<String, String> properties) {
    Map<String, Map<EngineHintKey, EngineHint>> hints = new LinkedHashMap<>();
    visitProperties(
        properties,
        (header, decoded) -> {
          if (header.type.kind != HintKind.COLUMN) {
            return;
          }
          EngineHintKey hintKey =
              new EngineHintKey(decoded.engineKind(), decoded.engineVersion(), header.payloadType);
          Map<String, String> meta = Map.of(COLUMN_HINT_COLUMN_KEY, header.columnId);
          hints
              .computeIfAbsent(header.columnId, ignored -> new LinkedHashMap<>())
              .put(
                  hintKey,
                  new EngineHint(
                      header.payloadType, decoded.payload(), decoded.payload().length, meta));
        });
    Map<String, Map<EngineHintKey, EngineHint>> normalized = new LinkedHashMap<>();
    for (Map.Entry<String, Map<EngineHintKey, EngineHint>> entry : hints.entrySet()) {
      normalized.put(entry.getKey(), Map.copyOf(entry.getValue()));
    }
    return Map.copyOf(normalized);
  }

  public record DecodedValue(String engineKind, String engineVersion, byte[] payload) {}

  /**
   * Shared traversal that handles prefix parsing, base64 decoding, and dispatching.
   *
   * <p>Each property is first mapped to a {@link HintHeader} (which knows whether it is a table or
   * column hint, the payload type, etc.). We then decode the value and invoke the provided visitor
   * with both pieces. This prevents duplicating parsing logic in {@link #hintsFromProperties} /
   * {@link #columnHints}.
   */
  private static void visitProperties(
      Map<String, String> properties,
      java.util.function.BiConsumer<HintHeader, DecodedValue> visitor) {
    if (properties == null || properties.isEmpty()) {
      return;
    }
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      parseHintHeader(entry.getKey())
          .flatMap(
              header -> decodeValue(entry.getValue()).map(value -> new HintPayload(header, value)))
          .ifPresent(payload -> visitor.accept(payload.header(), payload.decoded()));
    }
  }

  private record HintPayload(HintHeader header, DecodedValue decoded) {}

  private static Optional<HintHeader> parseHintHeader(String key) {
    if (key == null || key.isBlank()) {
      return Optional.empty();
    }
    for (HintType type : HintType.values()) {
      Optional<HintHeader> header = type.parse(key);
      if (header.isPresent()) {
        return header;
      }
    }
    return Optional.empty();
  }

  /** Tracks whether a hint is stored at relation level or per-column. */
  private enum HintKind {
    RELATION,
    COLUMN
  }

  /**
   * Represents every supported property prefix and provides helpers for parsing/serialization.
   *
   * <p>Each enum value carries the literal prefix used in the catalog properties so we don't
   * sprinkle string constants everywhere. The {@link #parse} implementation is responsible for
   * extracting the payload type (and column ordinal when applicable).
   */
  private enum HintType {
    COLUMN("engine.hint.column.", HintKind.COLUMN) {
      @Override
      Optional<HintHeader> parse(String key) {
        if (!key.startsWith(prefix)) {
          return Optional.empty();
        }
        String suffix = key.substring(prefix.length());
        int lastDot = suffix.lastIndexOf('.');
        if (lastDot <= 0) {
          return Optional.empty();
        }
        String payloadType = suffix.substring(0, lastDot);
        String ordinal = suffix.substring(lastDot + 1);
        if (payloadType.isBlank() || ordinal.isBlank()) {
          return Optional.empty();
        }
        if (!ordinal.chars().allMatch(Character::isDigit)) {
          return Optional.empty();
        }
        return Optional.of(new HintHeader(this, payloadType, ordinal));
      }
    },
    RELATION("engine.hint.", HintKind.RELATION) {
      @Override
      Optional<HintHeader> parse(String key) {
        if (!key.startsWith(prefix)) {
          return Optional.empty();
        }
        String suffix = key.substring(prefix.length());
        if (suffix.isBlank()) {
          return Optional.empty();
        }
        return Optional.of(new HintHeader(this, suffix, null));
      }
    };

    final String prefix;
    final HintKind kind;

    HintType(String prefix, HintKind kind) {
      this.prefix = prefix;
      this.kind = kind;
    }

    abstract Optional<HintHeader> parse(String key);

    String keyFor(String payloadType, String ordinal) {
      String normalizedPayload = Objects.requireNonNull(payloadType, "payloadType");
      if (kind == HintKind.COLUMN) {
        if (ordinal == null || ordinal.isBlank()) {
          throw new IllegalArgumentException("column ordinal required for column hints");
        }
        return prefix + normalizedPayload + "." + ordinal;
      }
      return prefix + normalizedPayload;
    }
  }

  /**
   * Describes the parsed header of a property key.
   *
   * <p>The payloadType is the canonical descriptor that gets stored after the prefix, and the
   * columnId is only populated for column hints, keeping the format compatible with the
   * `engine.hint.column...` naming scheme.
   */
  private record HintHeader(HintType type, String payloadType, String columnId) {}
}
