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

package ai.floedb.floecat.systemcatalog.engine;

import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Utility that converts engine-specific rules to metagraph hint maps. */
public final class EngineHintsMapper {

  private EngineHintsMapper() {}

  public static Map<EngineHintKey, EngineHint> toHints(
      String engineKind, String engineVersion, List<EngineSpecificRule> rules) {
    return toHints(engineKind, engineVersion, rules, null);
  }

  public static Map<EngineHintKey, EngineHint> toHints(
      String engineKind,
      String engineVersion,
      List<EngineSpecificRule> rules,
      String objectContext) {
    if (rules == null || rules.isEmpty()) {
      return Map.of();
    }
    Map<EngineHintKey, EngineHint> hints = new LinkedHashMap<>();
    for (EngineSpecificRule rule : rules) {
      if (!rule.hasPayloadType()) {
        continue;
      }
      byte[] payload = Objects.requireNonNullElse(rule.extensionPayload(), new byte[0]);
      EngineHint hint =
          new EngineHint(rule.payloadType(), payload, payload.length, rule.properties());
      String normalizedKind =
          engineKind == null || engineKind.isBlank() ? "" : engineKind.toLowerCase(Locale.ROOT);
      String normalizedVersion = engineVersion == null ? "" : engineVersion;
      EngineHintKey key = new EngineHintKey(normalizedKind, normalizedVersion, rule.payloadType());
      if (hints.containsKey(key)) {
        throw new IllegalStateException(
            "Duplicate engine hint for "
                + key
                + " (payload="
                + rule.payloadType()
                + ", engine="
                + key.engineKind()
                + ":"
                + key.engineVersion()
                + ", context="
                + (objectContext == null || objectContext.isBlank() ? "<unknown>" : objectContext)
                + ")");
      }
      hints.put(key, hint);
    }
    return Map.copyOf(hints);
  }
}
