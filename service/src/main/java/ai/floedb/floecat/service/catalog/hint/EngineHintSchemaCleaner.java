/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.catalog.hint;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.metagraph.hint.EngineHintMetadata;
import ai.floedb.floecat.systemcatalog.hint.HintClearContext;
import ai.floedb.floecat.systemcatalog.hint.HintClearDecision;
import ai.floedb.floecat.systemcatalog.provider.ServiceLoaderSystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** CDI-aware cleaner that iterates persisted hints per engine identity. */
@ApplicationScoped
public class EngineHintSchemaCleaner {

  private static final String HINT_PREFIX = "engine.hint.";

  private final ServiceLoaderSystemCatalogProvider catalogProvider;

  @Inject
  public EngineHintSchemaCleaner(ServiceLoaderSystemCatalogProvider catalogProvider) {
    this.catalogProvider = Objects.requireNonNull(catalogProvider);
  }

  public boolean shouldClearHints(FieldMask mask) {
    if (mask == null) {
      return false;
    }
    return normalizedMaskPaths(mask).stream().anyMatch(this::isSchemaPath);
  }

  private boolean isSchemaPath(String path) {
    return path.equals("schema_json")
        || path.startsWith("schema_json.")
        || path.equals("upstream")
        || path.startsWith("upstream.");
  }

  private Set<String> normalizedMaskPaths(FieldMask mask) {
    return mask.getPathsList().stream()
        .filter(Objects::nonNull)
        .map(String::trim)
        .map(String::toLowerCase)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  public void cleanTableHints(Table.Builder builder, FieldMask mask, Table before, Table after) {
    Map<String, String> properties = new LinkedHashMap<>(builder.getPropertiesMap());
    cleanHints(builder.getResourceId(), properties, mask, before, after, null, null);
    builder.clearProperties().putAllProperties(properties);
  }

  public void cleanViewHints(View.Builder builder, FieldMask mask, View before, View after) {
    Map<String, String> properties = new LinkedHashMap<>(builder.getPropertiesMap());
    cleanHints(builder.getResourceId(), properties, mask, null, null, before, after);
    builder.clearProperties().putAllProperties(properties);
  }

  private void cleanHints(
      ai.floedb.floecat.common.rpc.ResourceId resourceId,
      Map<String, String> properties,
      FieldMask mask,
      Table beforeTable,
      Table afterTable,
      View beforeView,
      View afterView) {
    if (properties == null || properties.isEmpty()) {
      return;
    }
    if (!shouldClearHints(mask)) {
      return;
    }
    Map<EngineIdentity, List<String>> grouped = groupHintsByEngine(properties);
    if (grouped.isEmpty()) {
      return;
    }
    HintClearContext ctx =
        new HintClearContext(resourceId, mask, beforeTable, afterTable, beforeView, afterView);
    for (Entry<EngineIdentity, List<String>> entry : grouped.entrySet()) {
      EngineIdentity identity = entry.getKey();
      List<String> keys = entry.getValue();
      EngineContext identityContext =
          EngineContext.of(identity.engineKind(), identity.engineVersion());
      HintClearDecision decision =
          catalogProvider
              .extensionFor(identity.engineKind())
              .map(ext -> safeDecide(ext, identityContext, ctx))
              .orElseGet(HintClearDecision::dropAll);
      applyDecision(decision, keys, properties);
    }
  }

  private void applyDecision(
      HintClearDecision decision, List<String> keys, Map<String, String> properties) {
    for (String key : keys) {
      Optional<EngineHintMetadata.HintKeyInfo> info = EngineHintMetadata.parseHintKey(key);
      if (info.isEmpty()) {
        properties.remove(key);
        continue;
      }
      EngineHintMetadata.HintKeyInfo hint = info.get();
      if (shouldRemove(decision, hint)) {
        properties.remove(key);
      }
    }
  }

  private boolean shouldRemove(HintClearDecision decision, EngineHintMetadata.HintKeyInfo hint) {
    if (hint.relationHint()) {
      if (decision.clearAllRelationHints()) {
        return true;
      }
      return decision.relationPayloadTypes().contains(hint.payloadType());
    }
    if (decision.clearAllColumnHints()) {
      return true;
    }
    if (decision.columnPayloadTypes().contains(hint.payloadType())) {
      return true;
    }
    Long id = hint.columnId();
    return id != null && decision.columnIds().contains(id);
  }

  private Map<EngineIdentity, List<String>> groupHintsByEngine(Map<String, String> properties) {
    Map<EngineIdentity, List<String>> grouped = new LinkedHashMap<>();
    var iterator = properties.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, String> entry = iterator.next();
      String key = entry.getKey();
      String value = entry.getValue();
      if (!isHintKey(key)) {
        continue;
      }
      Optional<EngineHintMetadata.HintValueHeader> header = EngineHintMetadata.peekHeader(value);
      if (header.isEmpty()) {
        iterator.remove();
        continue;
      }
      EngineHintMetadata.HintValueHeader h = header.get();
      EngineIdentity identity = new EngineIdentity(h.engineKind(), h.engineVersion());
      grouped.computeIfAbsent(identity, ignored -> new ArrayList<>()).add(key);
    }
    return grouped;
  }

  private boolean isHintKey(String key) {
    return key != null && key.startsWith(HINT_PREFIX);
  }

  private record EngineIdentity(String engineKind, String engineVersion) {}

  private HintClearDecision safeDecide(
      EngineSystemCatalogExtension ext, EngineContext ctx, HintClearContext context) {
    try {
      return Optional.ofNullable(ext.decideHintClear(ctx, context))
          .orElseGet(HintClearDecision::dropAll);
    } catch (RuntimeException e) {
      return HintClearDecision.dropAll();
    }
  }
}
