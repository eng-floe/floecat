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

package ai.floedb.floecat.systemcatalog.hint;

import ai.floedb.floecat.metagraph.hint.EngineHintProvider;
import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.systemcatalog.def.*;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificMatcher;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import java.util.*;
import org.jboss.logging.Logger;

/* Publishes per-object builtin metadata as engine hints. */
public class SystemCatalogHintProvider implements EngineHintProvider {

  private static final Set<GraphNodeKind> SUPPORTED_KINDS =
      EnumSet.of(
          GraphNodeKind.FUNCTION,
          GraphNodeKind.OPERATOR,
          GraphNodeKind.TYPE,
          GraphNodeKind.CAST,
          GraphNodeKind.COLLATION,
          GraphNodeKind.AGGREGATE);

  private static final Logger LOG = Logger.getLogger(SystemCatalogHintProvider.class);

  private final SystemNodeRegistry nodeRegistry;
  private final Map<GraphNodeKind, Set<String>> supportedHintTypes;

  /**
   * We precompute every payload type mentioned in the loaded catalogs so {@code supports()} can
   * screen out typos instead of silently returning empty hints. The extra strictness is safe
   * because any valid payload type must already be present in the catalog registry; unsupported
   * types will be rejected before compute().
   */
  public SystemCatalogHintProvider(
      SystemNodeRegistry nodeRegistry, SystemDefinitionRegistry definitionRegistry) {
    this.nodeRegistry = nodeRegistry;
    this.supportedHintTypes = buildSupportedHintTypes(definitionRegistry);
  }

  /**
   * Only accept hint types that match configured payloads. This lets us fail fast on typos instead
   * of returning empty hints later.
   */
  @Override
  public boolean supports(GraphNodeKind kind, String payloadType) {
    if (!SUPPORTED_KINDS.contains(kind) || payloadType == null || payloadType.isBlank()) {
      return false;
    }
    Set<String> allowed = supportedHintTypes.get(kind);
    return allowed != null && allowed.contains(payloadType);
  }

  @Override
  public boolean isAvailable(EngineKey engineKey) {
    return engineKey != null
        && engineKey.engineVersion() != null
        && !engineKey.engineVersion().isBlank();
  }

  @Override
  public String fingerprint(GraphNode node, EngineKey engineKey, String payloadType) {
    EngineSpecificRule r = matchedRule(node, engineKey, payloadType);

    String base =
        node.id().getId()
            + ":"
            + node.version()
            + ":"
            + engineKey.engineKind()
            + ":"
            + engineKey.engineVersion()
            + ":"
            + payloadType;

    return (r == null) ? base : base + ":" + r.hashCode();
  }

  @Override
  public Optional<EngineHint> compute(
      GraphNode node, EngineKey engineKey, String payloadType, String correlationId) {
    EngineSpecificRule rule = matchedRule(node, engineKey, payloadType);

    if (rule == null) {
      // Log at debug to help track legitimate request/payloadType combinations that simply have no
      // matching rule without leaking data via a default payload.
      if (LOG.isDebugEnabled()) {
        String engineKind = engineKey == null ? "" : engineKey.engineKind();
        String engineVersion = engineKey == null ? "" : engineKey.engineVersion();
        LOG.debugf(
            "No engine hint for node %s kind=%s v=%s hint=%s",
            node.id().getId(), engineKind, engineVersion, payloadType);
      }
      return Optional.empty();
    }

    String rulePayloadType = rule.payloadType();
    if (rulePayloadType == null || rulePayloadType.isBlank()) {
      throw new IllegalStateException(
          "EngineSpecificRule missing payloadType for payloadType="
              + payloadType
              + " def="
              + node.id());
    }
    if (!rulePayloadType.equals(payloadType)) {
      throw new IllegalStateException(
          "Payload mismatch: request="
              + payloadType
              + " rule="
              + rulePayloadType
              + " def="
              + node.id());
    }
    byte[] payload = rule.extensionPayload();
    Map<String, String> meta = rule.properties();

    return Optional.of(
        new EngineHint(
            payloadType,
            payload == null ? new byte[0] : payload,
            payload == null ? 0 : payload.length,
            meta == null ? Map.of() : meta));
  }

  /** Determines the matching rule for the provided node. */
  private EngineSpecificRule matchedRule(GraphNode node, EngineKey engineKey, String payloadType) {
    if (!SUPPORTED_KINDS.contains(node.kind())) {
      return null;
    }

    EngineContext ctx =
        engineKey == null
            ? EngineContext.empty()
            : EngineContext.of(engineKey.engineKind(), engineKey.engineVersion());
    var nodes = nodeRegistry.nodesFor(ctx);

    List<? extends SystemObjectDef> defs =
        switch (node.kind()) {
          case FUNCTION -> nodes.catalogData().functions();
          case OPERATOR -> nodes.catalogData().operators();
          case TYPE -> nodes.catalogData().types();
          case CAST -> nodes.catalogData().casts();
          case COLLATION -> nodes.catalogData().collations();
          case AGGREGATE -> nodes.catalogData().aggregates();
          default -> List.of();
        };

    String engineKind = ctx.normalizedKind();
    String engineVersion = ctx.normalizedVersion();

    @SuppressWarnings("unchecked")
    List<SystemObjectDef> candidates =
        (List<SystemObjectDef>)
            (List<?>)
                defs.stream()
                    .filter(def -> node.id().equals(SystemNodeRegistry.resourceId(engineKind, def)))
                    .toList();

    if (candidates.isEmpty()) {
      return null;
    }

    for (SystemObjectDef def : candidates) {
      EngineSpecificRule r =
          selectRule(def.engineSpecific(), engineKind, engineVersion, payloadType);
      if (r != null) {
        return r;
      }
    }

    return null;
  }

  /**
   * Returns the last rule whose payloadType equals the requested payloadType.
   *
   * <p>Engine-specific catalogs preserve the definition order (pbtxt/overlay append order), so we
   * iterate the original list backwards, skip non-matching engines, and treat later entries as
   * overrides.
   */
  private EngineSpecificRule selectRule(
      List<EngineSpecificRule> rules, String engineKind, String engineVersion, String payloadType) {
    if (rules == null || rules.isEmpty()) {
      return null;
    }
    for (int i = rules.size() - 1; i >= 0; i--) {
      EngineSpecificRule rule = rules.get(i);
      if (!EngineSpecificMatcher.matchesRule(rule, engineKind, engineVersion)) {
        continue;
      }
      String rulePayloadType = rule.payloadType();
      if (rulePayloadType != null
          && !rulePayloadType.isBlank()
          && rulePayloadType.equals(payloadType)) {
        return rule;
      }
    }
    return null;
  }

  private static Map<GraphNodeKind, Set<String>> buildSupportedHintTypes(
      SystemDefinitionRegistry definitionRegistry) {
    Objects.requireNonNull(definitionRegistry, "definitionRegistry");
    EnumMap<GraphNodeKind, Set<String>> builder = new EnumMap<>(GraphNodeKind.class);
    for (GraphNodeKind kind : SUPPORTED_KINDS) {
      builder.put(kind, new LinkedHashSet<>());
    }

    for (String rawKind : definitionRegistry.engineKinds()) {
      if (rawKind == null || rawKind.isBlank()) {
        continue;
      }
      EngineContext ctx = EngineContext.of(rawKind, "");
      SystemEngineCatalog catalog = definitionRegistry.catalog(ctx);
      if (catalog == null) {
        continue;
      }
      collectHintTypes(builder, GraphNodeKind.FUNCTION, catalog.functions());
      collectHintTypes(builder, GraphNodeKind.OPERATOR, catalog.operators());
      collectHintTypes(builder, GraphNodeKind.TYPE, catalog.types());
      collectHintTypes(builder, GraphNodeKind.CAST, catalog.casts());
      collectHintTypes(builder, GraphNodeKind.COLLATION, catalog.collations());
      collectHintTypes(builder, GraphNodeKind.AGGREGATE, catalog.aggregates());
    }

    EnumMap<GraphNodeKind, Set<String>> result = new EnumMap<>(GraphNodeKind.class);
    for (GraphNodeKind kind : SUPPORTED_KINDS) {
      result.put(kind, Set.copyOf(builder.getOrDefault(kind, Set.of())));
    }
    return Collections.unmodifiableMap(result);
  }

  private static void collectHintTypes(
      Map<GraphNodeKind, Set<String>> builder,
      GraphNodeKind kind,
      List<? extends SystemObjectDef> defs) {
    if (defs == null || defs.isEmpty()) {
      return;
    }
    Set<String> targets = builder.get(kind);
    if (targets == null) {
      return;
    }
    for (SystemObjectDef def : defs) {
      if (def == null) {
        continue;
      }
      List<EngineSpecificRule> rules = def.engineSpecific();
      if (rules == null || rules.isEmpty()) {
        continue;
      }
      for (EngineSpecificRule rule : rules) {
        if (rule == null) {
          continue;
        }
        String rulePayloadType = rule.payloadType();
        if (rulePayloadType != null && !rulePayloadType.isBlank()) {
          targets.add(rulePayloadType);
        }
      }
    }
  }
}
