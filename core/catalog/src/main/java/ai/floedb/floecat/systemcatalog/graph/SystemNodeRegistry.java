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

package ai.floedb.floecat.systemcatalog.graph;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.AggregateNode;
import ai.floedb.floecat.metagraph.model.CastNode;
import ai.floedb.floecat.metagraph.model.CollationNode;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.OperatorNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.systemcatalog.def.SystemAggregateDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastDef;
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemOperatorDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificMatcher;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.util.EngineCatalogNames;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jboss.logging.Logger;

/**
 * Materializes and caches engine-specific system catalog nodes.
 *
 * <p>This registry takes declarative {@link SystemCatalogData} from the {@link
 * SystemDefinitionRegistry}, filters it by engine kind and version, applies engine-specific rules,
 * and builds immutable {@link GraphNode} instances with stable {@code _system} {@link ResourceId}s.
 *
 * <p>Results are cached per {@code (engineKind, engineVersion)} pair to avoid repeated filtering
 * and node construction.
 *
 * <p>This registry is the authoritative source of system-level graph nodes (functions, types,
 * operators, casts, namespaces, tables, views) used by the catalog overlay.
 */
public class SystemNodeRegistry {

  public static final String SYSTEM_ACCOUNT = "_system";
  private final SystemDefinitionRegistry definitionRegistry;
  private static final Logger LOG = Logger.getLogger(SystemNodeRegistry.class);
  private final SystemObjectScannerProvider internalProvider;
  private final List<SystemObjectScannerProvider> extensionProviders;

  /*
   * Immutable cache of materialized system nodes.
   * Entries are never evicted or mutated; a fresh registry instance
   * should be created for test isolation or controlled reloads.
   */
  private final ConcurrentMap<VersionKey, BuiltinNodes> cache = new ConcurrentHashMap<>();

  public SystemNodeRegistry(
      SystemDefinitionRegistry definitionRegistry,
      SystemObjectScannerProvider internalProvider,
      List<SystemObjectScannerProvider> extensionProviders) {
    this.definitionRegistry = Objects.requireNonNull(definitionRegistry);
    this.internalProvider = Objects.requireNonNull(internalProvider, "internalProvider");
    this.extensionProviders =
        List.copyOf(Objects.requireNonNull(extensionProviders, "extensionProviders"));
  }

  private static final SystemCatalogData EMPTY_CATALOG = SystemCatalogData.empty();

  private static final BuiltinNodes EMPTY_NODES =
      new BuiltinNodes(
          "",
          "",
          "",
          List.of(),
          List.of(),
          List.of(),
          List.of(),
          List.of(),
          List.of(),
          EMPTY_CATALOG);

  public List<String> engineKinds() {
    return definitionRegistry.engineKinds();
  }

  public BuiltinNodes nodesFor(String engineKind, String engineVersion) {
    return nodesFor(EngineContext.of(engineKind, engineVersion));
  }

  public BuiltinNodes nodesFor(EngineContext ctx) {
    EngineContext canonical = ctx == null ? EngineContext.empty() : ctx;
    VersionKey key = new VersionKey(canonical.normalizedKind(), canonical.normalizedVersion());
    return cache.computeIfAbsent(key, ignored -> buildNodes(canonical));
  }

  private BuiltinNodes buildNodes(EngineContext canonical) {
    SystemEngineCatalog baseCatalog = definitionRegistry.catalog(canonical);
    SystemCatalogData mergedCatalogData = mergeCatalogData(canonical, baseCatalog);
    SystemEngineCatalog catalog =
        SystemEngineCatalog.from(baseCatalog.engineKind(), mergedCatalogData);
    long version = versionFromFingerprint(catalog.fingerprint());
    String normalizedKind = canonical.normalizedKind();
    String normalizedVersion = canonical.normalizedVersion();

    // --- Functions ---
    List<SystemFunctionDef> functionDefs =
        catalog.functions().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withFunctionRules(def, normalizedKind, normalizedVersion))
            .toList();
    List<FunctionNode> functionNodes =
        functionDefs.stream().map(def -> toFunctionNode(normalizedKind, version, def)).toList();

    // --- Operators ---
    List<SystemOperatorDef> operatorDefs =
        catalog.operators().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withOperatorRules(def, normalizedKind, normalizedVersion))
            .toList();
    List<OperatorNode> operatorNodes =
        operatorDefs.stream().map(def -> toOperatorNode(normalizedKind, version, def)).toList();

    // --- Types ---
    List<SystemTypeDef> typeDefs =
        catalog.types().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withTypeRules(def, normalizedKind, normalizedVersion))
            .toList();
    List<TypeNode> typeNodes =
        typeDefs.stream().map(def -> toTypeNode(normalizedKind, version, def)).toList();

    // --- Casts ---
    List<SystemCastDef> castDefs =
        catalog.casts().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withCastRules(def, normalizedKind, normalizedVersion))
            .toList();
    List<CastNode> castNodes =
        castDefs.stream().map(def -> toCastNode(normalizedKind, version, def)).toList();

    // --- Collations ---
    List<SystemCollationDef> collationDefs =
        catalog.collations().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withCollationRules(def, normalizedKind, normalizedVersion))
            .toList();
    List<CollationNode> collationNodes =
        collationDefs.stream().map(def -> toCollationNode(normalizedKind, version, def)).toList();

    // --- Aggregates ---
    List<SystemAggregateDef> aggregateDefs =
        catalog.aggregates().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withAggregateRules(def, normalizedKind, normalizedVersion))
            .toList();
    List<AggregateNode> aggregateNodes =
        aggregateDefs.stream().map(def -> toAggregateNode(normalizedKind, version, def)).toList();

    // --- Namespaces ---
    List<SystemNamespaceDef> namespaceDefs =
        catalog.namespaces().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withNamespaceRules(def, normalizedKind, normalizedVersion))
            .toList();

    // --- Tables ---
    List<SystemTableDef> tableDefs =
        catalog.tables().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withTableRules(def, normalizedKind, normalizedVersion))
            .toList();

    // --- Views ---
    List<SystemViewDef> viewDefs =
        catalog.views().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withViewRules(def, normalizedKind, normalizedVersion))
            .toList();

    return new BuiltinNodes(
        normalizedKind,
        normalizedVersion,
        catalog.fingerprint(),
        functionNodes,
        operatorNodes,
        typeNodes,
        castNodes,
        collationNodes,
        aggregateNodes,
        new SystemCatalogData(
            functionDefs,
            operatorDefs,
            typeDefs,
            castDefs,
            collationDefs,
            aggregateDefs,
            namespaceDefs,
            tableDefs,
            viewDefs,
            catalog.registryEngineSpecific()));
  }

  private SystemCatalogData mergeCatalogData(
      EngineContext canonical, SystemEngineCatalog baseCatalog) {
    String engineKind = baseCatalog.engineKind();
    String normalizedKind = canonical.normalizedKind();
    String normalizedVersion = canonical.normalizedVersion();
    boolean includeProviders =
        canonical.enginePluginOverlaysEnabled()
            && !EngineCatalogNames.FLOECAT_DEFAULT_CATALOG.equals(engineKind);

    Map<String, SystemNamespaceDef> namespaceByName = new LinkedHashMap<>();
    Map<String, SystemTableDef> tableByName = new LinkedHashMap<>();
    Map<String, SystemViewDef> viewByName = new LinkedHashMap<>();

    for (SystemObjectDef def :
        internalProvider.definitions(
            EngineCatalogNames.FLOECAT_DEFAULT_CATALOG, normalizedVersion)) {
      mergeDefinition(def, namespaceByName, tableByName, viewByName);
    }

    for (SystemNamespaceDef ns : baseCatalog.namespaces()) {
      namespaceByName.put(NameRefUtil.canonical(ns.name()), ns);
    }
    for (SystemTableDef table : baseCatalog.tables()) {
      tableByName.put(NameRefUtil.canonical(table.name()), table);
    }
    for (SystemViewDef view : baseCatalog.views()) {
      viewByName.put(NameRefUtil.canonical(view.name()), view);
    }

    if (includeProviders) {
      for (SystemObjectScannerProvider provider : extensionProviders) {
        if (!provider.supportsEngine(normalizedKind)) {
          continue;
        }
        for (SystemObjectDef def : provider.definitions(normalizedKind, normalizedVersion)) {
          if (!provider.supports(def.name(), normalizedKind, normalizedVersion)) {
            continue;
          }
          mergeDefinition(def, namespaceByName, tableByName, viewByName);
        }
      }
    }

    List<EngineSpecificRule> registryCandidates = new ArrayList<>();
    registryCandidates.addAll(
        internalProvider.registryEngineSpecific(normalizedKind, normalizedVersion));
    registryCandidates.addAll(baseCatalog.registryEngineSpecific());
    if (includeProviders) {
      for (SystemObjectScannerProvider provider : extensionProviders) {
        if (!provider.supportsEngine(normalizedKind)) {
          continue;
        }
        registryCandidates.addAll(
            provider.registryEngineSpecific(normalizedKind, normalizedVersion));
      }
    }
    List<EngineSpecificRule> registryRules =
        dedupeMatchingRules(
            matchingRules(
                dedupeRegistryRules(registryCandidates), normalizedKind, normalizedVersion),
            normalizedKind);

    return new SystemCatalogData(
        baseCatalog.functions(),
        baseCatalog.operators(),
        baseCatalog.types(),
        baseCatalog.casts(),
        baseCatalog.collations(),
        baseCatalog.aggregates(),
        List.copyOf(namespaceByName.values()),
        List.copyOf(tableByName.values()),
        List.copyOf(viewByName.values()),
        registryRules);
  }

  private static void mergeDefinition(
      SystemObjectDef def,
      Map<String, SystemNamespaceDef> namespaces,
      Map<String, SystemTableDef> tables,
      Map<String, SystemViewDef> views) {
    if (def instanceof SystemNamespaceDef ns) {
      putDefinition(namespaces, NameRefUtil.canonical(ns.name()), ns, "namespace");
    } else if (def instanceof SystemTableDef table) {
      putDefinition(tables, NameRefUtil.canonical(table.name()), table, "table");
    } else if (def instanceof SystemViewDef view) {
      putDefinition(views, NameRefUtil.canonical(view.name()), view, "view");
    }
  }

  private static <T extends SystemObjectDef> void putDefinition(
      Map<String, T> target, String canonicalName, T def, String type) {
    T previous = target.put(canonicalName, def);
    if (previous != null) {
      LOG.infof(
          "Overriding %s definition for %s: %s -> %s",
          type, canonicalName, previous.getClass().getSimpleName(), def.getClass().getSimpleName());
    }
  }

  // =======================================================================
  // Rule applications
  // =======================================================================

  private SystemFunctionDef withFunctionRules(
      SystemFunctionDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemFunctionDef(
        def.name(),
        def.argumentTypes(),
        def.returnType(),
        def.isAggregate(),
        def.isWindow(),
        matched);
  }

  private SystemOperatorDef withOperatorRules(
      SystemOperatorDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemOperatorDef(
        def.name(),
        def.leftType(),
        def.rightType(),
        def.returnType(),
        def.isCommutative(),
        def.isAssociative(),
        matched);
  }

  private SystemTypeDef withTypeRules(SystemTypeDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemTypeDef(def.name(), def.category(), def.array(), def.elementType(), matched);
  }

  private SystemCastDef withCastRules(SystemCastDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemCastDef(def.name(), def.sourceType(), def.targetType(), def.method(), matched);
  }

  private SystemCollationDef withCollationRules(
      SystemCollationDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemCollationDef(def.name(), def.locale(), matched);
  }

  private SystemAggregateDef withAggregateRules(
      SystemAggregateDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemAggregateDef(
        def.name(), def.argumentTypes(), def.stateType(), def.returnType(), matched);
  }

  private SystemNamespaceDef withNamespaceRules(
      SystemNamespaceDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemNamespaceDef(def.name(), def.displayName(), matched);
  }

  private SystemTableDef withTableRules(
      SystemTableDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemTableDef(
        def.name(), def.displayName(), def.columns(), def.backendKind(), def.scannerId(), matched);
  }

  private SystemViewDef withViewRules(SystemViewDef def, String engineKind, String engineVersion) {

    List<EngineSpecificRule> matched =
        matchingRules(def.engineSpecific(), engineKind, engineVersion);

    return new SystemViewDef(
        def.name(), def.displayName(), def.sql(), def.dialect(), def.columns(), matched);
  }

  // =======================================================================
  // Node Builders
  // =======================================================================

  private FunctionNode toFunctionNode(String engineKind, long version, SystemFunctionDef def) {

    return new FunctionNode(
        resourceId(engineKind, ResourceKind.RK_FUNCTION, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        def.argumentTypes().stream()
            .map(arg -> resourceId(engineKind, ResourceKind.RK_TYPE, arg))
            .toList(),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType()),
        def.isAggregate(),
        def.isWindow(),
        Map.of());
  }

  private OperatorNode toOperatorNode(String engineKind, long version, SystemOperatorDef def) {

    return new OperatorNode(
        resourceId(engineKind, ResourceKind.RK_OPERATOR, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.leftType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.rightType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType()),
        def.isCommutative(),
        def.isAssociative(),
        Map.of());
  }

  private TypeNode toTypeNode(String engineKind, long version, SystemTypeDef def) {
    return new TypeNode(
        resourceId(engineKind, ResourceKind.RK_TYPE, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        def.category(),
        def.array(),
        def.elementType() == null
            ? null
            : resourceId(engineKind, ResourceKind.RK_TYPE, def.elementType()),
        Map.of());
  }

  private CastNode toCastNode(String engineKind, long version, SystemCastDef def) {
    return new CastNode(
        resourceId(engineKind, ResourceKind.RK_CAST, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        resourceId(engineKind, ResourceKind.RK_TYPE, def.sourceType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.targetType()),
        def.method().wireValue(),
        Map.of());
  }

  private CollationNode toCollationNode(String engineKind, long version, SystemCollationDef def) {

    return new CollationNode(
        resourceId(engineKind, ResourceKind.RK_COLLATION, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        def.locale(),
        Map.of());
  }

  private AggregateNode toAggregateNode(String engineKind, long version, SystemAggregateDef def) {

    return new AggregateNode(
        resourceId(engineKind, ResourceKind.RK_AGGREGATE, def.name()),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        def.argumentTypes().stream()
            .map(arg -> resourceId(engineKind, ResourceKind.RK_TYPE, arg))
            .toList(),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.stateType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType()),
        Map.of());
  }

  // =======================================================================
  // Helpers
  // =======================================================================

  public static ResourceId resourceId(String engineKind, ResourceKind kind, NameRef name) {
    String engine =
        (engineKind == null || engineKind.isBlank())
            ? EngineCatalogNames.FLOECAT_DEFAULT_CATALOG
            : engineKind;
    return ResourceId.newBuilder()
        .setAccountId(SYSTEM_ACCOUNT)
        .setKind(kind)
        .setId(engine + ":" + safeName(name))
        .build();
  }

  /**
   * Builds a display-friendly (case-preserving) identifier for graph nodes and ResourceIds.
   *
   * <p>This deliberately keeps the original casing so `_system:floe-demo.pg_catalog.pg_fn` matches
   * what planners expect. For maps/overrides we use {@link
   * ai.floedb.floecat.systemcatalog.util.NameRefUtil#canonical}.
   */
  public static String safeName(NameRef ref) {
    if (ref == null) return "";
    String path = String.join(".", ref.getPathList());
    return path.isEmpty() ? ref.getName() : path + "." + ref.getName();
  }

  private static List<EngineSpecificRule> matchingRules(
      List<EngineSpecificRule> rules, String engineKind, String engineVersion) {
    if (rules == null || rules.isEmpty()) return List.of();
    return EngineSpecificMatcher.matchedRules(rules, engineKind, engineVersion);
  }

  private static List<EngineSpecificRule> dedupeMatchingRules(
      List<EngineSpecificRule> matched, String targetKind) {
    if (matched == null || matched.isEmpty()) return List.of();
    LinkedHashMap<String, EngineSpecificRule> dedup = new LinkedHashMap<>();
    for (EngineSpecificRule rule : matched) {
      if (rule == null) {
        continue;
      }
      String key = registryMatchKey(rule);
      EngineSpecificRule existing = dedup.get(key);
      if (shouldReplaceBySpecificity(existing, rule, targetKind)) {
        dedup.put(key, rule);
      }
    }
    return List.copyOf(dedup.values());
  }

  private static String registryMatchKey(EngineSpecificRule rule) {
    String payloadType = rule.payloadType();
    if (payloadType == null) {
      payloadType = "";
    }
    return payloadType;
  }

  private static boolean shouldReplaceBySpecificity(
      EngineSpecificRule existing, EngineSpecificRule candidate, String targetKind) {
    if (existing == null) {
      return true;
    }
    String existingKind = existing.engineKind();
    String candidateKind = candidate.engineKind();
    if (candidateKind != null
        && !candidateKind.isBlank()
        && !candidateKind.equals(existingKind)
        && candidateKind.equals(targetKind)) {
      return true;
    }
    boolean existingWild = existingKind == null || existingKind.isBlank();
    boolean candidateWild = candidateKind == null || candidateKind.isBlank();
    if (existingWild && !candidateWild) {
      return true;
    }
    if (!existingWild && candidateWild) {
      return false;
    }
    int existingScore = versionSpecificity(existing);
    int candidateScore = versionSpecificity(candidate);
    if (candidateScore != existingScore) {
      return candidateScore > existingScore;
    }
    return true; // tie goes to later candidate so overlays override earlier hints
  }

  private static int versionSpecificity(EngineSpecificRule rule) {
    int score = 0;
    if (rule.hasMinVersion()) {
      score++;
    }
    if (rule.hasMaxVersion()) {
      score++;
    }
    return score;
  }

  private static List<EngineSpecificRule> dedupeRegistryRules(List<EngineSpecificRule> candidates) {
    if (candidates == null || candidates.isEmpty()) {
      return List.of();
    }
    LinkedHashMap<String, EngineSpecificRule> dedup = new LinkedHashMap<>();
    for (EngineSpecificRule rule : candidates) {
      if (rule == null) {
        continue;
      }
      String key = registryRuleKey(rule);
      dedup.remove(key);
      dedup.put(key, rule);
    }
    return List.copyOf(dedup.values());
  }

  private static String registryRuleKey(EngineSpecificRule rule) {
    String payloadType = rule.payloadType();
    if (payloadType == null) {
      payloadType = "";
    }
    String kind = rule.engineKind() == null ? "" : rule.engineKind();
    String min = rule.minVersion() == null ? "" : rule.minVersion();
    String max = rule.maxVersion() == null ? "" : rule.maxVersion();
    return String.join("|", payloadType, kind, min, max);
  }

  private static boolean matches(
      List<EngineSpecificRule> rules, String engineKind, String engineVersion) {
    return EngineSpecificMatcher.matches(rules, engineKind, engineVersion);
  }

  private static long versionFromFingerprint(String fingerprint) {
    if (fingerprint == null || fingerprint.isBlank()) return 0L;
    String prefix = fingerprint.length() >= 16 ? fingerprint.substring(0, 16) : fingerprint;
    try {
      return Long.parseUnsignedLong(prefix, 16);
    } catch (NumberFormatException e) {
      return prefix.hashCode();
    }
  }

  public record BuiltinNodes(
      String engineKind,
      String engineVersion,
      String fingerprint,
      List<FunctionNode> functions,
      List<OperatorNode> operators,
      List<TypeNode> types,
      List<CastNode> casts,
      List<CollationNode> collations,
      List<AggregateNode> aggregates,
      SystemCatalogData catalogData) {

    public BuiltinNodes {
      functions = List.copyOf(functions);
      operators = List.copyOf(operators);
      types = List.copyOf(types);
      casts = List.copyOf(casts);
      collations = List.copyOf(collations);
      aggregates = List.copyOf(aggregates);
      catalogData = Objects.requireNonNull(catalogData);
    }

    public SystemCatalogData toCatalogData() {
      return catalogData;
    }
  }

  private record VersionKey(String engineKind, String engineVersion) {}
}
