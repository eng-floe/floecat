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
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.OperatorNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.def.SystemAggregateDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastDef;
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.def.SystemColumnDef;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemOperatorDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.engine.EngineHintsMapper;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificMatcher;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.util.EngineCatalogNames;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.util.SignatureUtil;
import ai.floedb.floecat.systemcatalog.util.SystemSchemaMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
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
          List.of(), // functions
          List.of(), // operators
          List.of(), // types
          List.of(), // casts
          List.of(), // collations
          List.of(), // aggregates
          List.of(), // namespaceNodes
          List.of(), // tableNodes
          List.of(), // viewNodes
          Map.of(), // tablesByNamespace
          Map.of(), // viewsByNamespace
          Map.of(), // tableNames
          Map.of(), // viewNames
          Map.of(), // namespaceNames
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
    ResourceId catalogId = systemCatalogId(normalizedKind);

    // --- Namespaces ---
    List<SystemNamespaceDef> namespaceDefs =
        catalog.namespaces().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withNamespaceRules(def, normalizedKind, normalizedVersion))
            .toList();

    Map<String, ResourceId> namespaceIds =
        namespaceDefs.stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    ns -> NameRefUtil.canonical(ns.name()), ns -> resourceId(normalizedKind, ns)));

    // --- Functions ---
    List<SystemFunctionDef> functionDefs =
        catalog.functions().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withFunctionRules(def, normalizedKind, normalizedVersion))
            .toList();
    List<FunctionNode> functionNodes =
        functionDefs.stream()
            .map(
                def ->
                    toFunctionNode(normalizedKind, normalizedVersion, version, def, namespaceIds))
            .toList();

    // --- Operators ---
    List<SystemOperatorDef> operatorDefs =
        catalog.operators().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withOperatorRules(def, normalizedKind, normalizedVersion))
            .toList();
    List<OperatorNode> operatorNodes =
        operatorDefs.stream()
            .map(def -> toOperatorNode(normalizedKind, normalizedVersion, version, def))
            .toList();

    // --- Types ---
    List<SystemTypeDef> typeDefs =
        catalog.types().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withTypeRules(def, normalizedKind, normalizedVersion))
            .toList();
    List<TypeNode> typeNodes =
        typeDefs.stream()
            .map(def -> toTypeNode(normalizedKind, normalizedVersion, version, def))
            .toList();

    // --- Casts ---
    List<SystemCastDef> castDefs =
        catalog.casts().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withCastRules(def, normalizedKind, normalizedVersion))
            .toList();
    List<CastNode> castNodes =
        castDefs.stream()
            .map(def -> toCastNode(normalizedKind, normalizedVersion, version, def))
            .toList();

    // --- Collations ---
    List<SystemCollationDef> collationDefs =
        catalog.collations().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withCollationRules(def, normalizedKind, normalizedVersion))
            .toList();
    List<CollationNode> collationNodes =
        collationDefs.stream()
            .map(def -> toCollationNode(normalizedKind, normalizedVersion, version, def))
            .toList();

    // --- Aggregates ---
    List<SystemAggregateDef> aggregateDefs =
        catalog.aggregates().stream()
            .filter(def -> matches(def.engineSpecific(), normalizedKind, normalizedVersion))
            .map(def -> withAggregateRules(def, normalizedKind, normalizedVersion))
            .toList();
    List<AggregateNode> aggregateNodes =
        aggregateDefs.stream()
            .map(def -> toAggregateNode(normalizedKind, normalizedVersion, version, def))
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

    Map<ResourceId, List<TableNode>> tablesByNamespace = new LinkedHashMap<>();
    Map<ResourceId, List<ViewNode>> viewsByNamespace = new LinkedHashMap<>();
    List<SystemTableNode> tableNodes = new ArrayList<>();
    List<ViewNode> viewNodes = new ArrayList<>();
    List<NamespaceNode> namespaceNodes = new ArrayList<>();
    Map<String, ResourceId> tableNames = new LinkedHashMap<>();
    Map<String, ResourceId> viewNames = new LinkedHashMap<>();
    Map<String, ResourceId> namespaceNames = new LinkedHashMap<>();

    for (SystemTableDef table : tableDefs) {
      Optional<ResourceId> namespaceId = findNamespaceId(table.name(), namespaceIds);
      if (namespaceId.isEmpty()) {
        logMissingNamespace("table", table.name());
        continue;
      }
      ResourceId tableId = resourceId(normalizedKind, table);
      Map<String, Map<EngineHintKey, EngineHint>> columnHints =
          buildColumnHints(table.columns(), normalizedKind, normalizedVersion);
      List<SchemaColumn> tableColumns = SystemSchemaMapper.toSchemaColumns(table.columns());
      Map<EngineHintKey, EngineHint> tableHints =
          EngineHintsMapper.toHints(normalizedKind, normalizedVersion, table.engineSpecific());
      SystemTableNode node;
      switch (table.backendKind()) {
        case TABLE_BACKEND_KIND_FLOECAT ->
            node =
                new SystemTableNode.FloeCatSystemTableNode(
                    tableId,
                    version,
                    Instant.EPOCH,
                    normalizedVersion,
                    table.displayName(),
                    namespaceId.get(),
                    tableColumns,
                    columnHints,
                    tableHints,
                    table.scannerId());
        case TABLE_BACKEND_KIND_STORAGE ->
            node =
                new SystemTableNode.StorageSystemTableNode(
                    tableId,
                    version,
                    Instant.EPOCH,
                    normalizedVersion,
                    table.displayName(),
                    namespaceId.get(),
                    tableColumns,
                    columnHints,
                    tableHints,
                    table.storagePath());
        case TABLE_BACKEND_KIND_ENGINE ->
            node =
                new SystemTableNode.EngineSystemTableNode(
                    tableId,
                    version,
                    Instant.EPOCH,
                    normalizedVersion,
                    table.displayName(),
                    namespaceId.get(),
                    tableColumns,
                    columnHints,
                    tableHints);
        default ->
            node =
                new SystemTableNode.GenericSystemTableNode(
                    tableId,
                    version,
                    Instant.EPOCH,
                    normalizedVersion,
                    table.displayName(),
                    namespaceId.get(),
                    tableColumns,
                    columnHints,
                    tableHints,
                    table.backendKind());
      }
      tableNodes.add(node);
      tablesByNamespace.computeIfAbsent(namespaceId.get(), ignored -> new ArrayList<>()).add(node);
      tableNames.put(NameRefUtil.canonical(table.name()), tableId);
    }

    for (SystemViewDef view : viewDefs) {
      Optional<ResourceId> namespaceId = findNamespaceId(view.name(), namespaceIds);
      if (namespaceId.isEmpty()) {
        logMissingNamespace("view", view.name());
        continue;
      }
      ResourceId viewId = resourceId(normalizedKind, view);
      List<SchemaColumn> viewColumns = SystemSchemaMapper.toSchemaColumns(view.columns());
      Map<EngineHintKey, EngineHint> viewHints =
          EngineHintsMapper.toHints(normalizedKind, normalizedVersion, view.engineSpecific());
      ViewNode node =
          new ViewNode(
              viewId,
              version,
              Instant.EPOCH,
              catalogId,
              namespaceId.get(),
              view.displayName(),
              view.sql(),
              view.dialect(),
              viewColumns,
              List.of(),
              List.of(),
              GraphNodeOrigin.SYSTEM,
              Map.of(),
              Optional.empty(),
              viewHints);
      viewNodes.add(node);
      viewsByNamespace.computeIfAbsent(namespaceId.get(), ignored -> new ArrayList<>()).add(node);
      viewNames.put(NameRefUtil.canonical(view.name()), viewId);
    }

    for (SystemNamespaceDef ns : namespaceDefs) {
      ResourceId namespaceId = resourceId(normalizedKind, ns);
      Map<EngineHintKey, EngineHint> namespaceHints =
          EngineHintsMapper.toHints(normalizedKind, normalizedVersion, ns.engineSpecific());
      NamespaceNode node =
          new NamespaceNode(
              namespaceId,
              version,
              Instant.EPOCH,
              catalogId,
              List.copyOf(ns.name().getPathList()),
              ns.displayName(),
              GraphNodeOrigin.SYSTEM,
              Map.of(),
              namespaceHints);
      namespaceNodes.add(node);
      namespaceNames.put(NameRefUtil.canonical(ns.name()), namespaceId);
      tablesByNamespace.computeIfAbsent(namespaceId, ignored -> List.of());
      viewsByNamespace.computeIfAbsent(namespaceId, ignored -> List.of());
    }

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
        namespaceNodes,
        tableNodes,
        viewNodes,
        freezeNamespaceMap(tablesByNamespace),
        freezeNamespaceMap(viewsByNamespace),
        tableNames,
        viewNames,
        namespaceNames,
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
        def.name(),
        def.displayName(),
        def.columns(),
        def.backendKind(),
        def.scannerId(),
        def.storagePath(),
        matched);
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

  private FunctionNode toFunctionNode(
      String engineKind,
      String engineVersion,
      long version,
      SystemFunctionDef def,
      Map<String, ResourceId> namespaceIds) {

    ResourceId nsId = findNamespaceId(def.name(), namespaceIds).orElse(null);

    Map<EngineHintKey, EngineHint> hints =
        EngineHintsMapper.toHints(engineKind, engineVersion, def.engineSpecific());
    return new FunctionNode(
        resourceId(engineKind, def),
        version,
        Instant.EPOCH,
        engineKind,
        nsId,
        safeName(def.name()),
        def.argumentTypes().stream()
            .map(arg -> resourceId(engineKind, ResourceKind.RK_TYPE, arg))
            .toList(),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType()),
        def.isAggregate(),
        def.isWindow(),
        hints);
  }

  private OperatorNode toOperatorNode(
      String engineKind, String engineVersion, long version, SystemOperatorDef def) {
    Map<EngineHintKey, EngineHint> hints =
        EngineHintsMapper.toHints(engineKind, engineVersion, def.engineSpecific());

    return new OperatorNode(
        resourceId(engineKind, def),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.leftType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.rightType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType()),
        def.isCommutative(),
        def.isAssociative(),
        hints);
  }

  private TypeNode toTypeNode(
      String engineKind, String engineVersion, long version, SystemTypeDef def) {
    Map<EngineHintKey, EngineHint> hints =
        EngineHintsMapper.toHints(engineKind, engineVersion, def.engineSpecific());
    return new TypeNode(
        resourceId(engineKind, def),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        def.category(),
        def.array(),
        def.elementType() == null
            ? null
            : resourceId(engineKind, ResourceKind.RK_TYPE, def.elementType()),
        hints);
  }

  private CastNode toCastNode(
      String engineKind, String engineVersion, long version, SystemCastDef def) {
    Map<EngineHintKey, EngineHint> hints =
        EngineHintsMapper.toHints(engineKind, engineVersion, def.engineSpecific());
    return new CastNode(
        resourceId(engineKind, def),
        version,
        Instant.EPOCH,
        engineKind,
        resourceId(engineKind, ResourceKind.RK_TYPE, def.sourceType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.targetType()),
        def.method().wireValue(),
        hints);
  }

  private CollationNode toCollationNode(
      String engineKind, String engineVersion, long version, SystemCollationDef def) {
    Map<EngineHintKey, EngineHint> hints =
        EngineHintsMapper.toHints(engineKind, engineVersion, def.engineSpecific());

    return new CollationNode(
        resourceId(engineKind, def),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        def.locale(),
        hints);
  }

  private AggregateNode toAggregateNode(
      String engineKind, String engineVersion, long version, SystemAggregateDef def) {
    Map<EngineHintKey, EngineHint> hints =
        EngineHintsMapper.toHints(engineKind, engineVersion, def.engineSpecific());

    return new AggregateNode(
        resourceId(engineKind, def),
        version,
        Instant.EPOCH,
        engineKind,
        safeName(def.name()),
        def.argumentTypes().stream()
            .map(arg -> resourceId(engineKind, ResourceKind.RK_TYPE, arg))
            .toList(),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.stateType()),
        resourceId(engineKind, ResourceKind.RK_TYPE, def.returnType()),
        hints);
  }

  // =======================================================================
  // ResourceId helpers
  // =======================================================================

  /**
   * Universal ResourceId entrypoint for system objects.
   *
   * <p>This method centralizes the mapping from {@link SystemObjectDef} to {@link ResourceKind} and
   * the identity string strategy (canonical name vs overload-safe signature).
   */
  public static ResourceId resourceId(String engineKind, SystemObjectDef def) {
    String suffix = SignatureUtil.identityString(def);
    if (def == null) {
      return resourceId(engineKind, ResourceKind.RK_UNSPECIFIED, "");
    }
    return resourceId(engineKind, def.kind(), suffix);
  }

  /** ResourceId builder for objects identified by NameRef only (no overloads). */
  public static ResourceId resourceId(String engineKind, ResourceKind kind, NameRef name) {
    return resourceId(engineKind, kind, NameRefUtil.canonical(name));
  }

  /**
   * Single implementation used by all ResourceId construction.
   *
   * <p>Used for objects that can be overloaded with NameRef only (e.g., system-defined functions)
   * or other identifiers.
   */
  public static ResourceId resourceId(String engineKind, ResourceKind kind, String idSuffix) {
    String engine =
        (engineKind == null || engineKind.isBlank())
            ? EngineCatalogNames.FLOECAT_DEFAULT_CATALOG
            : engineKind;
    return ResourceId.newBuilder()
        .setAccountId(SYSTEM_ACCOUNT)
        .setKind(kind)
        .setId(engine + ":" + (idSuffix == null ? "" : idSuffix))
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

  private static Map<String, Map<EngineHintKey, EngineHint>> buildColumnHints(
      List<SystemColumnDef> columns, String engineKind, String engineVersion) {
    if (columns == null || columns.isEmpty()) {
      return Map.of();
    }
    Map<String, Map<EngineHintKey, EngineHint>> hints = new LinkedHashMap<>();
    for (SystemColumnDef column : columns) {
      Map<EngineHintKey, EngineHint> columnHints =
          EngineHintsMapper.toHints(engineKind, engineVersion, column.engineSpecific());
      if (!columnHints.isEmpty()) {
        hints.put(column.name(), columnHints);
      }
    }
    return hints.isEmpty() ? Map.of() : hints;
  }

  private static Optional<ResourceId> findNamespaceId(
      NameRef name, Map<String, ResourceId> namespaceIds) {
    if (name == null) {
      return Optional.empty();
    }
    String canonical = NameRefUtil.canonical(name);
    int idx = canonical.lastIndexOf('.');
    if (idx < 0) {
      return Optional.empty();
    }
    String namespaceKey = canonical.substring(0, idx);
    if (namespaceKey.isBlank()) {
      return Optional.empty();
    }
    return Optional.ofNullable(namespaceIds.get(namespaceKey));
  }

  private static void logMissingNamespace(String objectType, NameRef name) {
    String canonical = name == null ? "<unknown>" : NameRefUtil.canonical(name);
    String namespaceKey = "";
    if (canonical != null) {
      int dot = canonical.lastIndexOf('.');
      if (dot > 0) {
        namespaceKey = canonical.substring(0, dot);
      }
    }
    if (namespaceKey.isBlank()) {
      namespaceKey = "<root>";
    }
    LOG.warnf(
        "Skipping system %s %s because namespace %s is missing",
        objectType, canonical, namespaceKey);
  }

  private static ResourceId systemCatalogId(String engineKind) {
    String id =
        (engineKind == null || engineKind.isBlank())
            ? EngineCatalogNames.FLOECAT_DEFAULT_CATALOG
            : engineKind;
    return ResourceId.newBuilder()
        .setAccountId(SYSTEM_ACCOUNT)
        .setKind(ResourceKind.RK_CATALOG)
        .setId(id)
        .build();
  }

  private static <T extends GraphNode> Map<ResourceId, List<T>> freezeNamespaceMap(
      Map<ResourceId, List<T>> map) {
    if (map == null || map.isEmpty()) {
      return Map.of();
    }
    Map<ResourceId, List<T>> frozen = new LinkedHashMap<>();
    for (Map.Entry<ResourceId, List<T>> entry : map.entrySet()) {
      frozen.put(entry.getKey(), List.copyOf(entry.getValue()));
    }
    return Map.copyOf(frozen);
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
      List<NamespaceNode> namespaceNodes,
      List<SystemTableNode> tableNodes,
      List<ViewNode> viewNodes,
      Map<ResourceId, List<TableNode>> tablesByNamespace,
      Map<ResourceId, List<ViewNode>> viewsByNamespace,
      Map<String, ResourceId> tableNames,
      Map<String, ResourceId> viewNames,
      Map<String, ResourceId> namespaceNames,
      SystemCatalogData catalogData) {

    public BuiltinNodes {
      functions = List.copyOf(functions);
      operators = List.copyOf(operators);
      types = List.copyOf(types);
      casts = List.copyOf(casts);
      collations = List.copyOf(collations);
      aggregates = List.copyOf(aggregates);
      namespaceNodes = List.copyOf(namespaceNodes);
      tableNodes = List.copyOf(tableNodes);
      viewNodes = List.copyOf(viewNodes);
      tablesByNamespace = tablesByNamespace == null ? Map.of() : tablesByNamespace;
      viewsByNamespace = viewsByNamespace == null ? Map.of() : viewsByNamespace;
      tableNames = Map.copyOf(tableNames);
      viewNames = Map.copyOf(viewNames);
      namespaceNames = Map.copyOf(namespaceNames);
      catalogData = Objects.requireNonNull(catalogData);
    }

    public SystemCatalogData toCatalogData() {
      return catalogData;
    }
  }

  private record VersionKey(String engineKind, String engineVersion) {}
}
