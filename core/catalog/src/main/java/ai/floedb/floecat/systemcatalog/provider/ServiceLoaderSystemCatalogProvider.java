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

package ai.floedb.floecat.systemcatalog.provider;

import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.informationschema.InformationSchemaProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension;
import ai.floedb.floecat.systemcatalog.util.EngineCatalogNames;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import ai.floedb.floecat.systemcatalog.util.EngineContextNormalizer;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jboss.logging.Logger;

/**
 * Production implementation of SystemCatalogProvider. Discovers EngineSystemCatalogExtension
 * implementations using ServiceLoader.
 */
public final class ServiceLoaderSystemCatalogProvider implements SystemCatalogProvider {

  private static final Logger LOG = Logger.getLogger(ServiceLoaderSystemCatalogProvider.class);

  private static final InformationSchemaProvider INFORMATION_SCHEMA_PROVIDER =
      new InformationSchemaProvider();

  private final Map<String, EngineSystemCatalogExtension> plugins;
  private final List<SystemObjectScannerProvider> providers;

  public ServiceLoaderSystemCatalogProvider() {
    List<EngineSystemCatalogExtension> engineExtensions;
    try {
      engineExtensions =
          ServiceLoader.load(EngineSystemCatalogExtension.class).stream()
              .map(ServiceLoader.Provider::get)
              .toList();
    } catch (Exception e) {
      LOG.warn("Failed to load EngineSystemCatalogExtension implementations", e);
      engineExtensions = List.of();
    }
    Map<String, EngineSystemCatalogExtension> tmp = new HashMap<>();
    for (EngineSystemCatalogExtension ext : engineExtensions) {
      String normalizedKind = EngineContextNormalizer.normalizeEngineKind(ext.engineKind());
      if (normalizedKind.isEmpty()) {
        continue;
      }
      EngineSystemCatalogExtension previous = tmp.put(normalizedKind, ext);
      if (previous != null) {
        LOG.warnf(
            "Replacing system catalog extension for engine_kind=%s (prev=%s, next=%s)",
            normalizedKind, previous.getClass().getName(), ext.getClass().getName());
      }
    }
    this.plugins = Map.copyOf(tmp);

    /*
     * Extract every SystemObjectScannerProvider from the extensions so we can merge any extra
     * namespace/table/view definitions into the cached catalog later on.
     * Floecat default Information schema objects are always added but can be overwritten by the
     * plugins own definition of the schema.
     *
     * Only the builtin information schema provider provides standalone definitions; every other
     * system-object provider implementations ships a full EngineSystemCatalogExtension so we
     * can reuse the same discovery stream.
     */
    Stream<SystemObjectScannerProvider> extensionProviders =
        engineExtensions.stream().map(ext -> (SystemObjectScannerProvider) ext);

    this.providers =
        Stream.concat(Stream.of(INFORMATION_SCHEMA_PROVIDER), extensionProviders)
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<String> engineKinds() {
    return plugins.keySet().stream().sorted().toList();
  }

  @Override
  public SystemEngineCatalog load(String engineKind) {
    // Blank or missing engineKind returns the floecat internal catalog (FLOECAT_DEFAULT_CATALOG)
    // so that information_schema and its overrides remain available even without an explicit
    // engine header. Non-blank kinds still try to resolve an extension first.
    EngineContext ctx = EngineContext.of(engineKind, "");
    String normalizedKind = ctx.normalizedKind();
    EngineSystemCatalogExtension ext = plugins.get(normalizedKind);
    SystemCatalogData catalog;
    if (!ctx.hasEngineKind()) {
      catalog = SystemCatalogData.empty();
    } else if (ext == null) {
      if (!EngineCatalogNames.FLOECAT_DEFAULT_CATALOG.equals(normalizedKind)) {
        LOG.warn(
            "No system catalog plugin found for engine_kind="
                + normalizedKind
                + " (raw="
                + engineKind
                + "), defaulting to empty catalog");
      }
      catalog = SystemCatalogData.empty();
    } else {
      LOG.info(
          "Loading system catalog plugin for engine_kind="
              + normalizedKind
              + " (raw="
              + engineKind
              + ")");
      catalog = ext.loadSystemCatalog();
    }

    SystemCatalogData data = mergeProviderDefinitions(normalizedKind, catalog);
    return SystemEngineCatalog.from(normalizedKind, data);
  }

  public List<SystemObjectScannerProvider> providers() {
    return providers;
  }

  private SystemCatalogData mergeProviderDefinitions(
      String normalizedEngineKind, SystemCatalogData base) {

    Map<String, SystemNamespaceDef> namespaceByName = new LinkedHashMap<>();
    Map<String, SystemTableDef> tableByName = new LinkedHashMap<>();
    Map<String, SystemViewDef> viewByName = new LinkedHashMap<>();

    // Seed with InformationSchemaProvider definitions always by default
    for (var def : INFORMATION_SCHEMA_PROVIDER.definitions()) {
      if (def instanceof SystemNamespaceDef ns) {
        namespaceByName.put(NameRefUtil.canonical(ns.name()), ns);
      } else if (def instanceof SystemTableDef table) {
        tableByName.put(NameRefUtil.canonical(table.name()), table);
      } else if (def instanceof SystemViewDef view) {
        viewByName.put(NameRefUtil.canonical(view.name()), view);
      }
    }

    for (SystemNamespaceDef ns : base.namespaces()) {
      namespaceByName.put(NameRefUtil.canonical(ns.name()), ns);
    }
    for (SystemTableDef table : base.tables()) {
      tableByName.put(NameRefUtil.canonical(table.name()), table);
    }
    for (SystemViewDef view : base.views()) {
      viewByName.put(NameRefUtil.canonical(view.name()), view);
    }

    // Overlay provider definitions on top of the canonical catalog data, allowing last-wins
    // overrides per NameRef.
    for (SystemObjectScannerProvider provider : providers) {
      if (!provider.supportsEngine(normalizedEngineKind)) {
        continue;
      }
      for (var def : provider.definitions()) {
        if (!provider.supports(def.name(), normalizedEngineKind)) {
          continue;
        }

        if (def instanceof SystemNamespaceDef ns) {
          namespaceByName.put(NameRefUtil.canonical(ns.name()), ns);
        } else if (def instanceof SystemTableDef table) {
          tableByName.put(NameRefUtil.canonical(table.name()), table);
        } else if (def instanceof SystemViewDef view) {
          viewByName.put(NameRefUtil.canonical(view.name()), view);
        }
      }
    }

    // Return a fresh catalog snapshot containing all builtin definitions plus the merged overlays.
    return new SystemCatalogData(
        base.functions(),
        base.operators(),
        base.types(),
        base.casts(),
        base.collations(),
        base.aggregates(),
        List.copyOf(namespaceByName.values()),
        List.copyOf(tableByName.values()),
        List.copyOf(viewByName.values()));
  }
}
