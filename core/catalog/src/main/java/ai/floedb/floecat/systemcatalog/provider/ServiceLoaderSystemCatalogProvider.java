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
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecoratorProvider;
import ai.floedb.floecat.scanner.utils.EngineCatalogNames;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.scanner.utils.EngineContextNormalizer;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssueFormatter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jboss.logging.Logger;

/**
 * Production implementation of SystemCatalogProvider. Discovers EngineSystemCatalogExtension
 * implementations using ServiceLoader.
 */
public final class ServiceLoaderSystemCatalogProvider
    implements SystemCatalogProvider, EngineMetadataDecoratorProvider {

  private static final Logger LOG = Logger.getLogger(ServiceLoaderSystemCatalogProvider.class);
  private static final int VALIDATION_LOG_LIMIT = 50;

  private static final FloecatInternalProvider FLOECAT_INTERNAL_PROVIDER =
      new FloecatInternalProvider();

  private final Map<String, EngineSystemCatalogExtension> plugins;
  private final Map<String, EngineMetadataDecorator> decorators;
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
    Map<String, EngineMetadataDecorator> decoratorMap = new HashMap<>();
    for (EngineSystemCatalogExtension ext : engineExtensions) {
      String normalizedKind = EngineContextNormalizer.normalizeEngineKind(ext.engineKind());
      if (normalizedKind.isEmpty()) {
        continue;
      }
      if (EngineCatalogNames.FLOECAT_DEFAULT_CATALOG.equals(normalizedKind)) {
        LOG.warn(
            "EngineSystemCatalogExtension for floecat_internal is reserved; ignoring "
                + ext.getClass());
        continue;
      }
      EngineSystemCatalogExtension previous = tmp.put(normalizedKind, ext);
      if (previous != null) {
        throw new IllegalStateException(
            "Multiple system catalog extensions registered for engine_kind="
                + normalizedKind
                + " (prev="
                + previous.getClass().getName()
                + ", next="
                + ext.getClass().getName()
                + ")");
      }
      ext.decorator()
          .ifPresent(
              dec -> {
                EngineMetadataDecorator previousDecorator = decoratorMap.put(normalizedKind, dec);
                if (previousDecorator != null) {
                  throw new IllegalStateException(
                      "Multiple decorators registered for engine_kind="
                          + normalizedKind
                          + " (prev="
                          + previousDecorator.getClass().getName()
                          + ", next="
                          + dec.getClass().getName()
                          + ")");
                }
              });
    }
    this.plugins = Map.copyOf(tmp);
    this.decorators = Map.copyOf(decoratorMap);

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
    List<SystemObjectScannerProvider> extensionProviders =
        engineExtensions.stream().map(ext -> (SystemObjectScannerProvider) ext).toList();

    this.providers = extensionProviders.stream().collect(Collectors.toUnmodifiableList());
  }

  @Override
  public List<String> engineKinds() {
    return Stream.concat(
            Stream.of(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG), plugins.keySet().stream())
        .distinct()
        .sorted()
        .toList();
  }

  @Override
  public SystemEngineCatalog load(EngineContext ctx) {
    EngineContext canonical = ctx == null ? EngineContext.empty() : ctx;

    // Rule: no header => floecat_internal only (which includes information_schema).
    String effectiveKind = canonical.effectiveEngineKind();
    boolean overlaysRequested = canonical.enginePluginOverlaysEnabled();

    EngineSystemCatalogExtension ext = plugins.get(effectiveKind);
    SystemCatalogData catalog;

    if (ext == null) {
      // No plugin registered: still serve floecat-internal (merged in later).
      if (overlaysRequested) {
        LOG.warn(
            "No system catalog plugin found for engine_kind="
                + effectiveKind
                + " (ctx="
                + canonical.engineKind()
                + "), defaulting to floecat_internal-only content scoped as "
                + effectiveKind);
      }
      catalog = SystemCatalogData.empty();
    } else {
      LOG.info(
          "Loading system catalog plugin for engine_kind="
              + effectiveKind
              + " (ctx="
              + canonical.engineKind()
              + ")");
      catalog = ext.loadSystemCatalog();

      List<ValidationIssue> extErrors = ext.validate(catalog);
      if (!extErrors.isEmpty()) {
        logValidationIssues(ext, extErrors);
      }
    }

    catalog = mergeWithInternalCatalog(catalog);

    String resolvedEngineKind =
        canonical.hasEngineHeaders() ? effectiveKind : EngineCatalogNames.FLOECAT_DEFAULT_CATALOG;

    return SystemEngineCatalog.from(resolvedEngineKind, catalog);
  }

  public List<SystemObjectScannerProvider> providers() {
    return providers;
  }

  /** Returns the floecat_internal provider that always seeds every catalog build. */
  public FloecatInternalProvider internalProvider() {
    return FLOECAT_INTERNAL_PROVIDER;
  }

  /** Returns the decorator registered for the given engine, if any. */
  @Override
  public Optional<EngineMetadataDecorator> decorator(EngineContext ctx) {
    if (ctx == null || !ctx.enginePluginOverlaysEnabled()) {
      return Optional.empty();
    }
    return Optional.ofNullable(decorators.get(ctx.effectiveEngineKind()));
  }

  /**
   * Returns the install extension for the provided engine kind. Versions are handled by the
   * extension itself through the provided {@link
   * ai.floedb.floecat.scanner.utils.EngineContext}.
   */
  public Optional<EngineSystemCatalogExtension> extensionFor(String engineKind) {
    if (engineKind == null || engineKind.isBlank()) {
      return Optional.empty();
    }
    return Optional.ofNullable(plugins.get(engineKind));
  }

  private static void logValidationIssues(
      EngineSystemCatalogExtension ext, List<ValidationIssue> issues) {
    LOG.warn(
        "Engine extension emitted "
            + issues.size()
            + " validation issues for engine_kind="
            + ext.engineKind());
    int limit = Math.min(VALIDATION_LOG_LIMIT, issues.size());
    for (int i = 0; i < limit; i++) {
      LOG.warn("Engine validation: " + ValidationIssueFormatter.format(issues.get(i)));
    }
    if (issues.size() > VALIDATION_LOG_LIMIT) {
      LOG.warn(
          "Engine validation: results truncated (showing first "
              + VALIDATION_LOG_LIMIT
              + " of "
              + issues.size()
              + ")");
    }

    Map<String, Long> counts =
        issues.stream()
            .map(ValidationIssue::code)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    counts.entrySet().stream()
        .sorted(Map.Entry.<String, Long>comparingByValue(Comparator.reverseOrder()))
        .limit(5)
        .forEach(
            entry -> LOG.warnf("Engine validation count: %s=%d", entry.getKey(), entry.getValue()));
  }

  private static SystemCatalogData mergeWithInternalCatalog(SystemCatalogData baseCatalog) {
    SystemCatalogData internalCatalog = FloecatInternalProvider.catalogData();

    Map<String, SystemNamespaceDef> namespaceByName = new LinkedHashMap<>();
    Map<String, SystemTableDef> tableByName = new LinkedHashMap<>();
    Map<String, SystemViewDef> viewByName = new LinkedHashMap<>();

    overlayDefinitions(internalCatalog.namespaces(), namespaceByName);
    overlayDefinitions(baseCatalog.namespaces(), namespaceByName);
    overlayDefinitions(internalCatalog.tables(), tableByName);
    overlayDefinitions(baseCatalog.tables(), tableByName);
    overlayDefinitions(internalCatalog.views(), viewByName);
    overlayDefinitions(baseCatalog.views(), viewByName);

    List<EngineSpecificRule> registryRules =
        Stream.concat(
                internalCatalog.registryEngineSpecific().stream(),
                baseCatalog.registryEngineSpecific().stream())
            .toList();

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

  private static <T extends SystemObjectDef> void overlayDefinitions(
      List<T> definitions, Map<String, T> target) {
    for (T def : definitions) {
      target.put(NameRefUtil.canonical(def.name()), def);
    }
  }
}
