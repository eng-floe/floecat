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

import ai.floedb.floecat.engine.util.EngineIdentityNormalizer;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.scanner.utils.EngineCatalogNames;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.spi.EngineCatalogProvider;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecoratorProvider;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.validation.SystemCatalogValidator;
import ai.floedb.floecat.systemcatalog.validation.ValidationFailures;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssueFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jboss.logging.Logger;

/**
 * Production implementation of SystemCatalogProvider. Discovers engine catalog providers using
 * ServiceLoader.
 */
public final class ServiceLoaderSystemCatalogProvider
    implements SystemCatalogProvider, EngineMetadataDecoratorProvider {

  private static final Logger LOG = Logger.getLogger(ServiceLoaderSystemCatalogProvider.class);
  private static final int VALIDATION_LOG_LIMIT = 50;

  private static final FloecatInternalProvider FLOECAT_INTERNAL_PROVIDER =
      new FloecatInternalProvider();

  private final Map<String, EngineCatalogProvider> providersByEngine;
  private final List<CatalogEnvironmentProvider> environmentProviders;
  private final Map<String, EngineMetadataDecorator> decorators;
  private final List<SystemObjectScannerProvider> providers;

  public ServiceLoaderSystemCatalogProvider() {
    List<EngineCatalogProvider> engineProviders;
    try {
      engineProviders =
          ServiceLoader.load(EngineCatalogProvider.class).stream()
              .map(ServiceLoader.Provider::get)
              .toList();
    } catch (Exception e) {
      LOG.warn("Failed to load EngineCatalogProvider implementations", e);
      engineProviders = List.of();
    }

    Map<String, EngineCatalogProvider> providerMap = new HashMap<>();
    Map<String, EngineMetadataDecorator> decoratorMap = new HashMap<>();
    List<EngineCatalogProvider> acceptedEngineProviders = new ArrayList<>();
    for (EngineCatalogProvider provider : engineProviders) {
      String normalizedKind = EngineIdentityNormalizer.normalizeEngineKind(provider.engineKind());
      if (normalizedKind.isEmpty()) {
        continue;
      }
      if (EngineCatalogNames.FLOECAT_DEFAULT_CATALOG.equals(normalizedKind)) {
        LOG.warn(
            "EngineCatalogProvider for floecat_internal is reserved; ignoring "
                + provider.getClass());
        continue;
      }
      acceptedEngineProviders.add(provider);
      EngineCatalogProvider previous = providerMap.put(normalizedKind, provider);
      if (previous != null) {
        throw new IllegalStateException(
            "Multiple engine catalog providers registered for engine_kind="
                + normalizedKind
                + " (prev="
                + previous.getClass().getName()
                + ", next="
                + provider.getClass().getName()
                + ")");
      }
      provider
          .decorator()
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

    this.providersByEngine = Map.copyOf(providerMap);
    this.decorators = Map.copyOf(decoratorMap);

    List<CatalogEnvironmentProvider> loadedEnvironmentProviders;
    try {
      loadedEnvironmentProviders =
          ServiceLoader.load(CatalogEnvironmentProvider.class).stream()
              .map(ServiceLoader.Provider::get)
              .toList();
    } catch (Exception e) {
      LOG.warn("Failed to load CatalogEnvironmentProvider implementations", e);
      loadedEnvironmentProviders = List.of();
    }
    this.environmentProviders = List.copyOf(loadedEnvironmentProviders);

    this.providers =
        acceptedEngineProviders.stream()
            .map(provider -> (SystemObjectScannerProvider) provider)
            .toList();
  }

  @Override
  public List<String> engineKinds() {
    return Stream.concat(
            Stream.of(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG),
            providersByEngine.keySet().stream())
        .distinct()
        .sorted()
        .toList();
  }

  @Override
  public SystemEngineCatalog load(CatalogContext context) {
    CatalogContext canonical = Objects.requireNonNull(context, "context");
    EngineContext engine = canonical.engine();

    // Rule: no header => floecat_internal only (which includes information_schema).
    String effectiveKind = engine.effectiveEngineKind();
    boolean overlaysRequested = engine.enginePluginOverlaysEnabled();

    EngineCatalogProvider provider = providersByEngine.get(effectiveKind);
    SystemCatalogData catalog;

    if (provider == null) {
      // No plugin registered: still serve floecat-internal (merged in later).
      if (overlaysRequested) {
        LOG.warn(
            "No engine catalog provider found for engine_kind="
                + effectiveKind
                + " (ctx="
                + engine.engineKind()
                + "), defaulting to floecat_internal-only content scoped as "
                + effectiveKind);
      }
      catalog = SystemCatalogData.empty();
    } else {
      LOG.info(
          "Loading engine catalog provider for engine_kind="
              + effectiveKind
              + " (ctx="
              + engine.engineKind()
              + ")");
      catalog = provider.loadSystemCatalog();

      List<ValidationIssue> extErrors = provider.validate(catalog);
      if (!extErrors.isEmpty()) {
        logValidationIssues(provider, extErrors);
      }
      ValidationFailures.throwOnErrorIssues(
          "Engine extension validation failed for engine_kind=" + effectiveKind, extErrors);

      List<ValidationIssue> validatorIssues = SystemCatalogValidator.validate(catalog);
      if (!validatorIssues.isEmpty()) {
        logValidationIssues(provider, validatorIssues);
      }
      ValidationFailures.throwOnErrorIssues(
          "System catalog validation failed for engine_kind=" + effectiveKind, validatorIssues);
    }

    catalog = mergeWithInternalCatalog(catalog);

    String resolvedEngineKind =
        engine.hasEngineHeaders() ? effectiveKind : EngineCatalogNames.FLOECAT_DEFAULT_CATALOG;

    return SystemEngineCatalog.from(resolvedEngineKind, catalog);
  }

  public List<SystemObjectScannerProvider> providers() {
    return providers;
  }

  /** Returns all environment providers discovered through the service loader. */
  public List<CatalogEnvironmentProvider> environmentProviders() {
    return environmentProviders;
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
   * Returns the engine catalog provider for the provided engine kind. Versions are handled by the
   * provider through the supplied {@link ai.floedb.floecat.scanner.utils.EngineContext}.
   */
  public Optional<EngineCatalogProvider> providerFor(String engineKind) {
    if (engineKind == null || engineKind.isBlank()) {
      return Optional.empty();
    }
    return Optional.ofNullable(
        providersByEngine.get(EngineIdentityNormalizer.normalizeEngineKind(engineKind)));
  }

  private static void logValidationIssues(
      EngineCatalogProvider provider, List<ValidationIssue> issues) {
    LOG.warn(
        "Engine extension emitted "
            + issues.size()
            + " validation issues for engine_kind="
            + provider.engineKind());
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
