package ai.floedb.floecat.systemcatalog.provider;

import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.informationschema.InformationSchemaProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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

  private final Map<String, EngineSystemCatalogExtension> plugins;
  private final List<SystemObjectScannerProvider> providers;

  public ServiceLoaderSystemCatalogProvider() {
    var engineExtensions =
        ServiceLoader.load(EngineSystemCatalogExtension.class).stream()
            .map(ServiceLoader.Provider::get)
            .toList();
    Map<String, EngineSystemCatalogExtension> tmp = new HashMap<>();
    engineExtensions.forEach(ext -> tmp.put(ext.engineKind().toLowerCase(Locale.ROOT), ext));
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
        Stream.concat(Stream.of(new InformationSchemaProvider()), extensionProviders)
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public SystemEngineCatalog load(String engineKind) {
    if (engineKind == null || engineKind.isBlank()) {
      throw new IllegalArgumentException("engine_kind must be provided");
    }

    EngineSystemCatalogExtension ext = plugins.get(engineKind.toLowerCase(Locale.ROOT));

    if (ext == null) {
      LOG.warn("No builtin plugin found for engine_kind=" + engineKind);
      return SystemEngineCatalog.from(engineKind, SystemCatalogData.empty());
    }

    // Merge the scanner-provided namespace/table/view defs into the base catalog snapshot before
    // materializing it so both sources share the same cache.
    SystemCatalogData data = mergeProviderDefinitions(engineKind, ext.loadSystemCatalog());
    return SystemEngineCatalog.from(engineKind, data);
  }

  public List<SystemObjectScannerProvider> providers() {
    return providers;
  }

  private SystemCatalogData mergeProviderDefinitions(String engineKind, SystemCatalogData base) {

    Map<String, SystemNamespaceDef> namespaceByName = new LinkedHashMap<>();
    Map<String, SystemTableDef> tableByName = new LinkedHashMap<>();
    Map<String, SystemViewDef> viewByName = new LinkedHashMap<>();

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
      if (!provider.supportsEngine(engineKind)) {
        continue;
      }
      for (var def : provider.definitions()) {
        if (!provider.supports(def.name(), engineKind)) {
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
