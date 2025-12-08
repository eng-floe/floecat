package ai.floedb.floecat.catalog.builtin.provider;

import ai.floedb.floecat.catalog.builtin.registry.BuiltinCatalogData;
import ai.floedb.floecat.catalog.builtin.registry.BuiltinEngineCatalog;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Test-only provider for supplying fixed builtin catalogs. */
public final class StaticBuiltinCatalogProvider implements BuiltinCatalogProvider {

  private final Map<String, BuiltinEngineCatalog> catalogs = new HashMap<>();

  public StaticBuiltinCatalogProvider(Map<String, BuiltinCatalogData> input) {
    input.forEach(
        (kind, data) ->
            catalogs.put(kind.toLowerCase(Locale.ROOT), BuiltinEngineCatalog.from(kind, data)));
  }

  @Override
  public BuiltinEngineCatalog load(String engineKind) {
    return catalogs.getOrDefault(
        engineKind.toLowerCase(Locale.ROOT),
        BuiltinEngineCatalog.from(engineKind, BuiltinCatalogData.empty()));
  }
}
