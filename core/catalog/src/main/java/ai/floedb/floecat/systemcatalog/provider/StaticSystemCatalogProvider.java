package ai.floedb.floecat.systemcatalog.provider;

import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Test-only provider for supplying fixed builtin catalogs. */
public final class StaticSystemCatalogProvider implements SystemCatalogProvider {

  private final Map<String, SystemCatalogData> catalogs = new HashMap<>();

  public StaticSystemCatalogProvider(Map<String, SystemCatalogData> input) {
    input.forEach((kind, data) -> catalogs.put(kind.toLowerCase(Locale.ROOT), data));
  }

  @Override
  public List<String> engineKinds() {
    return catalogs.keySet().stream().sorted().toList();
  }

  @Override
  public SystemEngineCatalog load(String engineKind) {
    SystemCatalogData data = catalogs.get(engineKind.toLowerCase(Locale.ROOT));
    if (data != null) {
      return SystemEngineCatalog.from(engineKind, data);
    }
    return SystemEngineCatalog.from(engineKind, SystemCatalogData.empty());
  }
}
