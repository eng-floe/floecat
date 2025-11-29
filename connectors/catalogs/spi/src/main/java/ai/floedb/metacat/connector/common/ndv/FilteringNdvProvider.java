package ai.floedb.metacat.connector.common.ndv;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

public final class FilteringNdvProvider implements NdvProvider {
  private final Predicate<String> pathFilter;
  private final NdvProvider delegate;

  public FilteringNdvProvider(Predicate<String> pathFilter, NdvProvider delegate) {
    this.pathFilter = pathFilter;
    this.delegate = delegate;
  }

  @Override
  public void contributeNdv(String filePath, Map<String, ColumnNdv> sinks) throws Exception {
    if (filePath != null && pathFilter.test(filePath)) {
      delegate.contributeNdv(filePath, sinks);
    }
  }

  public static FilteringNdvProvider bySuffix(Set<String> lowercaseSuffixes, NdvProvider delegate) {
    return new FilteringNdvProvider(
        p -> {
          String s = p.toLowerCase(Locale.ROOT);
          for (String suf : lowercaseSuffixes) {
            if (s.endsWith(suf)) {
              return true;
            }
          }

          return false;
        },
        delegate);
  }
}
