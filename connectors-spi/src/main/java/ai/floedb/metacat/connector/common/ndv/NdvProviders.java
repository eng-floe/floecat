package ai.floedb.metacat.connector.common.ndv;

import java.util.Locale;
import java.util.Set;

public final class NdvProviders {
  private NdvProviders() {}

  public static NdvProvider filterBySuffix(Set<String> lowercaseSuffixes, NdvProvider delegate) {
    return (filePath, sinks) -> {
      String p = filePath.toLowerCase(Locale.ROOT);
      for (String s : lowercaseSuffixes) {
        if (p.endsWith(s)) {
          delegate.contributeNdv(filePath, sinks);
          break;
        }
      }
    };
  }

  public static NdvProvider chain(NdvProvider... providers) {
    return (filePath, sinks) -> {
      for (NdvProvider p : providers) if (p != null) p.contributeNdv(filePath, sinks);
    };
  }
}
