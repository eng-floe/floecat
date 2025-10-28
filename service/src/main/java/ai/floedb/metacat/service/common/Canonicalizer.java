package ai.floedb.metacat.service.common;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public final class Canonicalizer {
  private final StringBuilder sb = new StringBuilder(256);

  public Canonicalizer scalar(String k, Object v) {
    sb.append(k).append('=').append(v == null ? "" : v).append('\n');
    return this;
  }

  public Canonicalizer list(String k, List<?> vs) {
    if (vs != null) for (Object v : vs) sb.append(k).append("[]=").append(v).append('\n');
    return this;
  }

  public <V> Canonicalizer map(String k, Map<String, V> m) {
    if (m != null)
      m.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .forEach(
              e ->
                  sb.append(k)
                      .append('[')
                      .append(e.getKey())
                      .append("]=")
                      .append(e.getValue())
                      .append('\n'));
    return this;
  }

  public byte[] bytes() {
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }
}
