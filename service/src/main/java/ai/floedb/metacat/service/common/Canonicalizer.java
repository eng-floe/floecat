package ai.floedb.metacat.service.common;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public final class Canonicalizer {
  private final StringBuilder sb = new StringBuilder(256);
  private final Deque<String> prefix = new ArrayDeque<>();

  private String buildKey(String k) {
    if (prefix.isEmpty()) return k;
    var it = prefix.descendingIterator();
    StringBuilder full = new StringBuilder(32);
    while (it.hasNext()) {
      if (full.length() > 0) full.append('.');
      full.append(it.next());
    }
    if (k != null && !k.isBlank()) {
      if (full.length() > 0) full.append('.');
      full.append(k);
    }
    return full.toString();
  }

  public Canonicalizer group(String k, Consumer<Canonicalizer> nested) {
    if (k != null && !k.isBlank()) prefix.push(k);
    try {
      nested.accept(this);
    } finally {
      if (k != null && !k.isBlank()) prefix.pop();
    }
    return this;
  }

  public Canonicalizer scalar(String k, Object v) {
    sb.append(buildKey(k)).append('=').append(v == null ? "" : v).append('\n');
    return this;
  }

  public Canonicalizer list(String k, List<?> vs) {
    if (vs != null) {
      String key = buildKey(k);
      for (Object v : vs) {
        sb.append(key).append("[]=").append(v == null ? "" : v).append('\n');
      }
    }
    return this;
  }

  public <V> Canonicalizer map(String k, Map<String, V> m) {
    if (m != null) {
      String key = buildKey(k);
      m.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .forEach(
              e ->
                  sb.append(key)
                      .append('[')
                      .append(e.getKey())
                      .append("]=")
                      .append(e.getValue() == null ? "" : e.getValue())
                      .append('\n'));
    }
    return this;
  }

  public byte[] bytes() {
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }
}
