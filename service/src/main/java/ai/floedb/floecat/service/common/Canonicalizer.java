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

package ai.floedb.floecat.service.common;

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
    String key = (k == null) ? "" : k.trim();
    if (prefix.isEmpty()) {
      return key;
    }

    var it = prefix.descendingIterator();
    StringBuilder full = new StringBuilder(32);
    while (it.hasNext()) {
      if (full.length() > 0) full.append('.');
      full.append(it.next());
    }
    if (!key.isBlank()) {
      if (full.length() > 0) full.append('.');
      full.append(key);
    }
    return full.toString();
  }

  public Canonicalizer group(String k, Consumer<Canonicalizer> nested) {
    String key = (k == null) ? "" : k.trim();
    if (!key.isBlank()) prefix.push(key);
    try {
      nested.accept(this);
    } finally {
      if (!key.isBlank()) prefix.pop();
    }
    return this;
  }

  public Canonicalizer scalar(String k, Object v) {
    sb.append(buildKey(k)).append('=');
    appendValue(v);
    sb.append('\n');
    return this;
  }

  public Canonicalizer list(String k, List<?> vs) {
    if (vs != null) {
      String key = buildKey(k);
      for (Object v : vs) {
        sb.append(key).append("[]=");
        appendValue(v);
        sb.append('\n');
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
              e -> {
                sb.append(key).append("[]k=");
                appendValue(e.getKey());
                sb.append(" v=");
                appendValue(e.getValue());
                sb.append('\n');
              });
    }
    return this;
  }

  public byte[] bytes() {
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  private void appendValue(Object v) {
    sb.append(valueToken(v));
  }

  private static String valueToken(Object v) {
    String s = (v == null) ? "" : v.toString();
    return s.length() + ":" + s;
  }
}
