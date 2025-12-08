package ai.floedb.floecat.connector.common.ndv;

import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public final class StaticOnceNdvProvider implements NdvProvider {
  private final Map<String, ColumnNdv> byName;
  private final AtomicBoolean done = new AtomicBoolean(false);

  public StaticOnceNdvProvider(Map<String, ColumnNdv> byName) {
    this.byName = byName;
  }

  @Override
  public void contributeNdv(String filePath, Map<String, ColumnNdv> sinks) {
    if (!done.compareAndSet(false, true)) {
      return;
    }

    if (byName == null || byName.isEmpty() || sinks == null || sinks.isEmpty()) {
      return;
    }

    for (var e : byName.entrySet()) {
      ColumnNdv sink = sinks.get(e.getKey());
      ColumnNdv from = e.getValue();
      if (sink == null || from == null) {
        continue;
      }

      if (sink.approx == null && from.approx != null) {
        sink.approx = from.approx;
      }

      if (from.sketches != null && !from.sketches.isEmpty()) {
        for (var sk : from.sketches) {
          if (sk == null || sk.data == null) {
            continue;
          }

          String t = sk.type == null ? "" : sk.type.toLowerCase(Locale.ROOT);

          if (t.contains("datasketches") && t.contains("theta")) {
            sink.mergeTheta(sk.data);
          } else {
            if (sink.sketches == null) {
              sink.sketches = new ArrayList<>();
            }

            sink.sketches.add(sk);
          }
        }
      }
    }
  }
}
