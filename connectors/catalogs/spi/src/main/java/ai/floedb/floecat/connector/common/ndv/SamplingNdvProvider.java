package ai.floedb.floecat.connector.common.ndv;

import java.util.Map;
import java.util.Objects;
import java.util.Random;

public final class SamplingNdvProvider implements NdvProvider {
  private final NdvProvider delegate;
  private final double fraction;
  private final long maxFiles;
  private long seen = 0;
  private final Random rnd;

  public SamplingNdvProvider(NdvProvider delegate, double fraction, long maxFiles) {
    this.delegate = Objects.requireNonNull(delegate, "delegate NDV provider required");

    if (fraction <= 0.0 || fraction > 1.0) {
      this.fraction = 1.0;
    } else {
      this.fraction = fraction;
    }

    this.maxFiles = Math.max(0, maxFiles);

    this.rnd = new Random(1L);
  }

  @Override
  public void contributeNdv(String filePath, Map<String, ColumnNdv> sinks) throws Exception {
    seen++;

    if (maxFiles > 0 && seen > maxFiles) {
      return;
    }

    if (fraction < 1.0 && rnd.nextDouble() > fraction) {
      return;
    }

    delegate.contributeNdv(filePath, sinks);
  }
}
