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

package ai.floedb.floecat.stats.spi;

import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Shared value-level statistics reused across column and expression targets. */
public record StatsValueSummary(
    long valueCount,
    Optional<Long> nullCount,
    Optional<Long> nanCount,
    Optional<Ndv> ndv,
    Optional<String> min,
    Optional<String> max,
    Optional<byte[]> histogram,
    Optional<byte[]> tdigest,
    Optional<StatsMetadata> metadata,
    Map<String, String> properties) {

  public StatsValueSummary {
    nullCount = Objects.requireNonNullElse(nullCount, Optional.empty());
    nanCount = Objects.requireNonNullElse(nanCount, Optional.empty());
    ndv = Objects.requireNonNullElse(ndv, Optional.empty());
    min = Objects.requireNonNullElse(min, Optional.empty());
    max = Objects.requireNonNullElse(max, Optional.empty());
    Optional<byte[]> normalizedHistogram = histogram == null ? Optional.empty() : histogram;
    Optional<byte[]> normalizedTdigest = tdigest == null ? Optional.empty() : tdigest;
    histogram = normalizedHistogram.map(v -> Arrays.copyOf(v, v.length));
    tdigest = normalizedTdigest.map(v -> Arrays.copyOf(v, v.length));
    metadata = Objects.requireNonNullElse(metadata, Optional.empty());
    properties = Map.copyOf(Objects.requireNonNullElse(properties, Map.of()));
  }

  public static StatsValueSummary fromColumn(
      ai.floedb.floecat.catalog.rpc.ColumnStats columnStats) {
    Objects.requireNonNull(columnStats, "columnStats");
    return new StatsValueSummary(
        columnStats.getValueCount(),
        columnStats.hasNullCount() ? Optional.of(columnStats.getNullCount()) : Optional.empty(),
        columnStats.hasNanCount() ? Optional.of(columnStats.getNanCount()) : Optional.empty(),
        columnStats.hasNdv() ? Optional.of(columnStats.getNdv()) : Optional.empty(),
        columnStats.hasMin() ? Optional.of(columnStats.getMin()) : Optional.empty(),
        columnStats.hasMax() ? Optional.of(columnStats.getMax()) : Optional.empty(),
        columnStats.getHistogram().isEmpty()
            ? Optional.empty()
            : Optional.of(columnStats.getHistogram().toByteArray()),
        columnStats.getTdigest().isEmpty()
            ? Optional.empty()
            : Optional.of(columnStats.getTdigest().toByteArray()),
        columnStats.hasMetadata() ? Optional.of(columnStats.getMetadata()) : Optional.empty(),
        columnStats.getPropertiesMap());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof StatsValueSummary other)) {
      return false;
    }
    return valueCount == other.valueCount
        && nullCount.equals(other.nullCount)
        && nanCount.equals(other.nanCount)
        && ndv.equals(other.ndv)
        && min.equals(other.min)
        && max.equals(other.max)
        && optionalBytesEqual(histogram, other.histogram)
        && optionalBytesEqual(tdigest, other.tdigest)
        && metadata.equals(other.metadata)
        && properties.equals(other.properties);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(valueCount, nullCount, nanCount, ndv, min, max, metadata, properties);
    result = 31 * result + optionalBytesHash(histogram);
    result = 31 * result + optionalBytesHash(tdigest);
    return result;
  }

  private static boolean optionalBytesEqual(Optional<byte[]> left, Optional<byte[]> right) {
    if (left.isEmpty() && right.isEmpty()) {
      return true;
    }
    if (left.isEmpty() || right.isEmpty()) {
      return false;
    }
    return Arrays.equals(left.get(), right.get());
  }

  private static int optionalBytesHash(Optional<byte[]> value) {
    return value.map(Arrays::hashCode).orElse(0);
  }
}
