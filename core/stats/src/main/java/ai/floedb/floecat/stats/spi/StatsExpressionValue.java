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

import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/** Expression-target statistics value shape. */
public record StatsExpressionValue(
    String engineKind,
    byte[] engineExpressionKey,
    String engineVersion,
    String logicalType,
    Optional<UpstreamStamp> upstream,
    StatsValueSummary valueStats) {

  public StatsExpressionValue {
    engineKind = engineKind == null ? "" : engineKind;
    engineExpressionKey = engineExpressionKey == null ? new byte[0] : engineExpressionKey.clone();
    engineVersion = engineVersion == null ? "" : engineVersion;
    logicalType = logicalType == null ? "" : logicalType;
    upstream = Objects.requireNonNullElse(upstream, Optional.empty());
    valueStats = Objects.requireNonNull(valueStats, "valueStats");
  }

  @Override
  public byte[] engineExpressionKey() {
    return engineExpressionKey.clone();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof StatsExpressionValue other)) {
      return false;
    }
    return engineKind.equals(other.engineKind)
        && Arrays.equals(engineExpressionKey, other.engineExpressionKey)
        && engineVersion.equals(other.engineVersion)
        && logicalType.equals(other.logicalType)
        && upstream.equals(other.upstream)
        && valueStats.equals(other.valueStats);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(engineKind, engineVersion, logicalType, upstream, valueStats);
    result = 31 * result + Arrays.hashCode(engineExpressionKey);
    return result;
  }
}
