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

package ai.floedb.floecat.metagraph.model;

import java.util.Map;

/**
 * Lightweight statistics summary cached with {@link TableNode}.
 *
 * <p>The full statistics payloads (table/column/file stats) can be large, so this record captures
 * only a bounded subset (row count, data size, NDV sketches) that planners commonly use for
 * pruning. Consumers can always fall back to the statistics RPCs if they need detailed histograms.
 */
public record TableStatsSummary(
    long rowCount, long dataSizeBytes, Map<String, Double> ndvPerColumn) {

  public TableStatsSummary {
    ndvPerColumn = Map.copyOf(ndvPerColumn);
  }
}
