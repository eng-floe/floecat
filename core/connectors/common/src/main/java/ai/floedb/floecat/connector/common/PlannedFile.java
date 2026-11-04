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

package ai.floedb.floecat.connector.common;

import java.util.Map;

/**
 * A planned data file and its statistics. {@code liveRowCount} is the rows its deletes leave, or
 * null when the planner does not know them.
 */
public record PlannedFile<K>(
    String path,
    String format,
    long rowCount,
    long sizeBytes,
    Map<K, Long> rowCounts,
    Map<K, Long> nullCounts,
    Map<K, Long> nanCounts,
    Map<K, Object> lowerBounds,
    Map<K, Object> upperBounds,
    String partitionDataJson,
    int partitionSpecId,
    Long sequenceNumber,
    String contentIdentity,
    Long liveRowCount) {
  public PlannedFile {
    contentIdentity = contentIdentity == null ? "" : contentIdentity.trim();
  }

  /** A planned data file whose live rows are unknown. */
  public PlannedFile(
      String path,
      String format,
      long rowCount,
      long sizeBytes,
      Map<K, Long> rowCounts,
      Map<K, Long> nullCounts,
      Map<K, Long> nanCounts,
      Map<K, Object> lowerBounds,
      Map<K, Object> upperBounds,
      String partitionDataJson,
      int partitionSpecId,
      Long sequenceNumber,
      String contentIdentity) {
    this(
        path,
        format,
        rowCount,
        sizeBytes,
        rowCounts,
        nullCounts,
        nanCounts,
        lowerBounds,
        upperBounds,
        partitionDataJson,
        partitionSpecId,
        sequenceNumber,
        contentIdentity,
        null);
  }

  /** The rows its deletes leave when known, else its rows. */
  public long rowsAfterDeletes() {
    return liveRowCount == null ? rowCount : liveRowCount;
  }
}
