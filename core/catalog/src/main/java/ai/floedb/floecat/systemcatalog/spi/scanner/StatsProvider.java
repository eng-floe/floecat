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

package ai.floedb.floecat.systemcatalog.spi.scanner;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.Optional;

/** Shared provider that surfaces table/column stats to metadata consumers. */
public interface StatsProvider {

  default Optional<TableStatsView> tableStats(ResourceId tableId) {
    return Optional.empty();
  }

  default Optional<ColumnStatsView> columnStats(ResourceId tableId, long columnId) {
    return Optional.empty();
  }

  StatsProvider NONE = new StatsProvider() {};

  interface TableStatsView {
    ResourceId tableId();

    long snapshotId();

    long rowCount();

    long totalSizeBytes();
  }

  interface ColumnStatsView {
    ResourceId tableId();

    long columnId();

    String columnName();

    long valueCount();

    long nullCount();
  }
}
