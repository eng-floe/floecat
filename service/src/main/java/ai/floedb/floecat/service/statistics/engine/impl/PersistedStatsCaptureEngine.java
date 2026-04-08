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

package ai.floedb.floecat.service.statistics.engine.impl;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsEngineCapabilities;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsSamplingSupport;
import ai.floedb.floecat.stats.spi.StatsStatisticKind;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Baseline engine exposing already-persisted stats through the new capture SPI. */
@ApplicationScoped
public class PersistedStatsCaptureEngine implements StatsCaptureEngine {

  private static final String ENGINE_ID = "persisted_stats";
  private static final Set<StatsStatisticKind> TABLE_STATISTIC_KINDS =
      Set.of(
          StatsStatisticKind.ROW_COUNT,
          StatsStatisticKind.FILE_COUNT,
          StatsStatisticKind.TOTAL_BYTES);
  private static final Set<StatsStatisticKind> VALUE_STATISTIC_KINDS =
      Set.of(
          StatsStatisticKind.NULL_COUNT,
          StatsStatisticKind.NDV,
          StatsStatisticKind.MIN_MAX,
          StatsStatisticKind.HISTOGRAM);
  private static final StatsEngineCapabilities CAPABILITIES =
      StatsEngineCapabilities.builder()
          .connectors(Set.of()) // empty = all connectors
          .targetTypes(Set.of(StatsTargetType.TABLE, StatsTargetType.COLUMN, StatsTargetType.FILE))
          .statisticKindsByTarget(
              Map.of(
                  StatsTargetType.TABLE, TABLE_STATISTIC_KINDS,
                  StatsTargetType.COLUMN, VALUE_STATISTIC_KINDS,
                  StatsTargetType.FILE, VALUE_STATISTIC_KINDS))
          .executionModes(Set.of(StatsExecutionMode.SYNC, StatsExecutionMode.ASYNC))
          .samplingSupport(Set.of(StatsSamplingSupport.NONE))
          .snapshotAware(true)
          .build();

  private final StatsRepository statsRepository;

  @Inject
  public PersistedStatsCaptureEngine(StatsRepository statsRepository) {
    this.statsRepository = statsRepository;
  }

  @Override
  public String id() {
    return ENGINE_ID;
  }

  @Override
  public int priority() {
    // Baseline fallback engine: keep lower precedence than specialized providers.
    return 10_000;
  }

  @Override
  public StatsEngineCapabilities capabilities() {
    return CAPABILITIES;
  }

  @Override
  public Optional<StatsCaptureResult> capture(StatsCaptureRequest request) {
    StatsTarget target = request.target();
    return switch (target.getTargetCase()) {
      case TABLE -> captureTable(request, target);
      case COLUMN -> captureColumn(request, target);
      case FILE -> captureFile(request, target);
      case EXPRESSION, TARGET_NOT_SET -> Optional.empty();
    };
  }

  private Optional<StatsCaptureResult> captureTable(
      StatsCaptureRequest request, StatsTarget target) {
    if (!supportsRequestedKinds(request.requestedKinds(), TABLE_STATISTIC_KINDS)) {
      return Optional.empty();
    }
    Optional<TableStats> table =
        statsRepository.getTableStats(request.tableId(), request.snapshotId());
    if (table.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        StatsCaptureResult.forTable(
            ENGINE_ID, target, table.get(), Map.of("source", "repository")));
  }

  private Optional<StatsCaptureResult> captureColumn(
      StatsCaptureRequest request, StatsTarget target) {
    if (!supportsRequestedKinds(request.requestedKinds(), VALUE_STATISTIC_KINDS)) {
      return Optional.empty();
    }
    long columnId = target.getColumn().getColumnId();
    Optional<ColumnStats> column =
        statsRepository.getColumnStats(request.tableId(), request.snapshotId(), columnId);
    if (column.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        StatsCaptureResult.forColumn(
            ENGINE_ID, target, column.get(), Map.of("source", "repository")));
  }

  private Optional<StatsCaptureResult> captureFile(
      StatsCaptureRequest request, StatsTarget target) {
    if (!supportsRequestedKinds(request.requestedKinds(), VALUE_STATISTIC_KINDS)) {
      return Optional.empty();
    }
    String filePath = StatsTargetIdentity.filePath(target.getFile().getFilePath());
    StatsTarget normalizedTarget = StatsTargetIdentity.fileTarget(filePath);
    Optional<FileColumnStats> file =
        statsRepository.getFileColumnStats(request.tableId(), request.snapshotId(), filePath);
    if (file.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        StatsCaptureResult.forFile(
            ENGINE_ID, normalizedTarget, file.get(), Map.of("source", "repository")));
  }

  private static boolean supportsRequestedKinds(
      Set<StatsStatisticKind> requestedKinds, Set<StatsStatisticKind> supportedKinds) {
    return requestedKinds.isEmpty() || supportedKinds.containsAll(requestedKinds);
  }
}
