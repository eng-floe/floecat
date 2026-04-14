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

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCapabilities;
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsKind;
import ai.floedb.floecat.stats.spi.StatsSamplingSupport;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Baseline reference engine exposing already-persisted stats through the capture SPI.
 *
 * <p>Custom engines should generally use a lower priority value (less than {@code 10_000}) to take
 * precedence over this fallback engine.
 */
@ApplicationScoped
public class PersistedStatsCaptureEngine implements StatsCaptureEngine {

  private static final String ENGINE_ID = "persisted_stats";
  private static final Set<StatsKind> TABLE_STATISTIC_KINDS =
      Set.of(StatsKind.ROW_COUNT, StatsKind.FILE_COUNT, StatsKind.TOTAL_BYTES);
  private static final Set<StatsKind> VALUE_STATISTIC_KINDS =
      Set.of(StatsKind.NULL_COUNT, StatsKind.NDV, StatsKind.MIN_MAX, StatsKind.HISTOGRAM);
  private static final Set<StatsKind> FILE_STATISTIC_KINDS =
      Set.of(
          StatsKind.ROW_COUNT,
          StatsKind.NULL_COUNT,
          StatsKind.NDV,
          StatsKind.MIN_MAX,
          StatsKind.HISTOGRAM);
  private static final StatsCapabilities CAPABILITIES =
      StatsCapabilities.builder()
          .connectors(Set.of()) // empty = all connectors
          .targetTypes(Set.of(StatsTargetType.TABLE, StatsTargetType.COLUMN, StatsTargetType.FILE))
          .statisticKindsByTarget(
              Map.of(
                  StatsTargetType.TABLE, TABLE_STATISTIC_KINDS,
                  StatsTargetType.COLUMN, VALUE_STATISTIC_KINDS,
                  StatsTargetType.FILE, FILE_STATISTIC_KINDS))
          .executionModes(Set.of(StatsExecutionMode.SYNC))
          .samplingSupport(Set.of(StatsSamplingSupport.NONE))
          .snapshotAware(true)
          .build();

  private final StatsStore statsStore;

  @Inject
  public PersistedStatsCaptureEngine(StatsStore statsStore) {
    this.statsStore = statsStore;
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
  public StatsCapabilities capabilities() {
    return CAPABILITIES;
  }

  @Override
  public Optional<StatsCaptureResult> capture(StatsCaptureRequest request) {
    StatsTarget target = request.target();
    return switch (target.getTargetCase()) {
      case TABLE -> captureTable(request);
      case COLUMN -> captureColumn(request, target);
      case FILE -> captureFile(request, target);
      case EXPRESSION -> Optional.empty();
      case TARGET_NOT_SET -> Optional.empty();
    };
  }

  // Returns empty when no table record exists for the normalized target or value type mismatches.
  // Result attributes include source=repository to make provenance explicit in routing traces.
  private Optional<StatsCaptureResult> captureTable(StatsCaptureRequest request) {
    if (!supportsRequestedKinds(request.requestedKinds(), TABLE_STATISTIC_KINDS)) {
      return Optional.empty();
    }
    StatsTarget normalizedTarget = StatsTargetIdentity.tableTarget();
    Optional<TargetStatsRecord> tableRecord =
        statsStore.getTargetStats(request.tableId(), request.snapshotId(), normalizedTarget);
    if (tableRecord.isEmpty() || !tableRecord.get().hasTable()) {
      return Optional.empty();
    }
    return Optional.of(
        StatsCaptureResult.forRecord(ENGINE_ID, tableRecord.get(), Map.of("source", "repository")));
  }

  // Returns empty when repository has no column target record or payload is not scalar.
  // Result attributes include source=repository to signal persisted lookup path.
  private Optional<StatsCaptureResult> captureColumn(
      StatsCaptureRequest request, StatsTarget target) {
    if (!supportsRequestedKinds(request.requestedKinds(), VALUE_STATISTIC_KINDS)) {
      return Optional.empty();
    }
    long columnId = target.getColumn().getColumnId();
    StatsTarget normalizedTarget = StatsTargetIdentity.columnTarget(columnId);
    Optional<TargetStatsRecord> columnRecord =
        statsStore.getTargetStats(request.tableId(), request.snapshotId(), normalizedTarget);
    if (columnRecord.isEmpty() || !columnRecord.get().hasScalar()) {
      return Optional.empty();
    }
    return Optional.of(
        StatsCaptureResult.forRecord(
            ENGINE_ID, columnRecord.get(), Map.of("source", "repository")));
  }

  // Returns empty when repository has no file target record or payload is not file stats.
  // Result attributes include source=repository to signal persisted lookup path.
  private Optional<StatsCaptureResult> captureFile(
      StatsCaptureRequest request, StatsTarget target) {
    if (!supportsRequestedKinds(request.requestedKinds(), FILE_STATISTIC_KINDS)) {
      return Optional.empty();
    }
    StatsTarget normalizedTarget = StatsTargetIdentity.fileTarget(target.getFile().getFilePath());
    Optional<TargetStatsRecord> fileRecord =
        statsStore.getTargetStats(request.tableId(), request.snapshotId(), normalizedTarget);
    if (fileRecord.isEmpty() || !fileRecord.get().hasFile()) {
      return Optional.empty();
    }
    return Optional.of(
        StatsCaptureResult.forRecord(ENGINE_ID, fileRecord.get(), Map.of("source", "repository")));
  }

  private static boolean supportsRequestedKinds(
      Set<StatsKind> requestedKinds, Set<StatsKind> supportedKinds) {
    return requestedKinds.isEmpty() || supportedKinds.containsAll(requestedKinds);
  }
}
