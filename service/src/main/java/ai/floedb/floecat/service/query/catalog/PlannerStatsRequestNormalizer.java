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

package ai.floedb.floecat.service.query.catalog;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.FetchTableConstraintsRequest;
import ai.floedb.floecat.query.rpc.FetchTargetStatsRequest;
import ai.floedb.floecat.query.rpc.RequestedStat;
import ai.floedb.floecat.query.rpc.StatRole;
import ai.floedb.floecat.query.rpc.TableStatsRequest;
import ai.floedb.floecat.query.rpc.TargetStatsNeed;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

final class PlannerStatsRequestNormalizer {
  private final int maxTables;
  private final int maxTargets;

  PlannerStatsRequestNormalizer(int maxTables, int maxTargets) {
    this.maxTables = Math.max(1, maxTables);
    this.maxTargets = Math.max(1, maxTargets);
  }

  PlannerStatsNormalizedRequest normalizeTargets(
      String correlationId, FetchTargetStatsRequest request) {
    if (request.getTablesCount() == 0) {
      throw GrpcErrors.invalidArgument(
          correlationId, PLANNER_STATS_REQUEST_TABLES_MISSING, Map.of());
    }
    if (request.getTablesCount() > maxTables) {
      throw GrpcErrors.invalidArgument(
          correlationId,
          PLANNER_STATS_REQUEST_TABLES_LIMIT,
          Map.of("max_tables", Integer.toString(maxTables)));
    }

    PlannerStatsServingPolicy servingPolicy =
        PlannerStatsServingPolicy.from(request.getOptions(), maxTargets);
    List<PlannerStatsTableRequest> normalized = new ArrayList<>(request.getTablesCount());
    long totalTargets = 0;
    long omittedTargets = 0;
    int sketchTargets = 0;

    for (int tableIndex = 0; tableIndex < request.getTablesCount(); tableIndex++) {
      TableStatsRequest table = request.getTables(tableIndex);
      if (!table.hasTableId()) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_TABLE_ID_MISSING,
            Map.of("table_index", Integer.toString(tableIndex)));
      }
      if (table.getTargetsCount() == 0) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_TARGETS_MISSING,
            Map.of("table_id", table.getTableId().getId()));
      }

      List<PlannerStatsTargetNeed> asTargets = new ArrayList<>(table.getTargetsCount());
      for (TargetStatsNeed need : table.getTargetsList()) {
        StatsTarget target = need.getTarget();
        switch (target.getTargetCase()) {
          case COLUMN -> {
            if (target.getColumn().getColumnId() <= 0) {
              throw GrpcErrors.invalidArgument(
                  correlationId, PLANNER_STATS_REQUEST_TARGET_INVALID, Map.of());
            }
          }
          case EXPRESSION -> {
            /* Engine-owned expression key: accepted as-is after identity validation. */
          }
          default ->
              throw GrpcErrors.invalidArgument(
                  correlationId, PLANNER_STATS_REQUEST_TARGET_INVALID, Map.of());
        }
        int priority = need.getPriority() > 0 ? need.getPriority() : Integer.MAX_VALUE;
        List<PlannerStatsStatRequest> requestedStats = normalizeRequestedStats(correlationId, need);
        asTargets.add(
            new PlannerStatsTargetNeed(
                target,
                requestedStats,
                validatedStorageId(correlationId, table.getTableId().getId(), target),
                priority));
      }

      asTargets.sort(Comparator.comparingInt(PlannerStatsTargetNeed::priority));
      List<PlannerStatsTargetNeed> dedupedRaw =
          dedupeTargets(correlationId, table.getTableId().getId(), asTargets);
      if (dedupedRaw.isEmpty()) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_TARGETS_MISSING,
            Map.of("table_id", table.getTableId().getId()));
      }

      List<PlannerStatsTargetNeed> deduped = new ArrayList<>(dedupedRaw.size());
      for (PlannerStatsTargetNeed need : dedupedRaw) {
        boolean hasSketchRequest = need.requestsAnySketchPayload();
        boolean omitSketchPayloads = false;
        if (hasSketchRequest) {
          if (sketchTargets < servingPolicy.maxSketchTargets()) {
            sketchTargets++;
          } else {
            omitSketchPayloads = true;
          }
        }
        deduped.add(
            new PlannerStatsTargetNeed(
                need.target(),
                applyRequestCaps(need.requestedStats(), omitSketchPayloads),
                need.storageId(),
                need.priority()));
      }

      long remaining = servingPolicy.maxScalarTargets() - totalTargets;
      List<PlannerStatsTargetNeed> omitted;
      if (remaining <= 0) {
        omitted = List.copyOf(deduped);
        omittedTargets += deduped.size();
        normalized.add(
            new PlannerStatsTableRequest(
                table.getTableId(), List.of(), omitted, snapshotOverride(table)));
        continue;
      }
      if (deduped.size() > remaining) {
        omitted = List.copyOf(deduped.subList((int) remaining, deduped.size()));
        omittedTargets += omitted.size();
        deduped = deduped.subList(0, (int) remaining);
      } else {
        omitted = List.of();
      }
      normalized.add(
          new PlannerStatsTableRequest(
              table.getTableId(), List.copyOf(deduped), omitted, snapshotOverride(table)));
      totalTargets += deduped.size();
    }

    return new PlannerStatsNormalizedRequest(List.copyOf(normalized), totalTargets, omittedTargets);
  }

  private static String validatedStorageId(
      String correlationId, String tableId, StatsTarget target) {
    try {
      return StatsTargetIdentity.storageId(target);
    } catch (IllegalArgumentException e) {
      throw GrpcErrors.invalidArgument(
          correlationId,
          PLANNER_STATS_REQUEST_TARGET_INVALID,
          Map.of("table_id", tableId, "detail", e.getMessage()));
    }
  }

  List<ResourceId> normalizeConstraints(
      String correlationId, FetchTableConstraintsRequest request) {
    if (request.getTableIdsCount() == 0) {
      throw GrpcErrors.invalidArgument(
          correlationId, PLANNER_STATS_REQUEST_TABLES_MISSING, Map.of());
    }

    Map<String, ResourceId> deduped = new LinkedHashMap<>(request.getTableIdsCount());
    for (int tableIndex = 0; tableIndex < request.getTableIdsCount(); tableIndex++) {
      ResourceId tableId = request.getTableIds(tableIndex);
      if (tableId.getId().isBlank()) {
        throw GrpcErrors.invalidArgument(
            correlationId,
            PLANNER_STATS_REQUEST_TABLE_ID_MISSING,
            Map.of("table_index", Integer.toString(tableIndex)));
      }
      deduped.putIfAbsent(RequestScopeConstraintPruner.relationKey(tableId), tableId);
    }
    if (deduped.size() > maxTables) {
      throw GrpcErrors.invalidArgument(
          correlationId,
          PLANNER_STATS_REQUEST_TABLES_LIMIT,
          Map.of("max_tables", Integer.toString(maxTables)));
    }
    return List.copyOf(deduped.values());
  }

  private static OptionalLong snapshotOverride(TableStatsRequest table) {
    return table.hasSnapshotId() ? OptionalLong.of(table.getSnapshotId()) : OptionalLong.empty();
  }

  private static List<PlannerStatsTargetNeed> dedupeTargets(
      String correlationId, String tableId, List<PlannerStatsTargetNeed> targets) {
    Map<String, PlannerStatsTargetNeed> deduped = new LinkedHashMap<>(targets.size());
    for (PlannerStatsTargetNeed need : targets) {
      StatsTarget target = need.target();
      if (target == null || target.getTargetCase() == StatsTarget.TargetCase.TARGET_NOT_SET) {
        throw GrpcErrors.invalidArgument(
            correlationId, PLANNER_STATS_REQUEST_TARGET_INVALID, Map.of());
      }
      if (target.hasColumn() && target.getColumn().getColumnId() <= 0) {
        throw GrpcErrors.invalidArgument(
            correlationId, PLANNER_STATS_REQUEST_TARGET_INVALID, Map.of());
      }
      deduped.putIfAbsent(need.storageId(), need);
    }
    return new ArrayList<>(deduped.values());
  }

  private static List<PlannerStatsStatRequest> normalizeRequestedStats(
      String correlationId, TargetStatsNeed need) {
    List<RequestedStat> input =
        need.getRequestedStatsCount() == 0
            ? List.of(RequestedStat.newBuilder().setRole(StatRole.STAT_ROLE_SCALAR).build())
            : need.getRequestedStatsList();
    Map<String, PlannerStatsStatRequest> deduped = new LinkedHashMap<>(input.size());
    for (RequestedStat requested : input) {
      StatRole role = requested.getRole();
      if (role == StatRole.STAT_ROLE_UNSPECIFIED || role == StatRole.UNRECOGNIZED) {
        throw GrpcErrors.invalidArgument(
            correlationId, PLANNER_STATS_REQUEST_TARGET_INVALID, Map.of());
      }
      String sketchType = requested.getSketchType();
      if (role == StatRole.STAT_ROLE_SCALAR && !sketchType.isBlank()) {
        throw GrpcErrors.invalidArgument(
            correlationId, PLANNER_STATS_REQUEST_TARGET_INVALID, Map.of());
      }
      if (isSketchRole(role) && sketchType.isBlank()) {
        throw GrpcErrors.invalidArgument(
            correlationId, PLANNER_STATS_REQUEST_TARGET_INVALID, Map.of());
      }
      int priority = requested.getPriority() > 0 ? requested.getPriority() : Integer.MAX_VALUE;
      PlannerStatsStatRequest stat =
          new PlannerStatsStatRequest(
              role, sketchType, priority, requested.getMaxBytes(), false, "");
      PlannerStatsStatRequest existing = deduped.get(stat.identityKey());
      if (existing == null || stat.priority() < existing.priority()) {
        deduped.put(stat.identityKey(), stat);
      }
    }
    List<PlannerStatsStatRequest> out = new ArrayList<>(deduped.values());
    out.sort(
        Comparator.comparingInt(PlannerStatsStatRequest::priority)
            .thenComparingInt(s -> s.role().getNumber())
            .thenComparing(PlannerStatsStatRequest::sketchType));
    return List.copyOf(out);
  }

  private static List<PlannerStatsStatRequest> applyRequestCaps(
      List<PlannerStatsStatRequest> requestedStats, boolean omitSketchPayloads) {
    if (!omitSketchPayloads) {
      return requestedStats;
    }
    List<PlannerStatsStatRequest> capped = new ArrayList<>(requestedStats.size());
    for (PlannerStatsStatRequest stat : requestedStats) {
      boolean omitted = stat.requestsSketchPayload();
      capped.add(omitted ? stat.omittedByPolicy("max_sketch_targets") : stat);
    }
    return List.copyOf(capped);
  }

  static boolean isSketchRole(StatRole role) {
    return switch (role) {
      case STAT_ROLE_NDV, STAT_ROLE_MCV, STAT_ROLE_QUANTILES, STAT_ROLE_TUPLE_NDV -> true;
      default -> false;
    };
  }
}
