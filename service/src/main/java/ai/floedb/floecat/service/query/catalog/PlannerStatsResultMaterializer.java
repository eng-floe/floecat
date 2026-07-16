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

import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.SketchPayload;
import ai.floedb.floecat.catalog.rpc.SketchRole;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.query.rpc.ReturnedStat;
import ai.floedb.floecat.query.rpc.StatRole;
import ai.floedb.floecat.query.rpc.StatsResultDegradation;
import ai.floedb.floecat.query.rpc.StatsResultStatus;
import ai.floedb.floecat.query.rpc.TargetStatsResult;
import java.util.ArrayList;
import java.util.List;

final class PlannerStatsResultMaterializer {
  private PlannerStatsResultMaterializer() {}

  record Materialized(
      TargetStatsRecord record, List<ReturnedStat> returnedStats, StatsResultStatus status) {}

  static Materialized materialize(
      PlannerStatsTargetNeed need, PlannerTargetStatsLookupResult lookupResult) {
    TargetStatsRecord original = lookupResult.stats().orElseThrow();
    TargetStatsRecord filtered =
        materializeRecordForRequestedStats(original, need.requestedStats());
    List<ReturnedStat> returnedStats = returnedStatsFor(need, original, filtered);
    return new Materialized(
        filtered, returnedStats, resultStatusForHit(lookupResult, returnedStats));
  }

  static TargetStatsResult buildFoundResult(Materialized materialized) {
    TargetStatsRecord stats = materialized.record();
    List<ReturnedStat> returnedStats = materialized.returnedStats();
    return TargetStatsResult.newBuilder()
        .setTableId(stats.getTableId())
        .setSnapshotId(stats.getSnapshotId())
        .setTarget(stats.getTarget())
        .setStatus(materialized.status())
        .addAllReturnedStats(returnedStats)
        .addAllDegradations(
            returnedStats.stream()
                .flatMap(s -> s.getDegradationsList().stream())
                .distinct()
                .toList())
        .setStats(stats)
        .build();
  }

  private static StatsResultStatus resultStatusForHit(
      PlannerTargetStatsLookupResult lookupResult, List<ReturnedStat> returnedStats) {
    if (lookupResult.stale()) {
      return StatsResultStatus.STATS_RESULT_HIT_STALE;
    }
    boolean complete =
        returnedStats.stream()
            .allMatch(s -> s.getStatus() == StatsResultStatus.STATS_RESULT_HIT_COMPLETE);
    return complete
        ? StatsResultStatus.STATS_RESULT_HIT_COMPLETE
        : StatsResultStatus.STATS_RESULT_HIT_PARTIAL;
  }

  private static TargetStatsRecord materializeRecordForRequestedStats(
      TargetStatsRecord record, List<PlannerStatsStatRequest> requestedStats) {
    if (!record.hasScalar()) {
      return record;
    }
    boolean returnScalar =
        requestedStats.stream().anyMatch(PlannerStatsResultMaterializer::isScalarRequest);
    ScalarStats scalar = record.getScalar();
    if (returnScalar
        && scalar.getSketchesCount() == 0
        && (!scalar.hasNdv() || scalar.getNdv().getSketchesCount() == 0)) {
      return record;
    }

    ScalarStats.Builder scalarBuilder =
        returnScalar ? scalar.toBuilder().clearSketches() : ScalarStats.newBuilder();
    for (SketchPayload sketch : scalar.getSketchesList()) {
      if (shouldReturnSketch(sketch, requestedStats)) {
        scalarBuilder.addSketches(sketch);
      }
    }
    if (scalar.hasNdv()) {
      Ndv.Builder ndvBuilder =
          returnScalar ? scalar.getNdv().toBuilder().clearSketches() : Ndv.newBuilder();
      for (SketchPayload sketch : scalar.getNdv().getSketchesList()) {
        if (shouldReturnSketch(sketch, requestedStats)) {
          ndvBuilder.addSketches(sketch);
        }
      }
      if (returnScalar || ndvBuilder.getSketchesCount() > 0) {
        scalarBuilder.setNdv(ndvBuilder.build());
      }
    }
    if (!returnScalar
        && scalarBuilder.getSketchesCount() == 0
        && (!scalarBuilder.hasNdv() || scalarBuilder.getNdv().getSketchesCount() == 0)) {
      return record.toBuilder().clearScalar().build();
    }
    return record.toBuilder().setScalar(scalarBuilder).build();
  }

  private static boolean isScalarRequest(PlannerStatsStatRequest request) {
    return !request.omittedByPolicy() && request.role() == StatRole.STAT_ROLE_SCALAR;
  }

  private static boolean shouldReturnSketch(
      SketchPayload sketch, List<PlannerStatsStatRequest> requestedStats) {
    for (PlannerStatsStatRequest requested : requestedStats) {
      if (requested.omittedByPolicy()
          || !requested.requestsSketchPayload()
          || !requested.sketchType().equals(sketch.getSketchType())) {
        continue;
      }
      SketchRole requestedRole = toSketchRole(requested.role());
      if (requestedRole == sketch.getRole()
          && (requested.maxBytes() <= 0 || sketch.getSerializedSize() <= requested.maxBytes())) {
        return true;
      }
    }
    return false;
  }

  private static List<ReturnedStat> returnedStatsFor(
      PlannerStatsTargetNeed need, TargetStatsRecord original, TargetStatsRecord materialized) {
    List<ReturnedStat> out = new ArrayList<>(need.requestedStats().size());
    for (PlannerStatsStatRequest requested : need.requestedStats()) {
      out.add(returnedStatFor(requested, original, materialized));
    }
    return List.copyOf(out);
  }

  private static ReturnedStat returnedStatFor(
      PlannerStatsStatRequest requested,
      TargetStatsRecord original,
      TargetStatsRecord materialized) {
    ReturnedStat.Builder out =
        ReturnedStat.newBuilder().setRole(requested.role()).setSketchType(requested.sketchType());
    if (requested.omittedByPolicy()) {
      return out.setStatus(StatsResultStatus.STATS_RESULT_OMITTED_BY_BUDGET)
          .setReason(requested.omitReason())
          .addDegradations(
              StatsResultDegradation.STATS_DEGRADATION_REQUESTED_STAT_OMITTED_BY_BUDGET)
          .build();
    }
    if (requested.role() == StatRole.STAT_ROLE_SCALAR) {
      if (original.hasScalar()) {
        return out.setStatus(StatsResultStatus.STATS_RESULT_HIT_COMPLETE)
            .setBytes(materialized.hasScalar() ? materialized.getScalar().getSerializedSize() : 0)
            .build();
      }
      return out.setStatus(StatsResultStatus.STATS_RESULT_NOT_FOUND)
          .setReason("scalar_stats_missing")
          .addDegradations(StatsResultDegradation.STATS_DEGRADATION_REQUESTED_STAT_MISSING)
          .build();
    }
    if (!requested.requestsSketchPayload()) {
      return out.setStatus(StatsResultStatus.STATS_RESULT_ERROR)
          .setReason("unsupported_stat_role")
          .addDegradations(StatsResultDegradation.STATS_DEGRADATION_UNSUPPORTED_STAT_ROLE)
          .build();
    }
    SketchPayload originalSketch = findMatchingSketch(original, requested);
    if (originalSketch == null) {
      return out.setStatus(StatsResultStatus.STATS_RESULT_NOT_FOUND)
          .setReason("requested_sketch_missing")
          .addDegradations(StatsResultDegradation.STATS_DEGRADATION_REQUESTED_STAT_MISSING)
          .build();
    }
    if (requested.maxBytes() > 0 && originalSketch.getSerializedSize() > requested.maxBytes()) {
      return out.setStatus(StatsResultStatus.STATS_RESULT_OMITTED_BY_BUDGET)
          .setReason("requested_stat_max_bytes")
          .setBytes(originalSketch.getSerializedSize())
          .addDegradations(
              StatsResultDegradation.STATS_DEGRADATION_REQUESTED_STAT_OMITTED_BY_BUDGET)
          .build();
    }
    SketchPayload returnedSketch = findMatchingSketch(materialized, requested);
    if (returnedSketch == null) {
      return out.setStatus(StatsResultStatus.STATS_RESULT_OMITTED_BY_BUDGET)
          .setReason("response_budget_or_target_cap")
          .setBytes(originalSketch.getSerializedSize())
          .addDegradations(
              StatsResultDegradation.STATS_DEGRADATION_REQUESTED_STAT_OMITTED_BY_BUDGET)
          .build();
    }
    return out.setStatus(StatsResultStatus.STATS_RESULT_HIT_COMPLETE)
        .setBytes(returnedSketch.getSerializedSize())
        .build();
  }

  /**
   * True when {@code record} can serve every stat {@code need} requests: the scalar header when
   * {@code STAT_ROLE_SCALAR} is requested, and a payload matching each requested sketch role and
   * type.
   *
   * <p>This is the planner-aware completeness rule behind generation fallback: a record that merely
   * EXISTS in the pinned generation is not a complete hit — a scalar-only record must not satisfy a
   * quantile need, or resolution would stop at a record the planner can only consume downgraded.
   * Lives next to {@link #findMatchingSketch} so resolution and serving judge capability with the
   * same rule and can never disagree.
   *
   * <p>Presence-only: a sketch that exists but exceeds the request's byte budget still satisfies —
   * budget omission is a serving-policy decision, not a data gap another generation could fill.
   */
  static boolean satisfiesNeed(TargetStatsRecord record, PlannerStatsTargetNeed need) {
    for (PlannerStatsStatRequest requested : need.requestedStats()) {
      if (requested.omittedByPolicy()) {
        // Served as OMITTED_BY_BUDGET regardless of what the record carries.
        continue;
      }
      if (requested.role() == StatRole.STAT_ROLE_SCALAR) {
        if (!record.hasScalar()) {
          return false;
        }
        continue;
      }
      if (!requested.requestsSketchPayload()) {
        // Unsupported role: served as ERROR; no generation can improve it.
        continue;
      }
      if (findMatchingSketch(record, requested) == null) {
        return false;
      }
    }
    return true;
  }

  private static SketchPayload findMatchingSketch(
      TargetStatsRecord record, PlannerStatsStatRequest request) {
    if (!record.hasScalar()) {
      return null;
    }
    ScalarStats scalar = record.getScalar();
    SketchRole role = toSketchRole(request.role());
    for (SketchPayload sketch : scalar.getSketchesList()) {
      if (sketch.getRole() == role && sketch.getSketchType().equals(request.sketchType())) {
        return sketch;
      }
    }
    if (scalar.hasNdv()) {
      for (SketchPayload sketch : scalar.getNdv().getSketchesList()) {
        if (sketch.getRole() == role && sketch.getSketchType().equals(request.sketchType())) {
          return sketch;
        }
      }
    }
    return null;
  }

  private static SketchRole toSketchRole(StatRole role) {
    return switch (role) {
      case STAT_ROLE_NDV -> SketchRole.SKETCH_ROLE_NDV;
      case STAT_ROLE_MCV -> SketchRole.SKETCH_ROLE_MCV;
      case STAT_ROLE_QUANTILES -> SketchRole.SKETCH_ROLE_QUANTILES;
      case STAT_ROLE_TUPLE_NDV -> SketchRole.SKETCH_ROLE_TUPLE_NDV;
      default -> SketchRole.SKETCH_ROLE_UNSPECIFIED;
    };
  }
}
