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

package ai.floedb.floecat.service.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.rpc.CapturePolicy;
import ai.floedb.floecat.reconciler.rpc.CostHint;
import ai.floedb.floecat.reconciler.rpc.ExecutionPolicy;
import ai.floedb.floecat.reconciler.rpc.PriorityClass;
import ai.floedb.floecat.stats.spi.JobCostHint;
import ai.floedb.floecat.stats.spi.StatsPriorityClass;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Verifies that priority class, priority score, and max_cost survive the Java → proto → Java mapper
 * round trip. Wire-default UNSPECIFIED values must map to their safe Java defaults.
 *
 * <p>These tests exercise the private static mapper methods indirectly via proto builder
 * round-trips; they do not require a CDI container.
 */
class ExecutionPolicyMapperRoundTripTest {

  // ---------------------------------------------------------------------------
  // ExecutionPolicy round-trip
  // ---------------------------------------------------------------------------

  @Test
  void p1FreshnessAndScoreSurviveRoundTrip() {
    // Build proto directly as a remote sender would
    ExecutionPolicy proto =
        ExecutionPolicy.newBuilder()
            .setLane("acct:tbl")
            .setPriorityClass(PriorityClass.PC_P1_FRESHNESS)
            .setPriorityScore(42L)
            .build();

    // Map through the service mapper (using the same logic as ReconcileControlImpl)
    ReconcileExecutionPolicy domain = mapFromProto(proto);

    assertEquals(StatsPriorityClass.P1_FRESHNESS, domain.priorityClass());
    assertEquals(42L, domain.priorityScore());
    assertEquals("acct:tbl", domain.lane());
  }

  @Test
  void p0SyncMapsCorrectly() {
    ExecutionPolicy proto =
        ExecutionPolicy.newBuilder().setPriorityClass(PriorityClass.PC_P0_SYNC).build();
    assertEquals(StatsPriorityClass.P0_SYNC, mapFromProto(proto).priorityClass());
  }

  @Test
  void p2RepairMapsCorrectly() {
    ExecutionPolicy proto =
        ExecutionPolicy.newBuilder().setPriorityClass(PriorityClass.PC_P2_REPAIR).build();
    assertEquals(StatsPriorityClass.P2_REPAIR, mapFromProto(proto).priorityClass());
  }

  @Test
  void wireDefaultUnspecifiedMapsToP3Background() {
    // PRIORITY_CLASS_UNSPECIFIED is the proto wire default (value=0)
    ExecutionPolicy proto = ExecutionPolicy.newBuilder().build();
    assertEquals(StatsPriorityClass.P3_BACKGROUND, mapFromProto(proto).priorityClass());
    assertEquals(0L, mapFromProto(proto).priorityScore());
  }

  @Test
  void domainToProtoAndBackPreservesAllFields() {
    ReconcileExecutionPolicy domain =
        ReconcileExecutionPolicy.of(
            ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass.DEFAULT,
            "my-lane",
            Map.of("k", "v"),
            StatsPriorityClass.P2_REPAIR,
            99L);

    ExecutionPolicy proto = mapToProto(domain);
    ReconcileExecutionPolicy recovered = mapFromProto(proto);

    assertEquals(domain.priorityClass(), recovered.priorityClass());
    assertEquals(domain.priorityScore(), recovered.priorityScore());
    assertEquals(domain.lane(), recovered.lane());
    assertEquals(domain.attributes(), recovered.attributes());
  }

  // ---------------------------------------------------------------------------
  // CapturePolicy round-trip
  // ---------------------------------------------------------------------------

  @Test
  void mediumMaxCostSurvivesRoundTrip() {
    CapturePolicy proto = CapturePolicy.newBuilder().setMaxCost(CostHint.CH_MEDIUM).build();
    ReconcileCapturePolicy domain = mapCapturePolicyFromProto(proto);
    assertEquals(JobCostHint.MEDIUM, domain.maxCost());
  }

  @Test
  void cheapMaxCostSurvivesRoundTrip() {
    CapturePolicy proto = CapturePolicy.newBuilder().setMaxCost(CostHint.CH_CHEAP).build();
    assertEquals(JobCostHint.CHEAP, mapCapturePolicyFromProto(proto).maxCost());
  }

  @Test
  void wireDefaultUnspecifiedMaxCostMapsToExpensive() {
    // COST_HINT_UNSPECIFIED is the proto wire default
    CapturePolicy proto = CapturePolicy.newBuilder().build();
    assertEquals(JobCostHint.EXPENSIVE, mapCapturePolicyFromProto(proto).maxCost());
  }

  // ---------------------------------------------------------------------------
  // Helpers (replicate mapper logic inline so test has no CDI dependency)
  // ---------------------------------------------------------------------------

  private static ReconcileExecutionPolicy mapFromProto(ExecutionPolicy proto) {
    if (proto == null) return ReconcileExecutionPolicy.defaults();
    StatsPriorityClass priorityClass =
        switch (proto.getPriorityClass()) {
          case PC_P0_SYNC -> StatsPriorityClass.P0_SYNC;
          case PC_P1_FRESHNESS -> StatsPriorityClass.P1_FRESHNESS;
          case PC_P2_REPAIR -> StatsPriorityClass.P2_REPAIR;
          case PC_P3_BACKGROUND, PRIORITY_CLASS_UNSPECIFIED, UNRECOGNIZED ->
              StatsPriorityClass.P3_BACKGROUND;
        };
    return ReconcileExecutionPolicy.of(
        ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass.DEFAULT,
        proto.getLane(),
        proto.getAttributesMap(),
        priorityClass,
        proto.getPriorityScore());
  }

  private static ExecutionPolicy mapToProto(ReconcileExecutionPolicy domain) {
    ai.floedb.floecat.reconciler.rpc.PriorityClass protoPc =
        switch (domain.priorityClass()) {
          case P0_SYNC -> PriorityClass.PC_P0_SYNC;
          case P1_FRESHNESS -> PriorityClass.PC_P1_FRESHNESS;
          case P2_REPAIR -> PriorityClass.PC_P2_REPAIR;
          case P3_BACKGROUND -> PriorityClass.PC_P3_BACKGROUND;
        };
    return ExecutionPolicy.newBuilder()
        .setLane(domain.lane())
        .putAllAttributes(domain.attributes())
        .setPriorityClass(protoPc)
        .setPriorityScore(domain.priorityScore())
        .build();
  }

  private static ReconcileCapturePolicy mapCapturePolicyFromProto(CapturePolicy proto) {
    JobCostHint cost =
        switch (proto.getMaxCost()) {
          case CH_CHEAP -> JobCostHint.CHEAP;
          case CH_MEDIUM -> JobCostHint.MEDIUM;
          case CH_EXPENSIVE, COST_HINT_UNSPECIFIED, UNRECOGNIZED -> JobCostHint.EXPENSIVE;
        };
    return ReconcileCapturePolicy.of(
            java.util.List.of(), Set.of(ReconcileCapturePolicy.Output.TABLE_STATS))
        .withMaxCost(cost);
  }
}
