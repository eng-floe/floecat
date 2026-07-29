/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package ai.floedb.floecat.reconciler.jobs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ReconcileSnapshotContentStateTest {
  @Test
  void coverageUsesSetSemanticsForExactSubsetAndSupersetRequests() {
    List<String> one =
        ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(policy("a")));
    List<String> five =
        ReconcileSnapshotContentState.coverage(
            CaptureMode.CAPTURE_ONLY, scope(policy("a", "b", "c", "d", "e")));
    List<String> nine =
        ReconcileSnapshotContentState.coverage(
            CaptureMode.CAPTURE_ONLY, scope(policy("a", "b", "c", "d", "e", "f", "g", "h", "i")));

    assertThat(five).containsAll(one);
    assertThat(nine).containsAll(five);
    assertThat(ReconcileSnapshotContentState.missingCoverage(nine, five)).hasSize(4);
    assertThat(ReconcileSnapshotContentState.missingCoverage(five, nine)).isEmpty();
    assertThat(ReconcileSnapshotContentState.unionCoverage(one, five))
        .containsExactlyElementsOf(five);
  }

  @Test
  void outputAndPolicyPropertiesArePartOfCoverageSemantics() {
    ReconcileCapturePolicy stats =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("a", true, false)),
            Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            32,
            Map.of("sketch", "v1"));
    ReconcileCapturePolicy changedProperty =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("a", true, false)),
            Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            32,
            Map.of("sketch", "v2"));
    ReconcileCapturePolicy index =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("a", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));

    List<String> existing =
        ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(stats));
    assertThat(
            ReconcileSnapshotContentState.missingCoverage(
                ReconcileSnapshotContentState.coverage(
                    CaptureMode.CAPTURE_ONLY, scope(changedProperty)),
                existing))
        .isNotEmpty();
    assertThat(
            ReconcileSnapshotContentState.missingCoverage(
                ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(index)),
                existing))
        .isNotEmpty();
  }

  @Test
  void statsCoverageFromIndexCaptureSatisfiesStatsOnlyRequest() {
    ReconcileCapturePolicy materialized =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("a", true, true)),
            Set.of(
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileCapturePolicy requested =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("a", true, false)),
            Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS));

    List<String> materializedCoverage =
        ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(materialized));
    List<String> requestedCoverage =
        ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(requested));

    assertThat(
            ReconcileSnapshotContentState.missingCoverage(requestedCoverage, materializedCoverage))
        .isEmpty();
  }

  @Test
  void narrowedScopePreservesRequestedColumnsForRecapture() {
    ReconcileScope requested = scope(policy("a", "b", "c"));
    List<String> all = ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, requested);
    List<String> alreadyMaterialized =
        ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(policy("a", "b")));
    List<String> missing = ReconcileSnapshotContentState.missingCoverage(all, alreadyMaterialized);

    ReconcileScope narrowed = ReconcileSnapshotContentState.narrowScope(requested, missing);

    assertThat(narrowed.capturePolicy().columns())
        .extracting(ReconcileCapturePolicy.Column::selector)
        .containsExactly("a", "b", "c");
  }

  @Test
  void canonicalFingerprintDoesNotDependOnMapOrSetIterationOrder() {
    String left =
        ReconcileSnapshotContentState.fingerprint(
            Map.of("properties", Map.of("b", "2", "a", "1"), "outputs", Set.of("x", "y")));
    String right =
        ReconcileSnapshotContentState.fingerprint(
            Map.of("outputs", Set.of("y", "x"), "properties", Map.of("a", "1", "b", "2")));

    assertThat(left).isEqualTo(right);
  }

  @Test
  void materializedColumnTargetsDoNotCrossProductWithPolicyColumns() {
    ReconcileCapturePolicy policy = policy("101", "202");
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            "table-1",
            List.of(
                new ReconcileScope.ScopedCaptureRequest("table-1", 7L, "column:101", List.of()),
                new ReconcileScope.ScopedCaptureRequest("table-1", 7L, "column:202", List.of())),
            policy);

    List<String> coverage = ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope);

    assertThat(coverage).hasSize(2);
    assertThat(coverage).anyMatch(atom -> atom.contains("10:column:101|0:|"));
    assertThat(coverage).anyMatch(atom -> atom.contains("10:column:202|0:|"));
  }

  @Test
  void narrowedProductionScopePreservesAllColumnTargetsForRecapture() {
    ReconcileCapturePolicy policy = policy("101", "202");
    ReconcileScope requested =
        ReconcileScope.of(
            List.of(),
            "table-1",
            List.of(
                new ReconcileScope.ScopedCaptureRequest("table-1", 7L, "column:101", List.of()),
                new ReconcileScope.ScopedCaptureRequest("table-1", 7L, "column:202", List.of())),
            policy);
    ReconcileScope firstOnly =
        ReconcileScope.of(
            List.of(),
            "table-1",
            List.of(
                new ReconcileScope.ScopedCaptureRequest("table-1", 7L, "column:101", List.of())),
            policy);
    List<String> missing =
        ReconcileSnapshotContentState.missingCoverage(
            ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, requested),
            ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, firstOnly));

    ReconcileScope narrowed = ReconcileSnapshotContentState.narrowScope(requested, missing);

    assertThat(narrowed.destinationCaptureRequests())
        .extracting(ReconcileScope.ScopedCaptureRequest::targetSpec)
        .containsExactly("column:101", "column:202");
    assertThat(narrowed.destinationCaptureRequests().getFirst().columnSelectors()).isEmpty();
    assertThat(narrowed.capturePolicy().columns())
        .extracting(ReconcileCapturePolicy.Column::selector)
        .containsExactly("101", "202");
  }

  @Test
  void narrowedScopePreservesColumnBreadthWithoutRecapturingCompleteOutputKinds() {
    ReconcileCapturePolicy requestedPolicy =
        ReconcileCapturePolicy.of(
            List.of(
                new ReconcileCapturePolicy.Column("a", true, true),
                new ReconcileCapturePolicy.Column("b", true, true)),
            Set.of(
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileCapturePolicy materializedPolicy =
        ReconcileCapturePolicy.of(
            List.of(
                new ReconcileCapturePolicy.Column("a", true, true),
                new ReconcileCapturePolicy.Column("b", true, false)),
            Set.of(
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileScope requested = scope(requestedPolicy);
    List<String> missing =
        ReconcileSnapshotContentState.missingCoverage(
            ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, requested),
            ReconcileSnapshotContentState.coverage(
                CaptureMode.CAPTURE_ONLY, scope(materializedPolicy)));

    ReconcileScope narrowed = ReconcileSnapshotContentState.narrowScope(requested, missing);

    assertThat(narrowed.capturePolicy().outputs())
        .containsExactly(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX);
    assertThat(narrowed.capturePolicy().columns())
        .extracting(
            ReconcileCapturePolicy.Column::selector,
            ReconcileCapturePolicy.Column::captureStats,
            ReconcileCapturePolicy.Column::captureIndex)
        .containsExactly(tuple("a", false, true), tuple("b", false, true));
  }

  @Test
  void narrowedScopeRegeneratesMaterializedIndexColumnsMissingFromIncomingRequest() {
    ReconcileCapturePolicy materializedPolicy =
        ReconcileCapturePolicy.of(
            List.of(
                new ReconcileCapturePolicy.Column("a", false, true),
                new ReconcileCapturePolicy.Column("b", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileCapturePolicy requestedPolicy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("c", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    List<String> materialized =
        ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(materializedPolicy));
    List<String> requested =
        ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(requestedPolicy));

    ReconcileScope narrowed =
        ReconcileSnapshotContentState.narrowScope(scope(requestedPolicy), requested, materialized);

    assertThat(narrowed.capturePolicy().columns())
        .extracting(ReconcileCapturePolicy.Column::selector)
        .containsExactly("c", "a", "b");
    assertThat(narrowed.capturePolicy().columns())
        .allMatch(ReconcileCapturePolicy.Column::captureIndex);
  }

  @Test
  void realizedDefaultIndexSelectorsArePreservedDuringLaterExplicitCapture() {
    ReconcileCapturePolicy defaultPolicy =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.FIRST_N,
            2);
    List<String> requestedDefault =
        ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(defaultPolicy));
    List<String> materializedDefault =
        ReconcileSnapshotContentState.materializedCoverage(
            requestedDefault, List.of(), List.of("customer_id", "order_id"));
    ReconcileCapturePolicy additionalPolicy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("region", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    List<String> additional =
        ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(additionalPolicy));

    ReconcileScope narrowed =
        ReconcileSnapshotContentState.narrowScope(
            scope(additionalPolicy), additional, materializedDefault);

    assertThat(materializedDefault).hasSize(3);
    assertThat(narrowed.capturePolicy().columns())
        .extracting(ReconcileCapturePolicy.Column::selector)
        .containsExactly("region", "customer_id", "order_id");
  }

  @Test
  void broaderDefaultCoverageSatisfiesNarrowerDefaultsAndRealizedExplicitColumns() {
    ReconcileCapturePolicy broad =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.FIRST_N,
            10);
    ReconcileCapturePolicy narrow =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.FIRST_N,
            3);
    List<String> requestedBroad =
        ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(broad));
    List<String> materializedBroad =
        ReconcileSnapshotContentState.materializedCoverage(
            requestedBroad,
            List.of("customer_id", "order_id", "region"),
            List.of("customer_id", "order_id", "region"));

    assertThat(
            ReconcileSnapshotContentState.missingCoverage(
                ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(narrow)),
                materializedBroad))
        .isEmpty();

    ReconcileCapturePolicy explicit =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("order_id", true, true)),
            Set.of(
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            3);
    assertThat(
            ReconcileSnapshotContentState.missingCoverage(
                ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(explicit)),
                materializedBroad))
        .isEmpty();
  }

  @Test
  void explicitColumnCoverageIncludesRealizedSelectorAliases() {
    ReconcileCapturePolicy explicit =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#2", true, true)),
            Set.of(
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            3);
    List<String> materialized =
        ReconcileSnapshotContentState.materializedCoverage(
            ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(explicit)),
            List.of("#2", "customer_name"),
            List.of("#2", "customer_name"));
    ReconcileCapturePolicy byName =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("customer_name", true, true)),
            Set.of(
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            3);

    assertThat(
            ReconcileSnapshotContentState.missingCoverage(
                ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(byName)),
                materialized))
        .isEmpty();
  }

  @Test
  void narrowerDefaultsAndUnrelatedExplicitColumnsDoNotSatisfyBroaderRequests() {
    ReconcileCapturePolicy narrow =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS),
            ReconcileCapturePolicy.DefaultColumnScope.FIRST_N,
            3);
    ReconcileCapturePolicy broad =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS),
            ReconcileCapturePolicy.DefaultColumnScope.FIRST_N,
            10);
    List<String> materializedNarrow =
        ReconcileSnapshotContentState.materializedCoverage(
            ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(narrow)),
            List.of("customer_id", "order_id", "region"),
            List.of());

    assertThat(
            ReconcileSnapshotContentState.missingCoverage(
                ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(broad)),
                materializedNarrow))
        .isNotEmpty();
    assertThat(
            ReconcileSnapshotContentState.missingCoverage(
                ReconcileSnapshotContentState.coverage(
                    CaptureMode.CAPTURE_ONLY, scope(policy("missing"))),
                materializedNarrow))
        .isNotEmpty();
  }

  @Test
  void nonColumnCoverageDoesNotDependOnColumnDefaultBreadth() {
    ReconcileCapturePolicy first =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(ReconcileCapturePolicy.Output.TABLE_STATS),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            8);
    ReconcileCapturePolicy second =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(ReconcileCapturePolicy.Output.TABLE_STATS),
            ReconcileCapturePolicy.DefaultColumnScope.ALL,
            128);

    assertThat(ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(first)))
        .isEqualTo(ReconcileSnapshotContentState.coverage(CaptureMode.CAPTURE_ONLY, scope(second)));
  }

  private static ReconcileScope scope(ReconcileCapturePolicy policy) {
    return ReconcileScope.of(List.of(), "table-1", List.of(), policy);
  }

  private static ReconcileCapturePolicy policy(String... selectors) {
    return ReconcileCapturePolicy.of(
        java.util.Arrays.stream(selectors)
            .map(selector -> new ReconcileCapturePolicy.Column(selector, true, false))
            .toList(),
        Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS),
        ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
        32);
  }
}
