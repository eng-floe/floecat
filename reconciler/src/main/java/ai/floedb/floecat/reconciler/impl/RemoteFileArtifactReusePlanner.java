/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleSelection;
import ai.floedb.floecat.reconciler.jobs.ReusableIndexArtifactReference;
import ai.floedb.floecat.reconciler.jobs.ReusableStatsArtifactReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/** Computes file artifact reuse in the remote planner, before file-group jobs are persisted. */
final class RemoteFileArtifactReusePlanner {
  private RemoteFileArtifactReusePlanner() {}

  static List<ReconcileFileExecutionPlan> enrichFromBundles(
      String executionSchemaJson,
      List<ReconcileFileExecutionPlan> plans,
      ReconcileCapturePolicy capturePolicy,
      boolean fullRescan,
      List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference> bundles) {
    Map<String, List<BundleStatsCandidate>> statsByPath = new HashMap<>();
    Map<String, List<BundleIndexCandidate>> indexesByPath = new HashMap<>();
    for (var bundle :
        bundles == null
            ? List.<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference>of()
            : bundles) {
      if (!bundle.hasArtifact() || bundle.getArtifact().getPayloadUri().isBlank()) {
        continue;
      }
      bundle
          .getFileStatsList()
          .forEach(
              metadata ->
                  statsByPath
                      .computeIfAbsent(metadata.getFilePath(), ignored -> new ArrayList<>())
                      .add(new BundleStatsCandidate(bundle, metadata)));
      bundle
          .getIndexArtifactsList()
          .forEach(
              metadata ->
                  indexesByPath
                      .computeIfAbsent(metadata.getFilePath(), ignored -> new ArrayList<>())
                      .add(new BundleIndexCandidate(bundle, metadata)));
    }
    String statsSignature = FileArtifactReuse.statsCaptureSignature(capturePolicy);
    String indexSignature = FileArtifactReuse.indexCaptureSignature(capturePolicy);
    boolean requestsStats =
        !FileGroupExecutionSupport.requestedFileGroupStatsTargetKinds(capturePolicy).isEmpty();
    boolean requestsIndexes = capturePolicy.requestsIndexes();
    List<ReconcileFileExecutionPlan> enriched = new ArrayList<>();
    for (ReconcileFileExecutionPlan plan :
        plans == null ? List.<ReconcileFileExecutionPlan>of() : plans) {
      String sourceFingerprint = FileArtifactReuse.sourceFingerprint(plan, executionSchemaJson);
      String indexSourceFingerprint = FileArtifactReuse.indexSourceFingerprint(plan);
      Map<String, String> auxiliaryFingerprints =
          FileArtifactReuse.auxiliaryStatsFingerprints(plan);
      Map<String, BundleSelectionBuilder> selected = new java.util.LinkedHashMap<>();
      boolean reusable = !fullRescan && !plan.contentIdentity().isBlank();
      if (reusable && requestsStats) {
        statsByPath.getOrDefault(plan.filePath(), List.of()).stream()
            .filter(
                candidate -> sourceFingerprint.equals(candidate.metadata().getSourceFingerprint()))
            .filter(
                candidate -> statsSignature.equals(candidate.metadata().getStatsCaptureSignature()))
            .findFirst()
            .ifPresent(candidate -> selectStats(selected, candidate.bundle(), plan.filePath()));
        for (Map.Entry<String, String> entry : auxiliaryFingerprints.entrySet()) {
          statsByPath.getOrDefault(entry.getKey(), List.of()).stream()
              .filter(
                  candidate -> entry.getValue().equals(candidate.metadata().getSourceFingerprint()))
              .filter(
                  candidate ->
                      statsSignature.equals(candidate.metadata().getStatsCaptureSignature()))
              .findFirst()
              .ifPresent(candidate -> selectStats(selected, candidate.bundle(), entry.getKey()));
        }
      }
      if (reusable && requestsIndexes) {
        indexesByPath.getOrDefault(plan.filePath(), List.of()).stream()
            .filter(
                candidate ->
                    indexSourceFingerprint.equals(candidate.metadata().getSourceFingerprint()))
            .filter(
                candidate -> indexSignature.equals(candidate.metadata().getIndexCaptureSignature()))
            .findFirst()
            .ifPresent(candidate -> selectIndex(selected, candidate.bundle(), plan.filePath()));
      }
      enriched.add(
          plan.withReuseBundleSelections(
              sourceFingerprint,
              indexSourceFingerprint,
              statsSignature,
              indexSignature,
              auxiliaryFingerprints,
              selected.values().stream().map(BundleSelectionBuilder::build).toList()));
    }
    return List.copyOf(enriched);
  }

  private static void selectStats(
      Map<String, BundleSelectionBuilder> selected,
      ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference bundle,
      String path) {
    selected
        .computeIfAbsent(
            bundle.getArtifact().getPayloadUri(), ignored -> new BundleSelectionBuilder(bundle))
        .statsPaths
        .add(path);
  }

  private static void selectIndex(
      Map<String, BundleSelectionBuilder> selected,
      ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference bundle,
      String path) {
    selected
        .computeIfAbsent(
            bundle.getArtifact().getPayloadUri(), ignored -> new BundleSelectionBuilder(bundle))
        .indexPaths
        .add(path);
  }

  private record BundleStatsCandidate(
      ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference bundle,
      ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata metadata) {}

  private record BundleIndexCandidate(
      ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference bundle,
      ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata metadata) {}

  private static final class BundleSelectionBuilder {
    final ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference bundle;
    final List<String> statsPaths = new ArrayList<>();
    final List<String> indexPaths = new ArrayList<>();

    BundleSelectionBuilder(
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference bundle) {
      this.bundle = bundle;
    }

    ReusableArtifactBundleSelection build() {
      var artifact = bundle.getArtifact();
      return new ReusableArtifactBundleSelection(
          artifact.getTargetStorageId(),
          artifact.getPayloadUri(),
          artifact.getPayloadBytes(),
          artifact.getPayloadSha256().toByteArray(),
          List.copyOf(statsPaths),
          List.copyOf(indexPaths));
    }
  }

  static List<ReconcileFileExecutionPlan> enrichFromReferences(
      String executionSchemaJson,
      List<ReconcileFileExecutionPlan> plans,
      ReconcileCapturePolicy capturePolicy,
      boolean fullRescan,
      List<ReusableStatsArtifactReference> historicalStats,
      List<ReusableIndexArtifactReference> historicalIndexes) {
    Map<String, List<ReusableStatsArtifactReference>> statsByPath =
        (historicalStats == null ? List.<ReusableStatsArtifactReference>of() : historicalStats)
            .stream()
                .filter(reference -> reference != null && !reference.isEmpty())
                .collect(Collectors.groupingBy(ReusableStatsArtifactReference::filePath));
    Map<String, List<ReusableIndexArtifactReference>> indexesByPath =
        (historicalIndexes == null ? List.<ReusableIndexArtifactReference>of() : historicalIndexes)
            .stream()
                .filter(reference -> reference != null && !reference.isEmpty())
                .collect(Collectors.groupingBy(ReusableIndexArtifactReference::filePath));
    String statsSignature = FileArtifactReuse.statsCaptureSignature(capturePolicy);
    String indexSignature = FileArtifactReuse.indexCaptureSignature(capturePolicy);
    boolean requestsStats =
        !FileGroupExecutionSupport.requestedFileGroupStatsTargetKinds(capturePolicy).isEmpty();
    boolean requestsIndexes = capturePolicy.requestsIndexes();
    List<ReconcileFileExecutionPlan> enriched = new ArrayList<>();
    for (ReconcileFileExecutionPlan plan :
        plans == null ? List.<ReconcileFileExecutionPlan>of() : plans) {
      String sourceFingerprint = FileArtifactReuse.sourceFingerprint(plan, executionSchemaJson);
      String indexSourceFingerprint = FileArtifactReuse.indexSourceFingerprint(plan);
      Map<String, String> auxiliaryFingerprints =
          FileArtifactReuse.auxiliaryStatsFingerprints(plan);
      ReusableStatsArtifactReference statsReference = null;
      List<ReusableStatsArtifactReference> auxiliaryReferences = new ArrayList<>();
      ReusableIndexArtifactReference indexReference = null;
      if (!fullRescan && !plan.contentIdentity().isBlank() && requestsStats) {
        statsReference =
            statsByPath.getOrDefault(plan.filePath(), List.of()).stream()
                .filter(reference -> sourceFingerprint.equals(reference.sourceFingerprint()))
                .filter(reference -> statsSignature.equals(reference.statsCaptureSignature()))
                .findFirst()
                .orElse(null);
        for (Map.Entry<String, String> entry : auxiliaryFingerprints.entrySet()) {
          statsByPath.getOrDefault(entry.getKey(), List.of()).stream()
              .filter(reference -> entry.getValue().equals(reference.sourceFingerprint()))
              .filter(reference -> statsSignature.equals(reference.statsCaptureSignature()))
              .findFirst()
              .ifPresent(auxiliaryReferences::add);
        }
      }
      if (!fullRescan && !plan.contentIdentity().isBlank() && requestsIndexes) {
        indexReference =
            indexesByPath.getOrDefault(plan.filePath(), List.of()).stream()
                .filter(reference -> indexSourceFingerprint.equals(reference.sourceFingerprint()))
                .filter(reference -> indexSignature.equals(reference.indexCaptureSignature()))
                .findFirst()
                .orElse(null);
      }
      enriched.add(
          plan.withReuseReferences(
              sourceFingerprint,
              indexSourceFingerprint,
              statsSignature,
              indexSignature,
              auxiliaryFingerprints,
              statsReference,
              auxiliaryReferences,
              indexReference));
    }
    return List.copyOf(enriched);
  }

  static List<ReconcileFileExecutionPlan> enrich(
      ResourceId tableId,
      long snapshotId,
      String executionSchemaJson,
      List<ReconcileFileExecutionPlan> plans,
      ReconcileCapturePolicy capturePolicy,
      boolean fullRescan,
      List<TargetStatsRecord> historicalStats,
      List<IndexArtifactRecord> historicalIndexes,
      BiFunction<Long, String, String> historicalContentIdentity) {
    Map<String, List<TargetStatsRecord>> statsByPath = statsByPath(historicalStats);
    Map<String, List<IndexArtifactRecord>> indexesByPath = indexesByPath(historicalIndexes);
    String statsSignature = FileArtifactReuse.statsCaptureSignature(capturePolicy);
    String indexSignature = FileArtifactReuse.indexCaptureSignature(capturePolicy);
    boolean requestsStats =
        !FileGroupExecutionSupport.requestedFileGroupStatsTargetKinds(capturePolicy).isEmpty();
    boolean requestsIndexes = capturePolicy.requestsIndexes();
    List<ReconcileFileExecutionPlan> enriched = new ArrayList<>();
    for (ReconcileFileExecutionPlan plan :
        plans == null ? List.<ReconcileFileExecutionPlan>of() : plans) {
      String sourceFingerprint = FileArtifactReuse.sourceFingerprint(plan, executionSchemaJson);
      String indexSourceFingerprint = FileArtifactReuse.indexSourceFingerprint(plan);
      Map<String, String> auxiliaryFingerprints =
          FileArtifactReuse.auxiliaryStatsFingerprints(plan);
      TargetStatsRecord reusableStats = TargetStatsRecord.getDefaultInstance();
      List<TargetStatsRecord> reusableAuxiliaryStats = List.of();
      IndexArtifactRecord reusableIndex = IndexArtifactRecord.getDefaultInstance();
      TargetStatsRecord sourceIdentityStats = null;
      boolean reusable = !fullRescan && snapshotId >= 0L && !plan.contentIdentity().isBlank();
      if (reusable && requestsStats) {
        TargetStatsRecord prior =
            statsByPath.getOrDefault(plan.filePath(), List.of()).stream()
                .filter(
                    candidate ->
                        FileArtifactReuse.compatibleStats(
                            candidate, plan.filePath(), sourceFingerprint, statsSignature))
                .findFirst()
                .orElse(null);
        boolean migratedLegacy = false;
        if (prior == null && legacyStatsPolicyCompatible(capturePolicy)) {
          prior =
              statsByPath.getOrDefault(plan.filePath(), List.of()).stream()
                  .filter(
                      candidate ->
                          legacyStatsCompatible(
                              candidate, plan, capturePolicy, historicalContentIdentity))
                  .findFirst()
                  .orElse(null);
          migratedLegacy = prior != null;
        }
        sourceIdentityStats = prior;
        if (prior != null) {
          reusableStats =
              FileArtifactReuse.bindStatsToSnapshot(
                  prior, tableId, snapshotId, sourceFingerprint, statsSignature);
          if (migratedLegacy) {
            reusableStats =
                FileArtifactReuse.stampStats(
                    reusableStats,
                    sourceFingerprint,
                    statsSignature,
                    prior.getFile().getColumnsList().stream()
                        .map(column -> "#" + column.getColumnId())
                        .toList());
          }
        }
        List<TargetStatsRecord> auxiliary = new ArrayList<>();
        for (Map.Entry<String, String> entry : auxiliaryFingerprints.entrySet()) {
          TargetStatsRecord priorAuxiliary =
              statsByPath.getOrDefault(entry.getKey(), List.of()).stream()
                  .filter(
                      candidate ->
                          FileArtifactReuse.compatibleStats(
                              candidate, entry.getKey(), entry.getValue(), statsSignature))
                  .findFirst()
                  .orElse(null);
          auxiliary.add(
              priorAuxiliary == null
                  ? FileArtifactReuse.auxiliaryStatsRecord(
                      plan, entry.getKey(), tableId, snapshotId, entry.getValue(), statsSignature)
                  : FileArtifactReuse.bindStatsToSnapshot(
                      priorAuxiliary, tableId, snapshotId, entry.getValue(), statsSignature));
        }
        reusableAuxiliaryStats = List.copyOf(auxiliary);
      }
      if (reusable && requestsIndexes && sourceIdentityStats == null) {
        sourceIdentityStats =
            statsByPath.getOrDefault(plan.filePath(), List.of()).stream()
                .filter(
                    candidate ->
                        legacySourceIdentityCompatible(candidate, plan, historicalContentIdentity))
                .findFirst()
                .orElse(null);
      }
      if (reusable && requestsIndexes) {
        IndexArtifactRecord prior =
            indexesByPath.getOrDefault(plan.filePath(), List.of()).stream()
                .filter(
                    candidate ->
                        FileArtifactReuse.compatibleIndex(
                            candidate, plan.filePath(), indexSourceFingerprint, indexSignature))
                .findFirst()
                .orElse(null);
        if (prior == null
            && sourceIdentityStats != null
            && legacyIndexPolicyCompatible(capturePolicy)) {
          long priorSnapshotId = sourceIdentityStats.getSnapshotId();
          prior =
              indexesByPath.getOrDefault(plan.filePath(), List.of()).stream()
                  .filter(candidate -> candidate.getSnapshotId() == priorSnapshotId)
                  .filter(candidate -> legacyIndexCompatible(candidate, plan, capturePolicy))
                  .findFirst()
                  .orElse(null);
        }
        if (prior != null) {
          reusableIndex =
              FileArtifactReuse.bindIndexToSnapshot(
                  prior, tableId, snapshotId, indexSourceFingerprint, indexSignature);
        }
      }
      enriched.add(
          plan.withReuse(
              sourceFingerprint,
              indexSourceFingerprint,
              statsSignature,
              indexSignature,
              auxiliaryFingerprints,
              reusableStats,
              reusableAuxiliaryStats,
              reusableIndex));
    }
    return List.copyOf(enriched);
  }

  private static Map<String, List<TargetStatsRecord>> statsByPath(List<TargetStatsRecord> records) {
    Map<String, List<TargetStatsRecord>> result = new HashMap<>();
    for (TargetStatsRecord record : records == null ? List.<TargetStatsRecord>of() : records) {
      if (record != null && record.hasTarget() && record.getTarget().hasFile()) {
        result
            .computeIfAbsent(
                record.getTarget().getFile().getFilePath(), ignored -> new ArrayList<>())
            .add(record);
      }
    }
    return result;
  }

  private static Map<String, List<IndexArtifactRecord>> indexesByPath(
      List<IndexArtifactRecord> records) {
    Map<String, List<IndexArtifactRecord>> result = new HashMap<>();
    for (IndexArtifactRecord record : records == null ? List.<IndexArtifactRecord>of() : records) {
      if (record != null && record.hasTarget() && record.getTarget().hasFile()) {
        result
            .computeIfAbsent(
                record.getTarget().getFile().getFilePath(), ignored -> new ArrayList<>())
            .add(record);
      }
    }
    return result;
  }

  private static boolean legacyStatsPolicyCompatible(ReconcileCapturePolicy policy) {
    if (!policy.properties().isEmpty()) {
      return false;
    }
    if (!policy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)) {
      return true;
    }
    Set<String> selectors = policy.selectorsForStats();
    return !selectors.isEmpty()
        && selectors.stream().allMatch(selector -> selector.startsWith("#"));
  }

  private static boolean legacyStatsCompatible(
      TargetStatsRecord record,
      ReconcileFileExecutionPlan plan,
      ReconcileCapturePolicy policy,
      BiFunction<Long, String, String> historicalContentIdentity) {
    if (!legacySourceIdentityCompatible(record, plan, historicalContentIdentity)) {
      return false;
    }
    Set<String> selectors = policy.selectorsForStats();
    if (selectors.isEmpty()) {
      return true;
    }
    Set<String> present =
        record.getFile().getColumnsList().stream()
            .map(column -> "#" + column.getColumnId())
            .collect(Collectors.toSet());
    return present.containsAll(selectors);
  }

  private static boolean legacySourceIdentityCompatible(
      TargetStatsRecord record,
      ReconcileFileExecutionPlan plan,
      BiFunction<Long, String, String> historicalContentIdentity) {
    if (FileArtifactReuse.legacyCompatibleIcebergStats(record, plan)) {
      return true;
    }
    if (record == null || !plan.contentIdentity().startsWith("delta-add-v1:")) {
      return false;
    }
    return FileArtifactReuse.legacyCompatibleDeltaStats(
        record, plan, historicalContentIdentity.apply(record.getSnapshotId(), plan.filePath()));
  }

  private static boolean legacyIndexPolicyCompatible(ReconcileCapturePolicy policy) {
    return policy.properties().isEmpty() && !policy.selectorsForIndex().isEmpty();
  }

  private static boolean legacyIndexCompatible(
      IndexArtifactRecord record, ReconcileFileExecutionPlan plan, ReconcileCapturePolicy policy) {
    if (record == null
        || !record.hasTarget()
        || !record.getTarget().hasFile()
        || !plan.filePath().equals(record.getTarget().getFile().getFilePath())
        || record.getState() != IndexArtifactState.IAS_READY
        || record.getArtifactUri().isBlank()) {
      return false;
    }
    Set<String> indexed =
        Arrays.stream(record.getPropertiesOrDefault("indexed_columns", "").split(","))
            .map(String::trim)
            .filter(selector -> !selector.isBlank())
            .collect(Collectors.toSet());
    return indexed.containsAll(policy.selectorsForIndex());
  }
}
