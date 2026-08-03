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

import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleSelection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    ReconcileCapturePolicy effectivePolicy =
        capturePolicy == null ? ReconcileCapturePolicy.empty() : capturePolicy;
    String statsSignature = FileArtifactReuse.statsCaptureSignature(effectivePolicy);
    String indexSignature =
        FileArtifactReuse.indexCaptureSignature(effectivePolicy, executionSchemaJson);
    boolean requestsStats =
        !FileGroupExecutionSupport.requestedFileGroupStatsTargetKinds(effectivePolicy).isEmpty();
    boolean requestsIndexes = effectivePolicy.requestsIndexes();
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
            .filter(candidate -> reusableIndexCoversPolicy(candidate.metadata(), effectivePolicy))
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

  private static boolean reusableIndexCoversPolicy(
      ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata metadata,
      ReconcileCapturePolicy capturePolicy) {
    Set<String> realizedSelectors =
        metadata.getRealizedIndexSelectorsList().stream()
            .map(String::trim)
            .filter(selector -> !selector.isBlank())
            .collect(Collectors.toSet());
    if (realizedSelectors.isEmpty()) {
      return false;
    }
    return realizedSelectors.containsAll(capturePolicy.selectorsForIndex());
  }

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
}
