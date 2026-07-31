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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.auth.ReconcileWorkerAuthProvider;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRegistry;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineResult;
import ai.floedb.floecat.storage.spi.BlobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class StandaloneJavaFileGroupExecutionRunner {
  @Inject CaptureEngineRegistry captureEngineRegistry;
  @Inject ReconcileWorkerAuthProvider reconcileWorkerAuthProvider;
  @Inject BlobStore blobStore;

  @ConfigProperty(name = "floecat.reconciler.worker.auth.required", defaultValue = "true")
  boolean workerAuthRequired = true;

  public CaptureEngineResult execute(
      StandaloneFileGroupExecutionPayload payload,
      BooleanSupplier shouldStop,
      Consumer<TargetStatsRecord> fileStatsPublisher) {
    if (payload == null
        || payload.tableId() == null
        || payload.sourceConnector() == null
        || payload.sourceConnector().equals(payload.sourceConnector().getDefaultInstance())
        || payload.snapshotId() < 0
        || payload.plannedFilePaths().isEmpty()) {
      return CaptureEngineResult.empty();
    }
    BooleanSupplier stop = shouldStop == null ? () -> false : shouldStop;
    throwIfCancellationRequested(stop);
    var requestedStatsKinds =
        FileGroupExecutionSupport.requestedFileGroupStatsTargetKinds(payload.capturePolicy());
    java.util.Optional<String> authorizationHeader =
        workerAuthorizationHeader(payload.tableId().getAccountId());
    Consumer<TargetStatsRecord> publisher =
        java.util.Objects.requireNonNull(fileStatsPublisher, "fileStatsPublisher");
    List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts = new ArrayList<>();
    List<TargetStatsRecord> partialAggregates = new ArrayList<>();
    List<TargetStatsRecord> reusedFileRecords = new ArrayList<>();
    Set<String> publishedStatsTargets = new HashSet<>();
    Set<String> realizedStatsSelectors = new java.util.TreeSet<>();
    Map<String, ReconcileFileExecutionPlan> plansByPath = new LinkedHashMap<>();
    Map<String, LoadedReuseBundle> reuseBundles = loadReuseBundles(payload.fileExecutionPlans());
    for (ReconcileFileExecutionPlan unresolvedPlan : payload.fileExecutionPlans()) {
      ReconcileFileExecutionPlan plan =
          resolveReferences(unresolvedPlan, payload.tableId(), payload.snapshotId(), reuseBundles);
      plansByPath.put(plan.filePath(), plan);
      if (plan.reusesFileStats()) {
        publishReusableStats(
            plan.reusableFileStats(), publisher, reusedFileRecords, publishedStatsTargets, stop);
        addEncodedSelectors(
            realizedStatsSelectors,
            plan.reusableFileStats()
                .getPropertiesOrDefault(FileArtifactReuse.REALIZED_STATS_SELECTORS_PROPERTY, ""));
      }
      for (TargetStatsRecord auxiliary : plan.reusableAuxiliaryStats()) {
        publishReusableStats(auxiliary, publisher, reusedFileRecords, publishedStatsTargets, stop);
      }
      if (plan.reusesIndexArtifact()) {
        stagedIndexArtifacts.add(
            new ReconcilerBackend.StagedIndexArtifact(
                plan.reusableIndexArtifact(), null, "application/x-parquet"));
      }
    }
    if (!reusedFileRecords.isEmpty()) {
      partialAggregates.addAll(
          FileGroupTargetStatsRollup.partialAggregatesFromFileRecords(
              payload.tableId(), payload.snapshotId(), requestedStatsKinds, reusedFileRecords));
    }

    for (boolean captureStats : List.of(false, true)) {
      for (boolean captureIndex : List.of(false, true)) {
        if (!captureStats && !captureIndex) {
          continue;
        }
        List<String> paths =
            payload.plannedFilePaths().stream()
                .filter(
                    path -> {
                      ReconcileFileExecutionPlan plan = plansByPath.get(path);
                      boolean needsStats =
                          !requestedStatsKinds.isEmpty()
                              && (plan == null || !plan.reusesFileStats());
                      boolean needsIndex =
                          payload.capturePageIndex()
                              && (plan == null || !plan.reusesIndexArtifact());
                      return needsStats == captureStats && needsIndex == captureIndex;
                    })
                .toList();
        if (paths.isEmpty()) {
          continue;
        }
        List<TargetStatsRecord> completedStats = new ArrayList<>();
        List<ai.floedb.floecat.connector.spi.FloecatConnector.ParquetPageIndexEntry>
            completedIndexes = new ArrayList<>();
        CaptureEngineResult capture =
            captureEngineRegistry.capture(
                new CaptureEngineRequest(
                    payload.sourceConnector(),
                    payload.sourceNamespace(),
                    payload.sourceTable(),
                    payload.tableId(),
                    payload.snapshotId(),
                    payload.planId(),
                    payload.groupId(),
                    paths,
                    captureStats ? payload.statsColumns() : Set.of(),
                    captureIndex ? payload.indexColumns() : Set.of(),
                    FileGroupExecutionSupport.columnSelectorPolicy(payload.capturePolicy()),
                    captureStats ? requestedStatsKinds : Set.of(),
                    captureIndex,
                    java.util.Optional.of(payload.storageLocation())
                        .filter(location -> !location.isBlank()),
                    authorizationHeader,
                    java.util.Optional.of(payload.jobId()),
                    java.util.Optional.of(payload.leaseEpoch()),
                    stop),
                (fileStats, pageIndexEntries) -> {
                  List<TargetStatsRecord> completedFileStats =
                      fileStats == null ? List.of() : fileStats;
                  List<ai.floedb.floecat.connector.spi.FloecatConnector.ParquetPageIndexEntry>
                      completedPageIndexEntries =
                          pageIndexEntries == null ? List.of() : pageIndexEntries;
                  throwIfCancellationRequested(stop);
                  completedStats.addAll(completedFileStats);
                  completedIndexes.addAll(completedPageIndexEntries);
                  throwIfCancellationRequested(stop);
                });
        throwIfCancellationRequested(stop);
        for (TargetStatsRecord record : completedStats) {
          TargetStatsRecord stamped =
              stampCapturedStats(record, plansByPath, capture.realizedStatsSelectors());
          String target =
              ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(stamped.getTarget());
          if (publishedStatsTargets.add(target)) {
            publisher.accept(stamped);
          }
        }
        if (captureIndex) {
          stagedIndexArtifacts.addAll(
              stampCapturedIndexes(
                  FileGroupIndexArtifactStager.stage(
                      payload.tableId(),
                      payload.snapshotId(),
                      paths,
                      completedStats,
                      completedIndexes),
                  plansByPath));
        }
        stagedIndexArtifacts.addAll(
            stampCapturedIndexes(capture.stagedIndexArtifacts(), plansByPath));
        partialAggregates.addAll(capture.statsRecords());
        realizedStatsSelectors.addAll(capture.realizedStatsSelectors());
      }
    }
    List<TargetStatsRecord> mergedPartialAggregates =
        FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
            payload.tableId(), payload.snapshotId(), requestedStatsKinds, partialAggregates);
    return CaptureEngineResult.of(
        mergedPartialAggregates,
        List.of(),
        stagedIndexArtifacts,
        List.copyOf(realizedStatsSelectors));
  }

  private ReconcileFileExecutionPlan resolveReferences(
      ReconcileFileExecutionPlan plan,
      ai.floedb.floecat.common.rpc.ResourceId tableId,
      long snapshotId,
      Map<String, LoadedReuseBundle> bundles) {
    if (plan.reusableFileStatsReference() == null
        && plan.reusableAuxiliaryStatsReferences().isEmpty()
        && plan.reusableIndexArtifactReference() == null
        && plan.reusableArtifactBundleSelections().isEmpty()) {
      return plan;
    }
    TargetStatsRecord fileStats = plan.reusableFileStats();
    List<TargetStatsRecord> auxiliary = new ArrayList<>(plan.reusableAuxiliaryStats());
    IndexArtifactRecord index = plan.reusableIndexArtifact();
    for (var selection : plan.reusableArtifactBundleSelections()) {
      var bundle = bundles.get(selection.payloadUri());
      if (bundle == null) {
        throw new IllegalStateException(
            "Reusable artifact bundle was not loaded: " + selection.payloadUri());
      }
      for (String path : selection.statsFilePaths()) {
        TargetStatsRecord record = bundle.statsByPath().get(path);
        if (record == null) {
          throw new IllegalStateException("Reusable artifact bundle is missing stats for " + path);
        }
        String fingerprint =
            path.equals(plan.filePath())
                ? plan.sourceFingerprint()
                : plan.auxiliaryStatsFingerprints().getOrDefault(path, "");
        TargetStatsRecord rebound =
            FileArtifactReuse.bindStatsToSnapshot(
                record, tableId, snapshotId, fingerprint, plan.statsCaptureSignature());
        if (path.equals(plan.filePath())) {
          fileStats = rebound;
        } else {
          auxiliary.add(rebound);
        }
      }
      for (String path : selection.indexFilePaths()) {
        IndexArtifactRecord record = bundle.indexesByPath().get(path);
        if (record == null) {
          throw new IllegalStateException("Reusable artifact bundle is missing index for " + path);
        }
        index =
            FileArtifactReuse.bindIndexToSnapshot(
                record,
                tableId,
                snapshotId,
                plan.indexSourceFingerprint(),
                plan.indexCaptureSignature());
      }
    }
    if (plan.reusableFileStatsReference() != null) {
      try {
        fileStats =
            TargetStatsRecord.parseFrom(
                readReference(
                    plan.reusableFileStatsReference().payloadUri(),
                    plan.reusableFileStatsReference().payloadBytes(),
                    plan.reusableFileStatsReference().payloadSha256()));
        fileStats =
            FileArtifactReuse.bindStatsToSnapshot(
                fileStats,
                tableId,
                snapshotId,
                plan.sourceFingerprint(),
                plan.statsCaptureSignature());
      } catch (Exception error) {
        throw new IllegalStateException("Failed to resolve reusable file stats", error);
      }
    }
    for (var reference : plan.reusableAuxiliaryStatsReferences()) {
      try {
        TargetStatsRecord record =
            TargetStatsRecord.parseFrom(
                readReference(
                    reference.payloadUri(), reference.payloadBytes(), reference.payloadSha256()));
        auxiliary.add(
            FileArtifactReuse.bindStatsToSnapshot(
                record,
                tableId,
                snapshotId,
                reference.sourceFingerprint(),
                reference.statsCaptureSignature()));
      } catch (Exception error) {
        throw new IllegalStateException("Failed to resolve reusable auxiliary stats", error);
      }
    }
    if (plan.reusableIndexArtifactReference() != null) {
      try {
        index =
            IndexArtifactRecord.parseFrom(
                readReference(
                    plan.reusableIndexArtifactReference().payloadUri(),
                    plan.reusableIndexArtifactReference().payloadBytes(),
                    plan.reusableIndexArtifactReference().payloadSha256()));
        index =
            FileArtifactReuse.bindIndexToSnapshot(
                index,
                tableId,
                snapshotId,
                plan.indexSourceFingerprint(),
                plan.indexCaptureSignature());
      } catch (Exception error) {
        throw new IllegalStateException("Failed to resolve reusable index artifact", error);
      }
    }
    return plan.withReuse(
        plan.sourceFingerprint(),
        plan.indexSourceFingerprint(),
        plan.statsCaptureSignature(),
        plan.indexCaptureSignature(),
        plan.auxiliaryStatsFingerprints(),
        fileStats,
        auxiliary,
        index);
  }

  private Map<String, LoadedReuseBundle> loadReuseBundles(List<ReconcileFileExecutionPlan> plans) {
    Map<String, ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleSelection> selections =
        new LinkedHashMap<>();
    plans.stream()
        .flatMap(plan -> plan.reusableArtifactBundleSelections().stream())
        .forEach(selection -> selections.putIfAbsent(selection.payloadUri(), selection));
    Map<String, LoadedReuseBundle> bundles = new LinkedHashMap<>();
    for (var selection : selections.values()) {
      try {
        byte[] bytes =
            readReference(
                selection.payloadUri(), selection.payloadBytes(), selection.payloadSha256());
        var bundle =
            ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundlePayload.parseFrom(bytes);
        if (bundle.getFormatVersion() != 1) {
          throw new IllegalStateException("Unsupported reusable artifact bundle version");
        }
        Map<String, TargetStatsRecord> statsByPath = new LinkedHashMap<>();
        bundle.getFileStatsList().forEach(record -> statsByPath.put(statsFilePath(record), record));
        Map<String, IndexArtifactRecord> indexesByPath = new LinkedHashMap<>();
        bundle
            .getIndexArtifactsList()
            .forEach(record -> indexesByPath.put(indexFilePath(record), record));
        bundles.put(selection.payloadUri(), new LoadedReuseBundle(statsByPath, indexesByPath));
      } catch (Exception error) {
        throw new IllegalStateException(
            "Failed to load reusable artifact bundle " + selection.payloadUri(), error);
      }
    }
    return bundles;
  }

  private static String statsFilePath(TargetStatsRecord record) {
    return record.hasTarget() && record.getTarget().hasFile()
        ? record.getTarget().getFile().getFilePath()
        : "";
  }

  private static String indexFilePath(IndexArtifactRecord record) {
    return record.hasTarget() && record.getTarget().hasFile()
        ? record.getTarget().getFile().getFilePath()
        : "";
  }

  private record LoadedReuseBundle(
      Map<String, TargetStatsRecord> statsByPath, Map<String, IndexArtifactRecord> indexesByPath) {}

  private byte[] readReference(String uri, long expectedBytes, byte[] expectedSha256) {
    if (blobStore == null) {
      throw new IllegalStateException("Blob store is unavailable");
    }
    byte[] bytes = blobStore.get(uri);
    if (bytes.length != expectedBytes || !MessageDigest.isEqual(expectedSha256, sha256(bytes))) {
      throw new IllegalStateException("Reusable artifact reference validation failed: " + uri);
    }
    return bytes;
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (java.security.NoSuchAlgorithmException error) {
      throw new IllegalStateException(error);
    }
  }

  private static void publishReusableStats(
      TargetStatsRecord record,
      Consumer<TargetStatsRecord> publisher,
      List<TargetStatsRecord> reused,
      Set<String> publishedTargets,
      BooleanSupplier stop) {
    throwIfCancellationRequested(stop);
    String target =
        ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(record.getTarget());
    if (publishedTargets.add(target)) {
      publisher.accept(record);
      reused.add(record);
    }
  }

  private static TargetStatsRecord stampCapturedStats(
      TargetStatsRecord record,
      Map<String, ReconcileFileExecutionPlan> plansByPath,
      List<String> realizedSelectors) {
    String filePath = record.hasFile() ? record.getFile().getFilePath() : "";
    ReconcileFileExecutionPlan owner = plansByPath.get(filePath);
    String fingerprint = owner == null ? "" : owner.sourceFingerprint();
    if (owner == null) {
      for (ReconcileFileExecutionPlan candidate : plansByPath.values()) {
        String auxiliary = candidate.auxiliaryStatsFingerprints().get(filePath);
        if (auxiliary != null) {
          owner = candidate;
          fingerprint = auxiliary;
          break;
        }
      }
    }
    return owner == null
        ? record
        : FileArtifactReuse.stampStats(
            record, fingerprint, owner.statsCaptureSignature(), realizedSelectors);
  }

  private static List<ReconcilerBackend.StagedIndexArtifact> stampCapturedIndexes(
      List<ReconcilerBackend.StagedIndexArtifact> artifacts,
      Map<String, ReconcileFileExecutionPlan> plansByPath) {
    if (artifacts == null || artifacts.isEmpty()) {
      return List.of();
    }
    List<ReconcilerBackend.StagedIndexArtifact> stamped = new ArrayList<>();
    for (ReconcilerBackend.StagedIndexArtifact artifact : artifacts) {
      String path =
          artifact.record().hasTarget() && artifact.record().getTarget().hasFile()
              ? artifact.record().getTarget().getFile().getFilePath()
              : "";
      ReconcileFileExecutionPlan plan = plansByPath.get(path);
      stamped.add(
          plan == null
              ? artifact
              : new ReconcilerBackend.StagedIndexArtifact(
                  FileArtifactReuse.stampIndex(
                      artifact.record(),
                      plan.indexSourceFingerprint(),
                      plan.indexCaptureSignature()),
                  artifact.content(),
                  artifact.contentType()));
    }
    return List.copyOf(stamped);
  }

  private static void addEncodedSelectors(Set<String> out, String encoded) {
    if (encoded == null || encoded.isBlank()) {
      return;
    }
    for (String selector : encoded.split(",")) {
      if (!selector.isBlank()) {
        out.add(selector.trim());
      }
    }
  }

  private static void throwIfCancellationRequested(BooleanSupplier shouldStop) {
    if (shouldStop.getAsBoolean()) {
      throw new CancellationException("file-group execution cancelled");
    }
  }

  private java.util.Optional<String> workerAuthorizationHeader(String accountId) {
    if (!workerAuthRequired) {
      return java.util.Optional.empty();
    }
    return reconcileWorkerAuthProvider.authorizationHeader(accountId);
  }
}
