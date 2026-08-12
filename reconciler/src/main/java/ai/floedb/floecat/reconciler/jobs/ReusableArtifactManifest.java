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
package ai.floedb.floecat.reconciler.jobs;

import ai.floedb.floecat.reconciler.impl.ReusableArtifactIndexStore;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Structural validation shared by reuse-manifest publishers and consumers. */
public final class ReusableArtifactManifest {
  public static final int FORMAT_VERSION = 1;

  private ReusableArtifactManifest() {}

  public static Coverage validate(SnapshotCaptureManifest manifest) {
    return validate(manifest, true);
  }

  public static Coverage validateStructure(SnapshotCaptureManifest manifest) {
    return validate(manifest, false);
  }

  private static Coverage validate(
      SnapshotCaptureManifest manifest, boolean validateReusableIndexes) {
    if (manifest == null) {
      throw new IllegalArgumentException("snapshot capture manifest is required");
    }
    if (manifest.getFormatVersion() != FORMAT_VERSION) {
      throw new IllegalArgumentException("snapshot capture manifest format is unsupported");
    }
    Map<String, List<String>> groupStatsPrefixes = new LinkedHashMap<>();
    Set<String> expectedStatsPrefixes = new HashSet<>();
    for (var group : manifest.getFileGroupsList()) {
      if (group.getGroupId().isBlank()
          || group.getStatsObjectPrefix().isBlank()
          || !expectedStatsPrefixes.add(group.getStatsObjectPrefix())) {
        throw new IllegalArgumentException(
            "snapshot capture manifest has duplicate file-group identity");
      }
      groupStatsPrefixes
          .computeIfAbsent("reuse-bundle:" + group.getGroupId(), ignored -> new ArrayList<>())
          .add(group.getStatsObjectPrefix());
    }

    if (!manifest.getReusableArtifactBundlesComplete()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest reuse bundle index is not complete");
    }
    if (manifest.getReusableArtifactBundlesCount() != manifest.getFileGroupsCount()) {
      throw new IllegalArgumentException("snapshot capture manifest reuse bundle count mismatch");
    }
    validateAppendOnlyBase(manifest, validateReusableIndexes);
    long coveredSourceFiles =
        manifest.getFileGroupsList().stream()
            .mapToLong(group -> group.getSucceededFileCount())
            .sum();
    if (manifest.hasAppendOnlyBase()) {
      coveredSourceFiles += manifest.getAppendOnlyBase().getSourceFileCount();
    }
    if (coveredSourceFiles != manifest.getSourceFileCount()) {
      throw new IllegalArgumentException("snapshot capture manifest source coverage mismatch");
    }
    Set<String> bundleUris = new HashSet<>();
    Set<String> bundledStatsPrefixes = new HashSet<>();
    Set<String> reusableFileStats = new HashSet<>();
    Set<String> reusableIndexArtifacts = new HashSet<>();
    for (var bundle : manifest.getReusableArtifactBundlesList()) {
      if (!bundle.hasArtifact()) {
        throw new IllegalArgumentException(
            "snapshot capture manifest reuse bundle has no artifact");
      }
      var artifact = bundle.getArtifact();
      String statsPrefix = null;
      for (String candidate :
          groupStatsPrefixes.getOrDefault(artifact.getTargetStorageId(), List.of())) {
        if (artifact
            .getPayloadUri()
            .startsWith(candidate + ReusableArtifactBundleUris.BUNDLE_DIRECTORY)) {
          if (statsPrefix != null) {
            throw new IllegalArgumentException(
                "snapshot capture manifest reuse bundle identity is ambiguous");
          }
          statsPrefix = candidate;
        }
      }
      if (statsPrefix == null
          || !bundledStatsPrefixes.add(statsPrefix)
          || artifact.getPayloadBytes() <= 0L
          || artifact.getPayloadSha256().size() != 32
          || !ReusableArtifactBundleUris.matchesDigest(
              artifact.getPayloadUri(), artifact.getPayloadSha256().toByteArray())
          || !bundleUris.add(artifact.getPayloadUri())) {
        throw new IllegalArgumentException(
            "snapshot capture manifest reuse bundle identity mismatch");
      }
      for (var metadata : bundle.getFileStatsList()) {
        if (metadata.getFilePath().isBlank()
            || metadata.getSourceFingerprint().isBlank()
            || metadata.getStatsCaptureSignature().isBlank()
            || hasInvalidSelectors(metadata.getRealizedStatsSelectorsList())
            || !reusableFileStats.add(metadata.getFilePath())) {
          throw new IllegalArgumentException(
              "snapshot capture manifest reuse stats metadata mismatch");
        }
      }
      for (var metadata : bundle.getIndexArtifactsList()) {
        if (metadata.getFilePath().isBlank()
            || metadata.getSourceFingerprint().isBlank()
            || metadata.getIndexCaptureSignature().isBlank()
            || hasInvalidSelectors(metadata.getRealizedIndexSelectorsList())
            || !reusableIndexArtifacts.add(metadata.getFilePath())) {
          throw new IllegalArgumentException(
              "snapshot capture manifest reuse index metadata mismatch");
        }
      }
    }
    if (bundledStatsPrefixes.size() != manifest.getFileGroupsCount()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest current file-group bundle coverage mismatch");
    }
    int inheritedFileStats =
        manifest.hasAppendOnlyBase() ? manifest.getAppendOnlyBase().getFileStatsRecordCount() : 0;
    int inheritedIndexArtifacts =
        manifest.hasAppendOnlyBase() ? manifest.getAppendOnlyBase().getIndexArtifactCount() : 0;
    if (reusableFileStats.size() != manifest.getFileStatsRecordCount() - inheritedFileStats
        || reusableIndexArtifacts.size()
            != manifest.getIndexArtifactCount() - inheritedIndexArtifacts) {
      throw new IllegalArgumentException(
          "snapshot capture manifest reuse bundle coverage mismatch");
    }
    if (manifest.hasReusableArtifactIndex()) {
      if (validateReusableIndexes) {
        ReusableArtifactIndexStore.validateReference(manifest.getReusableArtifactIndex());
      }
      if (manifest.getReusableArtifactIndex().getFileStatsRecordCount()
              != manifest.getFileStatsRecordCount()
          || manifest.getReusableArtifactIndex().getIndexArtifactCount()
              != manifest.getIndexArtifactCount()) {
        throw new IllegalArgumentException(
            "snapshot capture manifest reusable artifact index count mismatch");
      }
    }
    return new Coverage(Set.copyOf(reusableFileStats), Set.copyOf(reusableIndexArtifacts));
  }

  /**
   * Validates only the summary fields consumed when an already-published manifest is reused as an
   * append-only base. Unlike {@link #validate}, this does not rebuild file-level coverage sets from
   * a potentially very large historical manifest.
   */
  public static void validateReuseBaseSummary(SnapshotCaptureManifest manifest) {
    if (manifest == null) {
      throw new IllegalArgumentException("snapshot capture manifest is required");
    }
    if (manifest.getFormatVersion() != FORMAT_VERSION) {
      throw new IllegalArgumentException("snapshot capture manifest format is unsupported");
    }
    if (!manifest.getReusableArtifactBundlesComplete()
        || manifest.getReusableArtifactBundlesCount() != manifest.getFileGroupsCount()) {
      throw new IllegalArgumentException("snapshot capture manifest reuse bundle summary mismatch");
    }
    validateAppendOnlyBase(manifest, true);
    long coveredSourceFiles = 0L;
    for (var group : manifest.getFileGroupsList()) {
      if (group.getGroupId().isBlank()
          || group.getStatsObjectPrefix().isBlank()
          || group.getSucceededFileCount() < 0) {
        throw new IllegalArgumentException(
            "snapshot capture manifest file-group summary is invalid");
      }
      coveredSourceFiles = Math.addExact(coveredSourceFiles, group.getSucceededFileCount());
    }
    if (manifest.hasAppendOnlyBase()) {
      coveredSourceFiles =
          Math.addExact(coveredSourceFiles, manifest.getAppendOnlyBase().getSourceFileCount());
    }
    if (coveredSourceFiles != manifest.getSourceFileCount()) {
      throw new IllegalArgumentException("snapshot capture manifest source coverage mismatch");
    }
    if (!manifest.hasReusableArtifactIndex()) {
      throw new IllegalArgumentException("snapshot capture manifest reuse summary is invalid");
    }
    ReusableArtifactIndexStore.validateReference(manifest.getReusableArtifactIndex());
    if (manifest.getReusableArtifactIndex().getFileStatsRecordCount()
            != manifest.getFileStatsRecordCount()
        || manifest.getReusableArtifactIndex().getIndexArtifactCount()
            != manifest.getIndexArtifactCount()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest reusable artifact index count mismatch");
    }
  }

  private static boolean hasInvalidSelectors(List<String> selectors) {
    Set<String> distinct = new HashSet<>();
    for (String selector : selectors) {
      if (selector == null || selector.isBlank() || !distinct.add(selector)) {
        return true;
      }
    }
    return false;
  }

  private static void validateAppendOnlyBase(
      SnapshotCaptureManifest manifest, boolean validateReusableIndexes) {
    if (!manifest.hasAppendOnlyBase()) {
      return;
    }
    var base = manifest.getAppendOnlyBase();
    if (base.getFormatVersion() != 1
        || base.getChainDepth() < 0
        || base.getSnapshotId() == manifest.getSnapshotId()
        || base.getManifestUri().isBlank()
        || base.getManifestBytes() <= 0L
        || base.getManifestSha256().size() != 32
        || base.getSourceFileCount() == 0
        || base.getSourceFileCount() > manifest.getSourceFileCount()
        || base.getFileStatsRecordCount() > manifest.getFileStatsRecordCount()
        || base.getIndexArtifactCount() > manifest.getIndexArtifactCount()
        || base.getStatsGenerationId().isBlank()
        || (base.getIndexArtifactCount() > 0 && base.getIndexGenerationId().isBlank())
        || !base.hasReusableArtifactIndex()) {
      throw new IllegalArgumentException("snapshot capture manifest append-only base is invalid");
    }
    if (validateReusableIndexes) {
      ReusableArtifactIndexStore.validateReference(base.getReusableArtifactIndex());
    }
    if (base.getReusableArtifactIndex().getFileStatsRecordCount() != base.getFileStatsRecordCount()
        || base.getReusableArtifactIndex().getIndexArtifactCount()
            != base.getIndexArtifactCount()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest append-only base index count mismatch");
    }
  }

  /** Returns the authenticated append-only depth represented by this manifest. */
  public static int chainDepth(SnapshotCaptureManifest manifest) {
    if (manifest == null || !manifest.hasAppendOnlyBase()) {
      return 0;
    }
    int baseDepth = manifest.getAppendOnlyBase().getChainDepth();
    if (baseDepth < 0) {
      throw new IllegalArgumentException("snapshot capture manifest chain depth is invalid");
    }
    return Math.addExact(baseDepth, 1);
  }

  public record Coverage(Set<String> fileStatsPaths, Set<String> indexArtifactPaths) {}
}
