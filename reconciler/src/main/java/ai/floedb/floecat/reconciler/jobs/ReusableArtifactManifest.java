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

import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Structural validation shared by reuse-manifest publishers and consumers. */
public final class ReusableArtifactManifest {
  private ReusableArtifactManifest() {}

  public static Coverage validate(SnapshotCaptureManifest manifest) {
    if (manifest == null) {
      throw new IllegalArgumentException("snapshot capture manifest is required");
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
        if (artifact.getPayloadUri().startsWith(candidate + "reuse-bundles/")) {
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
    if (reusableFileStats.size() != manifest.getFileStatsRecordCount()
        || reusableIndexArtifacts.size() != manifest.getIndexArtifactCount()) {
      throw new IllegalArgumentException(
          "snapshot capture manifest reuse bundle coverage mismatch");
    }
    return new Coverage(Set.copyOf(reusableFileStats), Set.copyOf(reusableIndexArtifacts));
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

  public record Coverage(Set<String> fileStatsPaths, Set<String> indexArtifactPaths) {}
}
