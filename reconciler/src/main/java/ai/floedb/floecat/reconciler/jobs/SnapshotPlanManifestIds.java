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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public final class SnapshotPlanManifestIds {
  private SnapshotPlanManifestIds() {}

  public static String manifestHash(List<ReconcileFileGroupTask> fileGroups) {
    ManifestDigest digest = new ManifestDigest("snapshot-plan-v2");
    List<byte[]> groups =
        fileGroups == null
            ? List.of()
            : fileGroups.stream()
                .filter(group -> group != null && !group.isEmpty())
                .map(SnapshotPlanManifestIds::fileGroupDigest)
                .sorted(SnapshotPlanManifestIds::compareDigest)
                .toList();
    digest.add(groups.size());
    groups.forEach(digest::add);
    return digest.finish();
  }

  public static String manifestBlobUri(
      String accountId, String jobId, List<ReconcileFileGroupTask> fileGroups) {
    String acct = blankToEmpty(accountId);
    String job = blankToEmpty(jobId);
    if (acct.isBlank() || job.isBlank()) {
      throw new IllegalArgumentException(
          "accountId and jobId are required for snapshot plan manifests");
    }
    return "/accounts/"
        + acct
        + "/reconcile/jobs/"
        + job
        + "/snapshot-plan/"
        + "snapshot-plan-"
        + manifestHash(fileGroups)
        + ".json";
  }

  private static byte[] fileGroupDigest(ReconcileFileGroupTask group) {
    ManifestDigest digest = new ManifestDigest("file-group-v2");
    digest.add(group.planId());
    digest.add(group.groupId());
    digest.add(group.tableId());
    digest.add(group.snapshotId());
    digest.add(group.fileCount());
    addSortedStrings(digest, group.filePaths());
    digest.add(group.executionSchemaJson());
    List<byte[]> executionPlans =
        group.fileExecutionPlans().stream()
            .map(SnapshotPlanManifestIds::fileExecutionPlanDigest)
            .sorted(SnapshotPlanManifestIds::compareDigest)
            .toList();
    digest.add(executionPlans.size());
    executionPlans.forEach(digest::add);
    return digest.finishBytes();
  }

  private static byte[] fileExecutionPlanDigest(ReconcileFileExecutionPlan plan) {
    ManifestDigest digest = new ManifestDigest("file-execution-plan-v2");
    digest.add(plan.filePath());
    digest.add(plan.fileSizeInBytes());
    digest.add(plan.partitionDataJson());
    digest.add(plan.fileFormat());
    digest.add(plan.partitionSpecId());
    digest.add(plan.contentIdentity());

    ReconcileFileExecutionPlan.DeltaDeletionVector deletionVector = plan.deletionVector();
    digest.add(deletionVector != null);
    if (deletionVector != null) {
      digest.add(deletionVector.storageType());
      digest.add(deletionVector.pathOrInlineDv());
      digest.add(deletionVector.offset() != null);
      if (deletionVector.offset() != null) {
        digest.add(deletionVector.offset());
      }
      digest.add(deletionVector.sizeInBytes());
      digest.add(deletionVector.cardinality());
    }

    List<byte[]> deleteFiles =
        plan.icebergDeleteFiles().stream()
            .map(SnapshotPlanManifestIds::icebergDeleteFileDigest)
            .sorted(SnapshotPlanManifestIds::compareDigest)
            .toList();
    digest.add(deleteFiles.size());
    deleteFiles.forEach(digest::add);

    digest.add(plan.sourceFingerprint());
    digest.add(plan.indexSourceFingerprint());
    digest.add(plan.statsCaptureSignature());
    digest.add(plan.indexCaptureSignature());
    addSortedMap(digest, plan.auxiliaryStatsFingerprints());

    List<byte[]> selections =
        plan.reusableArtifactBundleSelections().stream()
            .map(SnapshotPlanManifestIds::bundleSelectionDigest)
            .sorted(SnapshotPlanManifestIds::compareDigest)
            .toList();
    digest.add(selections.size());
    selections.forEach(digest::add);
    return digest.finishBytes();
  }

  private static byte[] icebergDeleteFileDigest(
      ReconcileFileExecutionPlan.IcebergDeleteFile deleteFile) {
    ManifestDigest digest = new ManifestDigest("iceberg-delete-file-v2");
    digest.add(deleteFile.filePath());
    digest.add(deleteFile.fileSizeInBytes());
    digest.add(deleteFile.content().name());
    digest.add(deleteFile.partitionSpecId());
    List<Integer> equalityFieldIds = deleteFile.equalityFieldIds().stream().sorted().toList();
    digest.add(equalityFieldIds.size());
    equalityFieldIds.forEach(digest::add);
    digest.add(deleteFile.contentIdentity());
    return digest.finishBytes();
  }

  private static byte[] bundleSelectionDigest(ReusableArtifactBundleSelection selection) {
    ManifestDigest digest = new ManifestDigest("reuse-bundle-selection-v2");
    digest.add(selection.targetStorageId());
    digest.add(selection.payloadUri());
    digest.add(selection.payloadBytes());
    digest.add(selection.payloadSha256());
    addSortedStrings(digest, selection.statsFilePaths());
    addSortedStrings(digest, selection.indexFilePaths());
    return digest.finishBytes();
  }

  private static void addSortedStrings(ManifestDigest digest, List<String> values) {
    List<String> canonical =
        values == null
            ? List.of()
            : values.stream()
                .filter(value -> value != null && !value.isBlank())
                .map(String::trim)
                .sorted()
                .toList();
    digest.add(canonical.size());
    canonical.forEach(digest::add);
  }

  private static void addSortedMap(ManifestDigest digest, Map<String, String> values) {
    List<Map.Entry<String, String>> entries =
        values == null
            ? List.of()
            : values.entrySet().stream()
                .sorted(
                    Comparator.comparing((Map.Entry<String, String> entry) -> entry.getKey())
                        .thenComparing(Map.Entry::getValue))
                .toList();
    digest.add(entries.size());
    entries.forEach(
        entry -> {
          digest.add(entry.getKey());
          digest.add(entry.getValue());
        });
  }

  private static int compareDigest(byte[] left, byte[] right) {
    return java.util.Arrays.compareUnsigned(left, right);
  }

  private static final class ManifestDigest {
    private final MessageDigest digest;

    private ManifestDigest(String version) {
      try {
        digest = MessageDigest.getInstance("SHA-256");
      } catch (Exception e) {
        throw new IllegalStateException("Failed to create snapshot plan manifest digest", e);
      }
      add(version);
    }

    private void add(boolean value) {
      digest.update((byte) (value ? 1 : 0));
    }

    private void add(int value) {
      digest.update(
          new byte[] {
            (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
          });
    }

    private void add(long value) {
      digest.update(
          new byte[] {
            (byte) (value >>> 56),
            (byte) (value >>> 48),
            (byte) (value >>> 40),
            (byte) (value >>> 32),
            (byte) (value >>> 24),
            (byte) (value >>> 16),
            (byte) (value >>> 8),
            (byte) value
          });
    }

    private void add(String value) {
      add((value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
    }

    private void add(byte[] value) {
      byte[] bytes = value == null ? new byte[0] : value;
      add(bytes.length);
      digest.update(bytes);
    }

    private byte[] finishBytes() {
      return digest.digest();
    }

    private String finish() {
      return Base64.getUrlEncoder().withoutPadding().encodeToString(finishBytes());
    }
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }
}
