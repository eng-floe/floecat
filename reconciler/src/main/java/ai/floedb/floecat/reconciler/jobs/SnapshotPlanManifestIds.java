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
import java.util.List;

public final class SnapshotPlanManifestIds {
  private SnapshotPlanManifestIds() {}

  public static String manifestHash(List<ReconcileFileGroupTask> fileGroups) {
    return hashValue(String.join("\n", canonicalSnapshotFileGroups(fileGroups)));
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

  private static List<String> canonicalSnapshotFileGroups(List<ReconcileFileGroupTask> fileGroups) {
    if (fileGroups == null || fileGroups.isEmpty()) {
      return List.of();
    }
    return fileGroups.stream()
        .filter(group -> group != null && !group.isEmpty())
        .map(
            group ->
                blankToEmpty(group.planId())
                    + "|"
                    + blankToEmpty(group.groupId())
                    + "|"
                    + blankToEmpty(group.tableId())
                    + "|"
                    + group.snapshotId()
                    + "|"
                    + String.join(",", canonicalFilePaths(group)))
        .sorted()
        .toList();
  }

  private static List<String> canonicalFilePaths(ReconcileFileGroupTask group) {
    if (group == null || group.filePaths() == null || group.filePaths().isEmpty()) {
      return List.of();
    }
    return group.filePaths().stream()
        .filter(path -> path != null && !path.isBlank())
        .map(String::trim)
        .sorted()
        .toList();
  }

  private static String hashValue(String value) {
    try {
      byte[] bytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to hash snapshot plan manifest identity", e);
    }
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }
}
