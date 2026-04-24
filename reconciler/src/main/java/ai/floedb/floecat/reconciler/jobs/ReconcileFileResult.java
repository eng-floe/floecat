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

import com.fasterxml.jackson.annotation.JsonIgnore;

public record ReconcileFileResult(
    String filePath,
    State state,
    long statsProcessed,
    String message,
    ReconcileIndexArtifactResult indexArtifact) {
  public enum State {
    UNSPECIFIED,
    SUCCEEDED,
    FAILED,
    SKIPPED
  }

  public ReconcileFileResult {
    filePath = filePath == null ? "" : filePath.trim();
    state = state == null ? State.UNSPECIFIED : state;
    statsProcessed = Math.max(0L, statsProcessed);
    message = message == null ? "" : message.trim();
    indexArtifact = indexArtifact == null ? ReconcileIndexArtifactResult.empty() : indexArtifact;
  }

  public static ReconcileFileResult of(
      String filePath,
      State state,
      long statsProcessed,
      String message,
      ReconcileIndexArtifactResult indexArtifact) {
    if ((filePath == null || filePath.isBlank())
        && (state == null || state == State.UNSPECIFIED)
        && statsProcessed <= 0L
        && (message == null || message.isBlank())
        && (indexArtifact == null || indexArtifact.isEmpty())) {
      return empty();
    }
    return new ReconcileFileResult(filePath, state, statsProcessed, message, indexArtifact);
  }

  public static ReconcileFileResult of(
      String filePath, State state, long statsProcessed, String message) {
    return of(filePath, state, statsProcessed, message, ReconcileIndexArtifactResult.empty());
  }

  public static ReconcileFileResult succeeded(String filePath, long statsProcessed) {
    return succeeded(filePath, statsProcessed, ReconcileIndexArtifactResult.empty());
  }

  public static ReconcileFileResult succeeded(
      String filePath, long statsProcessed, ReconcileIndexArtifactResult indexArtifact) {
    return new ReconcileFileResult(filePath, State.SUCCEEDED, statsProcessed, "", indexArtifact);
  }

  public static ReconcileFileResult failed(String filePath, String message) {
    return new ReconcileFileResult(
        filePath, State.FAILED, 0L, message, ReconcileIndexArtifactResult.empty());
  }

  public static ReconcileFileResult empty() {
    return new ReconcileFileResult(
        "", State.UNSPECIFIED, 0L, "", ReconcileIndexArtifactResult.empty());
  }

  @JsonIgnore
  public boolean isEmpty() {
    return filePath.isBlank()
        && state == State.UNSPECIFIED
        && statsProcessed == 0L
        && message.isBlank()
        && indexArtifact.isEmpty();
  }
}
