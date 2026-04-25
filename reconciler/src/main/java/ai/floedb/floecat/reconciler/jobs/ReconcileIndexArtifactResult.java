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

public record ReconcileIndexArtifactResult(
    String artifactUri, String artifactFormat, int artifactFormatVersion) {
  public ReconcileIndexArtifactResult {
    artifactUri = artifactUri == null ? "" : artifactUri.trim();
    artifactFormat = artifactFormat == null ? "" : artifactFormat.trim();
    artifactFormatVersion = Math.max(0, artifactFormatVersion);
  }

  public static ReconcileIndexArtifactResult of(
      String artifactUri, String artifactFormat, int artifactFormatVersion) {
    if ((artifactUri == null || artifactUri.isBlank())
        && (artifactFormat == null || artifactFormat.isBlank())
        && artifactFormatVersion <= 0) {
      return empty();
    }
    return new ReconcileIndexArtifactResult(artifactUri, artifactFormat, artifactFormatVersion);
  }

  public static ReconcileIndexArtifactResult empty() {
    return new ReconcileIndexArtifactResult("", "", 0);
  }

  @JsonIgnore
  public boolean isEmpty() {
    return artifactUri.isBlank() && artifactFormat.isBlank() && artifactFormatVersion <= 0;
  }
}
