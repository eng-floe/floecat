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

package ai.floedb.floecat.service.integration;

import java.time.Instant;

/**
 * AWS credentials resolved for a Catalog Integration, independent of catalog protocol or format.
 */
record ResolvedAwsCredentials(
    String accessKeyId, String secretAccessKey, String sessionToken, Instant expiresAt) {
  @Override
  public String toString() {
    return "ResolvedAwsCredentials[accessKeyId=<redacted>, secretAccessKey=<redacted>,"
        + " sessionToken="
        + (sessionToken == null || sessionToken.isBlank() ? "<absent>" : "<redacted>")
        + ", expiresAt="
        + expiresAt
        + "]";
  }
}
