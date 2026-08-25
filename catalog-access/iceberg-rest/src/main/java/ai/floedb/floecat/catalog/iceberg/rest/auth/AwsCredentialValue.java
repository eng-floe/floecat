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

package ai.floedb.floecat.catalog.iceberg.rest.auth;

import java.time.Instant;
import java.util.Objects;

/** One resolved AWS credential value. Its string representation never includes secrets. */
public record AwsCredentialValue(
    String accessKeyId, String secretAccessKey, String sessionToken, Instant expiresAt) {
  public AwsCredentialValue {
    accessKeyId = requireNonBlank(accessKeyId, "accessKeyId");
    secretAccessKey = requireNonBlank(secretAccessKey, "secretAccessKey");
  }

  public boolean isSessionCredential() {
    return sessionToken != null && !sessionToken.isBlank();
  }

  public boolean hasKnownExpiry() {
    return expiresAt != null;
  }

  @Override
  public String toString() {
    return "AwsCredentialValue[accessKeyId=<redacted>, secretAccessKey=<redacted>, "
        + "sessionToken="
        + (isSessionCredential() ? "<redacted>" : "<absent>")
        + ", expiresAt="
        + expiresAt
        + "]";
  }

  private static String requireNonBlank(String value, String field) {
    value = Objects.requireNonNull(value, field).trim();
    if (value.isEmpty()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }
}
