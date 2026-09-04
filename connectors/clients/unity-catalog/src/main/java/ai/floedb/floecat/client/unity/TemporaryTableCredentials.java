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

package ai.floedb.floecat.client.unity;

/**
 * Cloud credentials returned by Unity Catalog's temporary-table-credentials endpoint.
 *
 * <p>When {@code awsCredentials} is null and {@code hasUnsupportedCredentials} is false, the
 * response carried no recognized credential field. Consumers may fall back to a storage authority;
 * a known unsupported cloud credential instead sets {@code hasUnsupportedCredentials}.
 *
 * @param awsCredentials the AWS session tuple, or null when the response carried none
 * @param hasUnsupportedCredentials whether the response carried credentials for a cloud this client
 *     does not map (Azure, GCP, R2), which is a "fall back to a storage authority" signal rather
 *     than a failure
 * @param expirationEpochMillis the raw {@code expiration_time} field, unparsed. The epoch-millis
 *     semantics belong to {@code VendedStorageCredentials.expiryFromEpochMillis} in the connector
 *     SPI, which this module does not depend on.
 * @param storageUrl the storage prefix the credentials are scoped to, or null when absent
 */
public record TemporaryTableCredentials(
    AwsCredentials awsCredentials,
    boolean hasUnsupportedCredentials,
    String expirationEpochMillis,
    String storageUrl) {

  /** One AWS session tuple. Its string representation never includes secrets. */
  public record AwsCredentials(
      String accessKeyId, String secretAccessKey, String sessionToken, String accessPoint) {

    @Override
    public String toString() {
      return "AwsCredentials[accessKeyId=<redacted>, secretAccessKey=<redacted>, "
          + "sessionToken="
          + (sessionToken != null && !sessionToken.isBlank() ? "<redacted>" : "<absent>")
          + ", accessPoint="
          + accessPoint
          + "]";
    }
  }
}
