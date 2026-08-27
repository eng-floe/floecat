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
 * @param awsCredentials the AWS session tuple, or null when the response carried none
 * @param hasUnsupportedCredentials whether the response carried credentials for a cloud this client
 *     does not map (Azure, GCP, R2), which is a "fall back to a storage authority" signal rather
 *     than a failure
 * @param expirationEpochMillis the raw {@code expiration_time} field, left unparsed on purpose. The
 *     epoch-millis semantics -- and what a blank, non-positive or malformed value means -- are
 *     owned by {@code VendedStorageCredentials.expiryFromEpochMillis} in the connector SPI so every
 *     vending connector agrees; this module deliberately does not depend on the SPI, so it carries
 *     the value through instead of parsing a second copy of the rule.
 * @param storageUrl the storage prefix the credentials are scoped to, or null when absent
 */
public record TemporaryTableCredentials(
    AwsCredentials awsCredentials,
    boolean hasUnsupportedCredentials,
    String expirationEpochMillis,
    String storageUrl) {

  public record AwsCredentials(
      String accessKeyId, String secretAccessKey, String sessionToken, String accessPoint) {}
}
