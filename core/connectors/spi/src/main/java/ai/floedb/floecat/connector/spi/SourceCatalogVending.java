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

package ai.floedb.floecat.connector.spi;

/**
 * Format-neutral entry point for "does this connector ask its source catalog to vend storage
 * credentials?".
 *
 * <p>Two gates depend on the answer and must agree: the storage service decides whether to
 * <em>attempt</em> source-catalog vending for a table with no matching storage authority, and the
 * reconciler's config resolver decides whether to <em>absorb</em> the resulting missing-authority
 * error. Each catalog format declares vending its own way -- Iceberg REST through the {@code
 * X-Iceberg-Access-Delegation} header, Unity Catalog through an explicit opt-in property -- so both
 * gates route through this single dispatcher rather than special-casing formats independently and
 * drifting apart.
 */
public final class SourceCatalogVending {

  private SourceCatalogVending() {}

  /** Whether {@code config} declares that its source catalog vends storage credentials. */
  public static boolean declaresVendedCredentials(ConnectorConfig config) {
    if (config == null) {
      return false;
    }
    return switch (config.kind()) {
      case ICEBERG -> IcebergAccessDelegation.declaresVendedCredentials(config);
      case DELTA, UNITY -> DatabricksAccessDelegation.declaresVendedCredentials(config);
      case GLUE -> false;
    };
  }

  /**
   * Whether a connector's own catalog client already carries vended storage credentials once a
   * table is loaded -- i.e. whether an <em>untouched</em> config is enough to read the table's
   * data.
   *
   * <p>Distinct from {@link #declaresVendedCredentials}, and deliberately narrower. Declaring
   * vending is what makes the storage service <em>attempt</em> a source-catalog vend; this is what
   * makes it safe for the reconciler's config resolver to <em>absorb</em> a missing-authority error
   * and hand the connector back unchanged. Only Iceberg REST satisfies it: {@code loadTable}
   * returns storage-credentials and the {@code FileIO} built from that response uses them.
   *
   * <p>Delta and Unity Catalog do not. Their connector vends only when asked, through {@code
   * vendStorageCredentials}, and the Delta engine's S3 client is built once from the connector's
   * own {@code s3.*} options -- so an untouched config carries no storage credentials at all.
   * Absorbing there would trade a precise "no authority covers this location" failure for an opaque
   * {@code 403 AccessDenied} on the first read.
   */
  public static boolean clientAppliesVendedCredentials(ConnectorConfig config) {
    if (config == null) {
      return false;
    }
    return switch (config.kind()) {
      case ICEBERG -> IcebergAccessDelegation.declaresVendedCredentials(config);
      case DELTA, UNITY, GLUE -> false;
    };
  }
}
