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

package ai.floedb.floecat.catalog.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StorageLocationsTest {
  @Test
  void aPrefixCoversLocationsBeneathIt() {
    assertTrue(StorageLocations.covers("s3://warehouse/orders", "s3://warehouse/orders/data/p0"));
    assertTrue(StorageLocations.covers("s3://warehouse/orders", "s3://warehouse/orders"));
    assertFalse(StorageLocations.covers("s3://warehouse/orders", "s3://warehouse/returns/p0"));
    assertFalse(StorageLocations.covers("s3://warehouse/orders", "s3://other/orders/p0"));
  }

  @Test
  void coverageIsTextualLikeTheUpstreamContract() {
    // Not a path-boundary rule, on purpose. Iceberg's S3FileIO.clientForStoragePath selects the
    // longest storagePath.startsWith(storagePrefix), so it would use this credential for these
    // paths -- and a credential that does not really reach them gets a 403 from S3, which knows the
    // real grant. Refusing here would instead guarantee failure: the vend path is reached only once
    // no storage authority matched, so there is nothing left to fall back to.
    //
    // The path-boundary rule belongs to StorageAuthorityResolver.matchesLocationPrefix, which
    // decides what floecat authorizes rather than what an upstream credential applies to.
    assertTrue(
        StorageLocations.covers("s3://warehouse/orders", "s3://warehouse/orders-archive/2025/p0"));
    assertTrue(StorageLocations.covers("s3://warehouse/tpch", "s3://warehouse/tpch_10/customer"));
    assertTrue(StorageLocations.covers("s3://bucket/db/tbl", "s3://bucket/db/tblX"));
  }

  @Test
  void aTrailingSlashOnTheScopeStillNamesItsOwnRoot() {
    // S3 Tables and Unity routinely vend the slashed form, and the table's own location is spelled
    // without it, so a literal comparison would make the scope cover nothing at all.
    assertTrue(StorageLocations.covers("s3://bucket/db/tbl/", "s3://bucket/db/tbl"));
    assertTrue(StorageLocations.covers("s3://bucket/db/tbl/", "s3://bucket/db/tbl/data/p0"));
    // Handled by equality rather than by stripping the slash and matching a prefix: stripping would
    // reach the sibling, which the slashed spelling plainly does not name.
    assertFalse(StorageLocations.covers("s3://bucket/db/tbl/", "s3://bucket/db/tblX"));
  }

  @Test
  void surroundingWhitespaceDoesNotChangeWhatIsCovered() {
    assertTrue(StorageLocations.covers("  s3://bucket/db/tbl  ", "s3://bucket/db/tbl/p0"));
    assertTrue(StorageLocations.covers("s3://bucket/db/tbl", " s3://bucket/db/tbl/p0 "));
    assertFalse(StorageLocations.covers(" s3://bucket/db/tbl ", "s3://other/db/tbl/p0"));
  }

  @Test
  void s3SchemeAliasesAddressTheSameStore() {
    assertTrue(StorageLocations.covers("s3a://warehouse/orders", "s3://warehouse/orders/p0"));
    assertTrue(StorageLocations.covers("s3://warehouse/orders", "s3n://warehouse/orders/p0"));
    // A different store is a different store, however similar the path looks.
    assertFalse(StorageLocations.covers("gs://warehouse/orders", "s3://warehouse/orders/p0"));
  }

  @Test
  void anEmptyPrefixCoversEverythingAndAnAbsentLocationIsCoveredByNothing() {
    assertTrue(StorageLocations.covers("", "s3://warehouse/orders/p0"));
    assertTrue(StorageLocations.covers("   ", "s3://warehouse/orders/p0"));
    assertTrue(StorageLocations.covers(null, "s3://warehouse/orders/p0"));
    // "no scope" means blank, not a bare slash: Iceberg keeps only prefixes starting with "s3", so
    // a "/" scope is not a scope any credential arrives with, and reading it as "everything" would
    // be inventing a meaning for a malformed value.
    assertFalse(StorageLocations.covers("/", "s3://warehouse/orders/p0"));
    // A blank prefix covers an absent location too: there is no narrower answer available.
    assertTrue(StorageLocations.covers("", null));
    assertTrue(StorageLocations.covers(null, null));
    assertFalse(StorageLocations.covers("s3://warehouse/orders", null));
  }

  @Test
  void normalizeSchemeToleratesAnAbsentValueLikeItsSibling() {
    assertNull(StorageLocations.normalizeScheme(null));
    assertEquals("s3://bucket/k", StorageLocations.normalizeScheme("s3a://bucket/k"));
    assertEquals("gs://bucket/k", StorageLocations.normalizeScheme("gs://bucket/k"));
    assertEquals("no-scheme", StorageLocations.normalizeScheme("no-scheme"));
  }

  @Test
  void stripTrailingSlashIsTotal() {
    assertEquals("s3://b/k", StorageLocations.stripTrailingSlash("s3://b/k//"));
    assertEquals("", StorageLocations.stripTrailingSlash("///"));
    assertEquals("", StorageLocations.stripTrailingSlash(null));
  }

  @Test
  void vendedCredentialsAnswerForTheirOwnScope() {
    var credentials =
        new VendedStorageCredentials(
            Map.of("s3.access-key-id", "ASIA", "s3.secret-access-key", "secret"),
            "s3://warehouse/orders",
            Optional.of(Instant.parse("2026-09-01T15:00:00Z")));

    assertTrue(credentials.covers("s3://warehouse/orders/metadata/v1.metadata.json"));
    assertFalse(credentials.covers("s3://external-data/orders/p0"));
  }
}
