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

package ai.floedb.floecat.service.repo.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class KeysTest {

  @Test
  void snapshotPointerByIdUsesPathSafeEncoding() {
    assertEquals(
        "/accounts/acct%20id/tables/table%20id/snapshots/by-id/0000000000000000000",
        Keys.snapshotPointerById("acct id", "table id", 0L));
  }

  @Test
  void snapshotConstraintsKeysUsePathSafeEncoding() {
    assertEquals(
        "/accounts/acct%20id/tables/table%20id/constraints/by-snapshot/0000000000000000007",
        Keys.snapshotConstraintsPointer("acct id", "table id", 7L));
    assertEquals(
        "/accounts/acct%20id/tables/table%20id/snapshots/0000000000000000007/stats/constraints",
        Keys.snapshotConstraintsStatsPointer("acct id", "table id", 7L));
  }

  @Test
  void encodeSegmentUsesRfc3986PathSegmentRules() {
    assertEquals("a%20b", Keys.encodeSegment("a b"));
    assertEquals("a%2Bb", Keys.encodeSegment("a+b"));
    assertEquals("a%2Fb", Keys.encodeSegment("a/b"));
    assertEquals("a%25b", Keys.encodeSegment("a%b"));
  }

  @Test
  void namespaceByPathKeyRoundTripsFullPathSegments() {
    String key =
        Keys.namespacePointerByPath(
            "acct id", "cat/id", List.of("finance", "sales/emea", "a+b", "a%b"));

    assertEquals(
        List.of("finance", "sales/emea", "a+b", "a%b"),
        Keys.extractNamespacePathSegments("acct id", "cat/id", key));
  }

  @Test
  void transactionDeleteSentinelEncodesOpaquePointerKey() {
    assertEquals(
        "/accounts/acct/transactions/tx/delete/%2Faccounts%2Fa%2Fb",
        Keys.transactionDeleteSentinelUri("acct", "tx", "/accounts/a/b"));
  }

  @Test
  void reconcileLeaseExpiryKeyPreservesLegacyRawAccountAndJobIds() {
    assertEquals(
        "/accounts/by-id/reconcile/job-leases/by-expiry/0000000000000000007"
            + "/accounts/acct+legacy/jobs/job%legacy",
        Keys.reconcileJobLeaseExpiryPointer(7L, "acct+legacy", "job%legacy"));
  }

  @Test
  void reconcileLeaseKeyPreservesLegacyRawAccountAndJobIds() {
    assertEquals(
        "/accounts/acct+legacy/reconcile/job-leases/by-id/job%legacy",
        Keys.reconcileJobLeasePointerById("acct+legacy", "job%legacy"));
  }

  @Test
  void snapshotIndexSidecarBlobUriUsesPathSafeEncoding() {
    assertEquals(
        "/accounts/acct%20id/tables/table%20id/index-sidecars/0000000000000000007/file%3As3%3A%2F%2Fb%2Fp%2Fa%20rquet/deadbeef.parquet",
        Keys.snapshotIndexSidecarBlobUri(
            "acct id", "table id", 7L, "file:s3://b/p/a rquet", "deadbeef"));
  }

  @Test
  void tableIdFromSnapshotPointerKeyCoversEverySnapshotPointerShape() {
    // A transaction touching ANY snapshot pointer must schedule a root resync, not only the
    // current-snapshot pointer. The extractor recovers the (percent-decoded) table id from all
    // of by-id, by-time, and current — and rejects unrelated keys.
    assertEquals(
        "tbl", Keys.tableIdFromSnapshotPointerKey(Keys.snapshotPointerById("a", "tbl", 3L)));
    assertEquals(
        "tbl", Keys.tableIdFromSnapshotPointerKey(Keys.snapshotPointerByTime("a", "tbl", 3L, 9L)));
    assertEquals(
        "tbl", Keys.tableIdFromSnapshotPointerKey(Keys.currentSnapshotPointerByTable("a", "tbl")));
    assertEquals(
        "t bl", Keys.tableIdFromSnapshotPointerKey(Keys.snapshotPointerById("a", "t bl", 3L)));
    assertEquals(null, Keys.tableIdFromSnapshotPointerKey(Keys.tableRootByTable("a", "tbl")));
    assertEquals(null, Keys.tableIdFromSnapshotPointerKey("/accounts/a/connectors/by-id/c"));
    assertEquals(null, Keys.tableIdFromSnapshotPointerKey(null));
  }

  @Test
  void ownerPointerKeyForBlobDerivesTheOwnerForEveryPointerRootedFamily() {
    // Ids with spaces exercise the percent-decode/re-encode round trip.
    assertEquals(
        Keys.accountPointerById("a c"),
        Keys.ownerPointerKeyForBlob(Keys.accountBlobUri("a c", "sha")));
    assertEquals(
        Keys.catalogPointerById("a c", "cat 1"),
        Keys.ownerPointerKeyForBlob(Keys.catalogBlobUri("a c", "cat 1", "sha")));
    assertEquals(
        Keys.namespacePointerById("a c", "ns 1"),
        Keys.ownerPointerKeyForBlob(Keys.namespaceBlobUri("a c", "ns 1", "sha")));
    assertEquals(
        Keys.tablePointerById("a c", "tbl 1"),
        Keys.ownerPointerKeyForBlob(Keys.tableBlobUri("a c", "tbl 1", "sha")));
    assertEquals(
        Keys.viewPointerById("a c", "v 1"),
        Keys.ownerPointerKeyForBlob(Keys.viewBlobUri("a c", "v 1", "sha")));
    assertEquals(
        Keys.connectorPointerById("a c", "con 1"),
        Keys.ownerPointerKeyForBlob(Keys.connectorBlobUri("a c", "con 1", "sha")));
    assertEquals(
        Keys.tableRootByTable("a c", "tbl 1"),
        Keys.ownerPointerKeyForBlob(Keys.tableRootBlobUri("a c", "tbl 1", "sha")));
    assertEquals(
        Keys.snapshotPointerById("a c", "tbl 1", 7L),
        Keys.ownerPointerKeyForBlob(Keys.snapshotBlobUri("a c", "tbl 1", 7L, "sha")));
    assertEquals(
        Keys.snapshotConstraintsPointer("a c", "tbl 1", 7L),
        Keys.ownerPointerKeyForBlob(Keys.snapshotConstraintsBlobUri("a c", "tbl 1", 7L, "sha")));
    assertEquals(
        Keys.snapshotTargetStatsManifestPointer("a c", "tbl 1", 7L),
        Keys.ownerPointerKeyForBlob(
            Keys.snapshotTargetStatsManifestBlobUri("a c", "tbl 1", 7L, "gen 1")));
    assertEquals(
        Keys.snapshotTargetStatsGenerationPointer("a c", "tbl 1", 7L, "gen 1", "tgt 1"),
        Keys.ownerPointerKeyForBlob(
            Keys.snapshotTargetStatsBlobUri("a c", "tbl 1", 7L, "gen 1", "tgt 1", "sha")));
    // Blob LISTs return keys without the leading slash — same derivation.
    assertEquals(
        Keys.tablePointerById("a c", "tbl 1"),
        Keys.ownerPointerKeyForBlob(Keys.tableBlobUri("a c", "tbl 1", "sha").substring(1)));
  }

  @Test
  void ownerPointerKeyForBlobIsNullWhereNoSingleOwnerIsDerivable() {
    // Root manifest pages are referenced from TableRoot blob content, not by any pointer.
    assertEquals(null, Keys.ownerPointerKeyForBlob(Keys.snapshotManifestBlobUri("a", "t", "sha")));
    // Per-target stats and file-stats blobs carry no snapshot id in the key, so their owning
    // /snapshots/<snapshot_id>/stats/... pointers cannot be derived.
    assertEquals(
        null, Keys.ownerPointerKeyForBlob(Keys.snapshotTargetStatsBlobUri("a", "t", "tgt", "sha")));
    assertEquals(
        null, Keys.ownerPointerKeyForBlob(Keys.snapshotFileStatsBlobUri("a", "t", "f", "sha")));
    assertEquals(null, Keys.ownerPointerKeyForBlob(null));
    assertEquals(null, Keys.ownerPointerKeyForBlob("/accounts/a"));
    assertEquals(null, Keys.ownerPointerKeyForBlob("/other/a/tables/t/table/sha.pb"));
    // Malformed (blank) segments degrade to "no owner", never throw.
    assertEquals(null, Keys.ownerPointerKeyForBlob("/accounts/%20/tables/t/table/sha.pb"));
  }
}
