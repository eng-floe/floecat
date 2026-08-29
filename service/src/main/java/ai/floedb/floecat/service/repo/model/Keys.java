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

import ai.floedb.floecat.storage.spi.PointerStoreKeys;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

public final class Keys {
  public static final int ACCOUNT_DELETION_FENCE_SHARDS = 64;
  private static final int DYNAMODB_TRANSACTION_ITEM_LIMIT = 100;

  static {
    if (ACCOUNT_DELETION_FENCE_SHARDS < 2
        || ACCOUNT_DELETION_FENCE_SHARDS + 1 > DYNAMODB_TRANSACTION_ITEM_LIMIT) {
      throw new ExceptionInInitializerError(
          "account deletion fence must have at least two shards and fit with its marker in one "
              + "DynamoDB transaction");
    }
  }

  public static final String SEG_ACCOUNT = "/account/";
  public static final String SEG_CATALOG = "/catalog/";
  public static final String SEG_NAMESPACE = "/namespace/";
  public static final String SEG_TABLE = "/table/";
  public static final String SEG_SNAPSHOTS = "/snapshots/";
  public static final String SEG_SNAPSHOT = "/snapshot/";
  public static final String SEG_TABLE_ROOT = "/root/";
  public static final String SEG_COMPAT = "/compat/";
  public static final String SEG_VIEW = "/view/";
  public static final String SEG_CONNECTOR = "/connector/";
  public static final String SEG_STORAGE_AUTHORITY = "/storage-authority/";
  public static final String SEG_TARGET_STATS = "/target-stats/";
  public static final String SEG_INDEX_ARTIFACTS = "/index-artifacts/";
  public static final String SEG_INDEX_SIDECARS = "/index-sidecars/";
  public static final String INDEX_ARTIFACT_DIRECT_GENERATION = "direct";
  public static final String INDEX_CAPTURE_MANIFEST_POINTER_FILE = "capture-manifest";
  public static final String INDEX_CAPTURE_MANIFEST_BLOB_DIRECTORY = "capture-manifests/";
  public static final String REUSABLE_ARTIFACT_INDEX_OBJECT_BLOB_DIRECTORY =
      "reusable-artifact-index/runs/";
  public static final String SEG_INDEX_CAPTURE_MANIFESTS =
      SEG_INDEX_ARTIFACTS + INDEX_CAPTURE_MANIFEST_BLOB_DIRECTORY;
  public static final String SUFFIX_INDEX_CAPTURE_MANIFEST_POINTER =
      SEG_INDEX_ARTIFACTS + INDEX_CAPTURE_MANIFEST_POINTER_FILE;
  public static final String SEG_CONSTRAINTS = "/constraints/";
  public static final String SEG_NAMESPACE_BY_PATH = "/namespaces/by-path/";
  public static final String SEG_TABLES_BY_NAME = "/tables/by-name/";
  public static final String SEG_VIEWS_BY_NAME = "/views/by-name/";
  public static final String SEG_STATS = "/stats/";
  public static final String SEG_IDEMPOTENCY = "/idempotency/";
  public static final String SEG_MARKERS = "/markers/";
  public static final String SEG_TRANSACTIONS = "/transactions/";
  public static final String SEG_CATALOG_INTEGRATION_CREDENTIAL_CLEANUP =
      "/catalog-integration-credential-cleanup/";

  private static String req(String name, String v) {
    if (v == null || v.isBlank()) {
      throw new IllegalArgumentException("key arg '" + name + "' is null/blank");
    }
    return v;
  }

  private static long reqNonNegative(String name, long v) {
    if (v < 0) {
      throw new IllegalArgumentException("key arg '" + name + "' must be >= 0");
    }
    return v;
  }

  private static long reqPositive(String name, long v) {
    if (v <= 0) {
      throw new IllegalArgumentException("key arg '" + name + "' must be > 0");
    }
    return v;
  }

  private static List<String> reqPath(String name, List<String> segs) {
    if (segs == null || segs.isEmpty()) {
      throw new IllegalArgumentException("key arg '" + name + "' is null/empty");
    }
    for (int i = 0; i < segs.size(); i++) {
      var s = segs.get(i);
      if (s == null || s.isBlank()) {
        throw new IllegalArgumentException(
            "key path '" + name + "' segment[" + i + "] is null/blank");
      }
    }
    return segs;
  }

  private static String encode(String s) {
    return ai.floedb.floecat.storage.kv.Keys.encodeSegment(
        Objects.requireNonNull(s, "encode value"));
  }

  public static String encodeSegment(String s) {
    return encode(s);
  }

  public static String catalogIntegrationCredentialCleanupPrefix() {
    return SEG_CATALOG_INTEGRATION_CREDENTIAL_CLEANUP;
  }

  public static String catalogIntegrationCredentialCleanupPointer(
      String accountId, String integrationId, long generation) {
    return SEG_CATALOG_INTEGRATION_CREDENTIAL_CLEANUP
        + encode(req("account_id", accountId))
        + "/"
        + encode(req("integration_id", integrationId))
        + "/"
        + reqPositive("generation", generation);
  }

  private static String joinPathSegments(List<String> segments) {
    if (segments == null) {
      throw new IllegalArgumentException("key arg 'segments' is null; use List.of()");
    }
    if (segments.isEmpty()) {
      return "";
    }
    String[] enc = new String[segments.size()];
    for (int i = 0; i < segments.size(); i++) {
      enc[i] = encode(req("segments[" + i + "]", segments.get(i)));
    }
    return String.join("/", enc);
  }

  // ===== Account =====

  public static String accountRootPointer(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid);
  }

  public static String accountPointerById(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/by-id/" + encode(tid);
  }

  public static String accountPointerByIdPrefix() {
    return "/accounts/by-id/";
  }

  public static String accountRootPrefix() {
    return "/accounts/";
  }

  public static boolean isReservedAccountDirectorySegment(String segment) {
    return "by-id".equals(segment) || "by-name".equals(segment);
  }

  public static String accountRootPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return accountRootPrefix() + encode(tid) + "/";
  }

  public static String accountBlobPrefix(String accountId) {
    return accountRootPrefix(accountId) + "account/";
  }

  public static String accountPointerByName(String displayName) {
    String name = req("display_name", displayName);
    return "/accounts/by-name/" + encode(name);
  }

  public static String accountPointerByNamePrefix() {
    return "/accounts/by-name/";
  }

  /** Durable state marker recording that account deletion has begun. */
  public static String accountDeletionMarker(String accountId) {
    return accountDeletionMarkerForEncodedSegment(encode(req("account_id", accountId)));
  }

  /** Builds the deletion fence when the account segment was extracted from an existing key. */
  public static String accountDeletionMarkerForEncodedSegment(String encodedAccountSegment) {
    String segment = req("encoded_account_segment", encodedAccountSegment);
    if (segment.indexOf('/') >= 0) {
      throw new IllegalArgumentException("encoded_account_segment must be one path segment");
    }
    return accountRootPrefix() + segment + "/deleting";
  }

  /**
   * One of the independent writer gates installed atomically when account deletion begins.
   *
   * <p>The gates live outside the account pointer prefix so teardown can sweep that prefix without
   * reopening writes. A writer checks only the shard selected by its primary pointer key; unrelated
   * writes therefore do not make every DynamoDB transaction read the same item.
   */
  public static String accountDeletionFenceShard(String accountId, String writeKey) {
    return accountDeletionFenceShardForEncodedSegment(
        encode(req("account_id", accountId)), writeKey);
  }

  public static String accountDeletionFenceShardForEncodedSegment(
      String encodedAccountSegment, String writeKey) {
    String segment = req("encoded_account_segment", encodedAccountSegment);
    if (segment.indexOf('/') >= 0) {
      throw new IllegalArgumentException("encoded_account_segment must be one path segment");
    }
    int hash = req("write_key", writeKey).hashCode();
    hash ^= hash >>> 16;
    return accountDeletionFenceShardKey(
        segment, Math.floorMod(hash, ACCOUNT_DELETION_FENCE_SHARDS));
  }

  public static List<String> accountDeletionFenceShards(String accountId) {
    String segment = encode(req("account_id", accountId));
    return java.util.stream.IntStream.range(0, ACCOUNT_DELETION_FENCE_SHARDS)
        .mapToObj(shard -> accountDeletionFenceShardKey(segment, shard))
        .toList();
  }

  private static String accountDeletionFenceShardKey(String encodedAccountSegment, int shard) {
    return PointerStoreKeys.ACCOUNT_DELETION_FENCE_PREFIX
        + encodedAccountSegment
        + "/"
        + String.format("%02d", shard);
  }

  public static String accountBlobUri(String accountId, String sha256) {
    String tid = req("account_id", accountId);
    String sha = req("sha256", sha256);
    return String.format("/accounts/%s/account/%s.pb", encode(tid), encode(sha));
  }

  // ===== Transactions =====

  public static String transactionPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/transactions/";
  }

  public static String transactionPointerById(String accountId, String txId) {
    String tid = req("account_id", accountId);
    String tx = req("tx_id", txId);
    return "/accounts/" + encode(tid) + "/transactions/by-id/" + encode(tx);
  }

  public static String transactionPointerByIdPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/transactions/by-id/";
  }

  public static String transactionBlobUri(String accountId, String txId, String sha256) {
    String tid = req("account_id", accountId);
    String tx = req("tx_id", txId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/transactions/%s/transaction/%s.pb", encode(tid), encode(tx), encode(sha));
  }

  public static String transactionBlobPrefix(String accountId, String txId) {
    String tid = req("account_id", accountId);
    String tx = req("tx_id", txId);
    return String.format("/accounts/%s/transactions/%s/transaction/", encode(tid), encode(tx));
  }

  public static String transactionDeleteSentinelUri(
      String accountId, String txId, String targetPointerKey) {
    String tid = req("account_id", accountId);
    String tx = req("tx_id", txId);
    String key = req("target_pointer_key", targetPointerKey);
    return String.format(
        "/accounts/%s/transactions/%s/delete/%s", encode(tid), encode(tx), encode(key));
  }

  public static String transactionIntentPointerByTarget(String accountId, String targetPointerKey) {
    String tid = req("account_id", accountId);
    String key = req("target_pointer_key", targetPointerKey);
    return "/accounts/" + encode(tid) + "/transactions/by-target/" + encode(key);
  }

  public static String transactionIntentPointerByTargetPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/transactions/by-target/";
  }

  public static String transactionIntentPointerByTx(
      String accountId, String txId, String targetPointerKey) {
    String tid = req("account_id", accountId);
    String tx = req("tx_id", txId);
    String key = req("target_pointer_key", targetPointerKey);
    return "/accounts/" + encode(tid) + "/transactions/" + encode(tx) + "/intents/" + encode(key);
  }

  public static String transactionIntentPointerByTxPrefix(String accountId, String txId) {
    String tid = req("account_id", accountId);
    String tx = req("tx_id", txId);
    return "/accounts/" + encode(tid) + "/transactions/" + encode(tx) + "/intents/";
  }

  public static String transactionIntentBlobUri(String accountId, String txId, String sha256) {
    String tid = req("account_id", accountId);
    String tx = req("tx_id", txId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/transactions/%s/intent/%s.pb", encode(tid), encode(tx), encode(sha));
  }

  public static String transactionIntentBlobPrefix(String accountId, String txId) {
    String tid = req("account_id", accountId);
    String tx = req("tx_id", txId);
    return String.format("/accounts/%s/transactions/%s/intent/", encode(tid), encode(tx));
  }

  public static String transactionObjectBlobUri(String accountId, String txId, String sha256) {
    String tid = req("account_id", accountId);
    String tx = req("tx_id", txId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/transactions/%s/objects/%s.bin", encode(tid), encode(tx), encode(sha));
  }

  public static String transactionObjectBlobPrefix(String accountId, String txId) {
    String tid = req("account_id", accountId);
    String tx = req("tx_id", txId);
    return String.format("/accounts/%s/transactions/%s/objects/", encode(tid), encode(tx));
  }

  // ===== Catalog =====

  public static String catalogPointerById(String accountId, String catalogId) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    return "/accounts/" + encode(tid) + "/catalogs/by-id/" + encode(cid);
  }

  public static String catalogPointerByIdPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/catalogs/by-id/";
  }

  public static String catalogRootPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/catalogs/";
  }

  public static String catalogPointerByName(String accountId, String displayName) {
    String tid = req("account_id", accountId);
    String name = req("display_name", displayName);
    return "/accounts/" + encode(tid) + "/catalogs/by-name/" + encode(name);
  }

  public static String catalogPointerByNamePrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/catalogs/by-name/";
  }

  public static String catalogBlobUri(String accountId, String catalogId, String sha256) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/catalogs/%s/catalog/%s.pb", encode(tid), encode(cid), encode(sha));
  }

  // ===== Storage Authority =====

  public static String storageAuthorityPointerById(String accountId, String authorityId) {
    String tid = req("account_id", accountId);
    String aid = req("authority_id", authorityId);
    return "/accounts/" + encode(tid) + "/storage-authorities/by-id/" + encode(aid);
  }

  public static String storageAuthorityPointerByIdPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/storage-authorities/by-id/";
  }

  public static String storageAuthorityRootPrefix(String accountId) {
    return "/accounts/" + encode(req("account_id", accountId)) + "/storage-authorities/";
  }

  public static String storageAuthorityPointerByName(String accountId, String displayName) {
    String tid = req("account_id", accountId);
    String name = req("display_name", displayName);
    return "/accounts/" + encode(tid) + "/storage-authorities/by-name/" + encode(name);
  }

  public static String storageAuthorityPointerByNamePrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/storage-authorities/by-name/";
  }

  public static String storageAuthorityBlobUri(
      String accountId, String authorityId, String sha256) {
    String tid = req("account_id", accountId);
    String aid = req("authority_id", authorityId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/storage-authorities/%s/storage-authority/%s.pb",
        encode(tid), encode(aid), encode(sha));
  }

  // ===== Namespace =====

  public static String namespacePointerById(String accountId, String namespaceId) {
    String tid = req("account_id", accountId);
    String nid = req("namespace_id", namespaceId);
    return "/accounts/" + encode(tid) + "/namespaces/by-id/" + encode(nid);
  }

  public static String casGcGenerationCursorPointer(String accountId) {
    return "/accounts/" + encode(req("account_id", accountId)) + "/gc/cas/generation-cursor";
  }

  public static String namespacePointerByIdPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/namespaces/by-id/";
  }

  public static String namespaceRootPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/namespaces/";
  }

  public static String namespacePointerByPath(
      String accountId, String catalogId, List<String> pathSegments) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String joined = joinPathSegments(reqPath("segments", pathSegments));
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/by-path/"
        + joined;
  }

  public static String namespacePointerByPathPrefix(
      String accountId, String catalogId, List<String> parentSegmentsOrEmpty) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    if (parentSegmentsOrEmpty == null)
      throw new IllegalArgumentException("key arg 'parent_segments' is null; use List.of()");
    String joined = joinPathSegments(parentSegmentsOrEmpty);
    String suffix = joined.isEmpty() ? "" : joined + "/";
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/by-path/"
        + suffix;
  }

  public static String namespaceBlobUri(String accountId, String namespaceId, String sha256) {
    String tid = req("account_id", accountId);
    String nid = req("namespace_id", namespaceId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/namespaces/%s/namespace/%s.pb", encode(tid), encode(nid), encode(sha));
  }

  // ===== Table =====

  public static String tablePointerById(String accountId, String tableId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return "/accounts/" + encode(tid) + "/tables/by-id/" + encode(tbid);
  }

  public static String tablePointerByIdPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/tables/by-id/";
  }

  public static String tableRootPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/tables/";
  }

  /** Immediate object-store prefix containing every blob family owned by one table. */
  public static String tableBlobPrefix(String accountId, String tableId) {
    return tableRootPrefix(accountId) + encode(req("table_id", tableId)) + "/";
  }

  public static String tablePointerByName(
      String accountId, String catalogId, String namespaceId, String tableName) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    String name = req("table_name", tableName);
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/tables/by-name/"
        + encode(name);
  }

  public static String tablePointerByNamePrefix(
      String accountId, String catalogId, String namespaceId) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/tables/by-name/";
  }

  /**
   * Shared, kind-agnostic relation-name claim pointer. Both tables and views reserve this pointer
   * for their (namespace, name), so a table and a view can never hold the same name in a namespace:
   * whichever is created second loses the atomic reservation. This is the source of truth for
   * cross-kind name uniqueness; the kind-specific {@code .../tables|views/by-name/} pointers remain
   * the lookup/listing indexes.
   */
  public static String relationPointerByName(
      String accountId, String catalogId, String namespaceId, String relationName) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    String name = req("relation_name", relationName);
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/relations/by-name/"
        + encode(name);
  }

  public static String tableBlobUri(String accountId, String tableId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/table/%s.pb", encode(tid), encode(tbid), encode(sha));
  }

  public static String tableDefinitionBlobPrefix(String accountId, String tableId) {
    return tableBlobPrefix(accountId, tableId) + "table/";
  }

  public static String tableSnapshotBlobPrefix(String accountId, String tableId) {
    return tableBlobPrefix(accountId, tableId) + "snapshots/";
  }

  public static String tableReusableArtifactIndexObjectBlobPrefix(
      String accountId, String tableId) {
    return tableBlobPrefix(accountId, tableId) + REUSABLE_ARTIFACT_INDEX_OBJECT_BLOB_DIRECTORY;
  }

  public static String tableConstraintsBlobPrefix(String accountId, String tableId) {
    return tableBlobPrefix(accountId, tableId) + "constraints/";
  }

  public static String tableRootBlobPrefix(String accountId, String tableId) {
    return tableBlobPrefix(accountId, tableId) + "root/";
  }

  // ===== Snapshot =====

  public static String snapshotPointerById(String accountId, String tableId, long snapshotId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return String.format(
        "/accounts/%s/tables/%s/snapshots/by-id/%019d", encode(tid), encode(tbid), sid);
  }

  public static String snapshotPointerByIdPrefix(String accountId, String tableId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return String.format("/accounts/%s/tables/%s/snapshots/by-id/", encode(tid), encode(tbid));
  }

  public static String snapshotRootPrefix(String accountId, String tableId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return String.format("/accounts/%s/tables/%s/snapshots/", encode(tid), encode(tbid));
  }

  public static String currentSnapshotPointerByTable(String accountId, String tableId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return String.format("/accounts/%s/tables/%s/snapshots/current", encode(tid), encode(tbid));
  }

  public static String currentSnapshotPointerBlobUri(
      String accountId, String tableId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/snapshots/current/%s.pb", encode(tid), encode(tbid), encode(sha));
  }

  /** The single CAS'd pointer to a table's current immutable {@code TableRoot}. */
  public static String tableRootByTable(String accountId, String tableId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return String.format("/accounts/%s/tables/%s/root/current", encode(tid), encode(tbid));
  }

  /** Content-addressed {@code TableRoot} blob (one per table commit). */
  public static String tableRootBlobUri(String accountId, String tableId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/root/%s.pb", encode(tid), encode(tbid), encode(sha));
  }

  /** Content-addressed snapshot-manifest page blob referenced from a {@code TableRoot}. */
  public static String snapshotManifestBlobPrefix(String accountId, String tableId) {
    return tableRootBlobPrefix(accountId, tableId) + "manifest/";
  }

  /** Content-addressed snapshot-manifest page blob referenced from a {@code TableRoot}. */
  public static String snapshotManifestBlobUri(String accountId, String tableId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/root/manifest/%s.pb", encode(tid), encode(tbid), encode(sha));
  }

  public static String snapshotPointerByTime(
      String accountId, String tableId, long snapshotId, long upstreamCreatedAtMs) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    long ts = reqNonNegative("upstream_created_at_ms", upstreamCreatedAtMs);
    long inverted = Long.MAX_VALUE - ts;
    long invertedSnapshotId = Long.MAX_VALUE - sid;
    return String.format(
        "/accounts/%s/tables/%s/snapshots/by-time/%019d-%019d",
        encode(tid), encode(tbid), inverted, invertedSnapshotId);
  }

  public static String snapshotPointerByTimePrefix(String accountId, String tableId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return String.format("/accounts/%s/tables/%s/snapshots/by-time/", encode(tid), encode(tbid));
  }

  /**
   * Recover the snapshot id from a by-time pointer key produced by {@link #snapshotPointerByTime}.
   * The trailing segment is the inverted snapshot id ({@code MAX_VALUE - snapshot_id}); this lets
   * an indexed by-time seek resolve the predecessor's id without fetching or parsing its blob.
   */
  public static long snapshotIdFromByTimeKey(String byTimeKey) {
    String key = req("by_time_key", byTimeKey);
    int dash = key.lastIndexOf('-');
    if (dash < 0 || dash + 1 >= key.length()) {
      throw new IllegalArgumentException("not a by-time snapshot key: " + byTimeKey);
    }
    long invertedSnapshotId = Long.parseLong(key.substring(dash + 1));
    return Long.MAX_VALUE - invertedSnapshotId;
  }

  public static String snapshotBlobUri(
      String accountId, String tableId, long snapshotId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/snapshots/%019d/snapshot/%s.pb",
        encode(tid), encode(tbid), sid, encode(sha));
  }

  // ===== Snapshot Stats =====

  private static String snapshotStatsRootPointer(
      String accountId, String tableId, long snapshotId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return String.format(
        "/accounts/%s/tables/%s/snapshots/%019d/stats/", encode(tid), encode(tbid), sid);
  }

  public static String snapshotStatsPrefix(String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId);
  }

  public static String snapshotTargetStatsManifestPointer(
      String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId) + "targets-active";
  }

  public static String snapshotTargetStatsGenerationRootPointer(
      String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId) + "target-generations/";
  }

  public static String snapshotTargetStatsGenerationDirectoryPointer(
      String accountId, String tableId, long snapshotId, String generationId) {
    return snapshotTargetStatsGenerationPointerPrefix(accountId, tableId, snapshotId, generationId)
        + "targets/";
  }

  public static String snapshotTargetStatsGenerationPointerPrefix(
      String accountId, String tableId, long snapshotId, String generationId) {
    String generation = req("generation_id", generationId);
    return snapshotTargetStatsGenerationRootPointer(accountId, tableId, snapshotId)
        + encode(generation)
        + "/";
  }

  public static String snapshotTargetStatsGenerationProtectionPointerPrefix(
      String accountId, String tableId, long snapshotId, String generationId, String protectionId) {
    return snapshotTargetStatsGenerationProtectionsPointerPrefix(
            accountId, tableId, snapshotId, generationId)
        + encode(req("protection_id", protectionId))
        + "/";
  }

  public static String snapshotTargetStatsGenerationProtectionsPointerPrefix(
      String accountId, String tableId, long snapshotId, String generationId) {
    return snapshotTargetStatsGenerationPointerPrefix(accountId, tableId, snapshotId, generationId)
        + "protections/";
  }

  public static String snapshotTargetStatsGenerationPointer(
      String accountId, String tableId, long snapshotId, String generationId, String targetId) {
    String target = req("target_id", targetId);
    return snapshotTargetStatsGenerationDirectoryPointer(
            accountId, tableId, snapshotId, generationId)
        + encode(target);
  }

  public static String snapshotTargetStatsGenerationPrefix(
      String accountId, String tableId, long snapshotId, String generationId) {
    return snapshotTargetStatsGenerationDirectoryPointer(
        accountId, tableId, snapshotId, generationId);
  }

  public static String snapshotTargetStatsGenerationLifecyclePointer(
      String accountId, String tableId, long snapshotId, String generationId) {
    return snapshotTargetStatsGenerationPointerPrefix(accountId, tableId, snapshotId, generationId)
        + "lifecycle";
  }

  /** The capture manifest whose immutable run index is this generation's shared file map. */
  public static String snapshotGenerationArtifactMapPointer(
      String accountId, String tableId, long snapshotId, String generationId) {
    return snapshotTargetStatsGenerationPointerPrefix(accountId, tableId, snapshotId, generationId)
        + "artifact-map";
  }

  public static String snapshotTargetStatsGenerationPublicationIntentPointer(
      String accountId, String tableId, long snapshotId, String generationId) {
    return snapshotTargetStatsGenerationPointerPrefix(accountId, tableId, snapshotId, generationId)
        + "publication-intent";
  }

  public static String snapshotTargetStatsGenerationPreparedFileGroupPointer(
      String accountId,
      String tableId,
      long snapshotId,
      String generationId,
      String jobId,
      String leaseEpoch) {
    return snapshotTargetStatsGenerationPointerPrefix(accountId, tableId, snapshotId, generationId)
        + "prepared-file-groups/"
        + encode(req("job_id", jobId))
        + "/"
        + sha256Hex(req("lease_epoch", leaseEpoch));
  }

  public static String snapshotTargetStatsDeletedGenerationFencePointer(
      String accountId, String tableId, long snapshotId, String generationId) {
    return accountPointerById(accountId)
        + "/reconcile/deleted-stats-generations/"
        + encode(req("table_id", tableId))
        + "/"
        + snapshotId
        + "/"
        + encode(req("generation_id", generationId));
  }

  public static String snapshotTargetColumnStatsGenerationPrefix(
      String accountId,
      String tableId,
      long snapshotId,
      String generationId,
      String columnTargetIdPrefix) {
    String prefix = req("column_target_id_prefix", columnTargetIdPrefix);
    return snapshotTargetStatsGenerationDirectoryPointer(
            accountId, tableId, snapshotId, generationId)
        + encode(prefix);
  }

  public static String snapshotTargetStatsManifestBlobUri(
      String accountId, String tableId, long snapshotId, String generationId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    String generation = req("generation_id", generationId);
    return String.format(
        "/accounts/%s/tables/%s/target-stats/%019d/manifests/%s.pb",
        encode(tid), encode(tbid), sid, encode(generation));
  }

  public static String snapshotTargetStatsBlobUri(
      String accountId,
      String tableId,
      long snapshotId,
      String generationId,
      String targetId,
      String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    String generation = req("generation_id", generationId);
    String target = req("target_id", targetId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/target-stats/%019d/generations/%s/%s/%s.pb",
        encode(tid), encode(tbid), sid, encode(generation), sha256Hex(target), encode(sha));
  }

  public static String snapshotTargetStatsGenerationBlobPrefix(
      String accountId, String tableId, long snapshotId, String generationId) {
    String generation = req("generation_id", generationId);
    return snapshotTargetStatsBlobPrefix(accountId, tableId, snapshotId)
        + "generations/"
        + encode(generation)
        + "/";
  }

  public static String snapshotTargetStatsBlobPrefix(
      String accountId, String tableId, long snapshotId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return String.format(
        "/accounts/%s/tables/%s/target-stats/%019d/", encode(tid), encode(tbid), sid);
  }

  public static String tableTargetStatsBlobPrefix(String accountId, String tableId) {
    return tableBlobPrefix(accountId, tableId) + "target-stats/";
  }

  public static String snapshotIndexArtifactDirectoryPointer(
      String accountId, String tableId, long snapshotId) {
    return String.format(
        "/accounts/%s/tables/%s/snapshots/%019d/index-artifacts/",
        encode(req("account_id", accountId)),
        encode(req("table_id", tableId)),
        reqNonNegative("snapshot_id", snapshotId));
  }

  public static String snapshotIndexArtifactActiveGenerationPointer(
      String accountId, String tableId, long snapshotId) {
    return snapshotIndexArtifactDirectoryPointer(accountId, tableId, snapshotId)
        + "active-generation";
  }

  public static String snapshotIndexArtifactCaptureManifestPointer(
      String accountId, String tableId, long snapshotId) {
    return snapshotIndexArtifactDirectoryPointer(accountId, tableId, snapshotId)
        + INDEX_CAPTURE_MANIFEST_POINTER_FILE;
  }

  public static String snapshotIndexArtifactCaptureManifestBlobPrefix(
      String accountId, String tableId, long snapshotId) {
    return snapshotIndexArtifactDirectoryPointer(accountId, tableId, snapshotId)
        + INDEX_CAPTURE_MANIFEST_BLOB_DIRECTORY;
  }

  public static String snapshotIndexArtifactCaptureManifestBlobUri(
      String accountId, String tableId, long snapshotId, String sha256) {
    return snapshotIndexArtifactCaptureManifestBlobPrefix(accountId, tableId, snapshotId)
        + encode(req("sha256", sha256))
        + ".pb";
  }

  public static String snapshotIndexArtifactGenerationPrefix(
      String accountId, String tableId, long snapshotId, String generationId) {
    return snapshotTargetStatsGenerationPointerPrefix(accountId, tableId, snapshotId, generationId)
        + "index-artifacts/";
  }

  public static String snapshotIndexArtifactGenerationBlobPrefix(
      String accountId, String tableId, long snapshotId, String generationId) {
    return snapshotTargetStatsGenerationBlobPrefix(accountId, tableId, snapshotId, generationId)
        + "index-artifacts/";
  }

  public static String snapshotIndexArtifactGenerationBlobUri(
      String accountId,
      String tableId,
      long snapshotId,
      String generationId,
      String targetId,
      String sha256) {
    return snapshotIndexArtifactGenerationBlobPrefix(accountId, tableId, snapshotId, generationId)
        + sha256Hex(req("target_id", targetId))
        + "/"
        + encode(req("sha256", sha256))
        + ".pb";
  }

  public static String snapshotIndexArtifactGenerationPointer(
      String accountId, String tableId, long snapshotId, String generationId, String targetId) {
    return snapshotIndexArtifactGenerationPrefix(accountId, tableId, snapshotId, generationId)
        + encode(req("target_id", targetId));
  }

  public static String snapshotIndexSidecarBlobUri(
      String accountId, String tableId, long snapshotId, String targetId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    String target = req("target_id", targetId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/index-sidecars/%019d/%s/%s.parquet",
        encode(tid), encode(tbid), sid, encode(target), encode(sha));
  }

  public static String tableIndexSidecarBlobPrefix(String accountId, String tableId) {
    return tableBlobPrefix(accountId, tableId) + SEG_INDEX_SIDECARS.substring(1);
  }

  public static String snapshotCompatDirectoryPointer(
      String accountId, String tableId, long snapshotId) {
    return String.format(
        "/accounts/%s/tables/%s/snapshots/%019d/compat/",
        encode(req("account_id", accountId)),
        encode(req("table_id", tableId)),
        reqNonNegative("snapshot_id", snapshotId));
  }

  public static String snapshotCompatIcebergRestPrefix(
      String accountId, String tableId, long snapshotId) {
    return snapshotCompatDirectoryPointer(accountId, tableId, snapshotId) + "iceberg-rest/";
  }

  public static String snapshotConstraintsStatsPointer(
      String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId) + "constraints";
  }

  public static String snapshotConstraintsPointer(
      String accountId, String tableId, long snapshotId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return String.format(
        "/accounts/%s/tables/%s/constraints/by-snapshot/%019d", encode(tid), encode(tbid), sid);
  }

  public static String snapshotConstraintsPointerPrefix(String accountId, String tableId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return String.format(
        "/accounts/%s/tables/%s/constraints/by-snapshot/", encode(tid), encode(tbid));
  }

  public static String snapshotConstraintsBlobUri(
      String accountId, String tableId, long snapshotId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/constraints/%019d/%s.pb",
        encode(tid), encode(tbid), sid, encode(sha));
  }

  public static String snapshotConstraintsBlobPrefix(
      String accountId, String tableId, long snapshotId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return String.format(
        "/accounts/%s/tables/%s/constraints/%019d/", encode(tid), encode(tbid), sid);
  }

  // ===== View =====

  public static String viewPointerById(String accountId, String viewId) {
    String tid = req("account_id", accountId);
    String vid = req("view_id", viewId);
    return "/accounts/" + encode(tid) + "/views/by-id/" + encode(vid);
  }

  public static String viewPointerByIdPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/views/by-id/";
  }

  public static String viewRootPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/views/";
  }

  public static String viewPointerByName(
      String accountId, String catalogId, String namespaceId, String viewName) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    String name = req("view_name", viewName);
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/views/by-name/"
        + encode(name);
  }

  public static String viewPointerByNamePrefix(
      String accountId, String catalogId, String namespaceId) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String nid = req("namespace_id", namespaceId);
    return "/accounts/"
        + encode(tid)
        + "/catalogs/"
        + encode(cid)
        + "/namespaces/"
        + encode(nid)
        + "/views/by-name/";
  }

  public static String viewBlobUri(String accountId, String viewId, String sha256) {
    String tid = req("account_id", accountId);
    String vid = req("view_id", viewId);
    String sha = req("sha256", sha256);
    return String.format("/accounts/%s/views/%s/view/%s.pb", encode(tid), encode(vid), encode(sha));
  }

  // ===== Connector =====

  public static String connectorPointerById(String accountId, String connectorId) {
    String tid = req("account_id", accountId);
    String cid = req("connector_id", connectorId);
    return "/accounts/" + encode(tid) + "/connectors/by-id/" + encode(cid);
  }

  public static String connectorPointerByIdPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/connectors/by-id/";
  }

  public static String connectorRootPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/connectors/";
  }

  public static String connectorPointerByName(String accountId, String displayName) {
    String tid = req("account_id", accountId);
    String name = req("display_name", displayName);
    return "/accounts/" + encode(tid) + "/connectors/by-name/" + encode(name);
  }

  public static String connectorPointerByNamePrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/connectors/by-name/";
  }

  public static String connectorBlobUri(String accountId, String connectorId, String sha256) {
    String tid = req("account_id", accountId);
    String cid = req("connector_id", connectorId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/connectors/%s/connector/%s.pb", encode(tid), encode(cid), encode(sha));
  }

  // ===== Catalog Integration =====

  public static String catalogIntegrationPointerById(String accountId, String integrationId) {
    String tid = req("account_id", accountId);
    String iid = req("integration_id", integrationId);
    return "/accounts/" + encode(tid) + "/catalog-integrations/by-id/" + encode(iid);
  }

  public static String catalogIntegrationPointerByIdPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/catalog-integrations/by-id/";
  }

  public static String catalogIntegrationRootPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/catalog-integrations/";
  }

  public static String catalogIntegrationPointerByName(String accountId, String displayName) {
    String tid = req("account_id", accountId);
    String name = req("display_name", displayName);
    return "/accounts/" + encode(tid) + "/catalog-integrations/by-name/" + encode(name);
  }

  public static String catalogIntegrationPointerByNamePrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/catalog-integrations/by-name/";
  }

  /** Fixed-key generation marker advanced by every overlay creation for this integration. */
  public static String catalogIntegrationOverlaysMarker(String accountId, String integrationId) {
    String tid = req("account_id", accountId);
    String iid = req("integration_id", integrationId);
    return "/accounts/" + encode(tid) + "/catalog-integrations/overlays-marker/" + encode(iid);
  }

  public static String catalogIntegrationDeletionMarker(String accountId, String integrationId) {
    String tid = req("account_id", accountId);
    String iid = req("integration_id", integrationId);
    return "/accounts/" + encode(tid) + "/catalog-integrations/deleting/" + encode(iid);
  }

  public static String catalogIntegrationBlobUri(
      String accountId, String integrationId, String sha256) {
    String tid = req("account_id", accountId);
    String iid = req("integration_id", integrationId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/catalog-integrations/%s/integration/%s.pb",
        encode(tid), encode(iid), encode(sha));
  }

  // ===== Catalog Overlay =====

  public static String catalogOverlayPointerById(String accountId, String overlayId) {
    String tid = req("account_id", accountId);
    String oid = req("overlay_id", overlayId);
    return "/accounts/" + encode(tid) + "/catalog-overlays/by-id/" + encode(oid);
  }

  public static String catalogOverlayPointerByIdPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/catalog-overlays/by-id/";
  }

  public static String catalogOverlayRootPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/catalog-overlays/";
  }

  public static String catalogOverlayPointerByName(String accountId, String displayName) {
    String tid = req("account_id", accountId);
    String name = req("display_name", displayName);
    return "/accounts/" + encode(tid) + "/catalog-overlays/by-name/" + encode(name);
  }

  public static String catalogOverlayPointerByNamePrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/catalog-overlays/by-name/";
  }

  public static String catalogOverlayPointerByIntegration(
      String accountId, String integrationId, String overlayId) {
    String tid = req("account_id", accountId);
    String iid = req("integration_id", integrationId);
    String oid = req("overlay_id", overlayId);
    return "/accounts/"
        + encode(tid)
        + "/catalog-overlays/by-integration/"
        + encode(iid)
        + "/"
        + encode(oid);
  }

  public static String catalogOverlayPointerByIntegrationPrefix(
      String accountId, String integrationId) {
    String tid = req("account_id", accountId);
    String iid = req("integration_id", integrationId);
    return "/accounts/" + encode(tid) + "/catalog-overlays/by-integration/" + encode(iid) + "/";
  }

  public static String catalogOverlayPointerByCatalog(
      String accountId, String catalogId, String overlayId) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    String oid = req("overlay_id", overlayId);
    return "/accounts/"
        + encode(tid)
        + "/catalog-overlays/by-catalog/"
        + encode(cid)
        + "/"
        + encode(oid);
  }

  public static String catalogOverlayPointerByCatalogPrefix(String accountId, String catalogId) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    return "/accounts/" + encode(tid) + "/catalog-overlays/by-catalog/" + encode(cid) + "/";
  }

  /** Fixed-key generation marker advanced by every overlay attachment to this catalog. */
  public static String catalogOverlaysMarker(String accountId, String catalogId) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    return "/accounts/" + encode(tid) + "/catalogs/overlays-marker/" + encode(cid);
  }

  public static String catalogOverlayDeletionMarker(String accountId, String overlayId) {
    String tid = req("account_id", accountId);
    String oid = req("overlay_id", overlayId);
    return "/accounts/" + encode(tid) + "/catalog-overlays/deleting/" + encode(oid);
  }

  public static String catalogOverlayBlobUri(String accountId, String overlayId, String sha256) {
    String tid = req("account_id", accountId);
    String oid = req("overlay_id", overlayId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/catalog-overlays/%s/overlay/%s.pb", encode(tid), encode(oid), encode(sha));
  }

  // ===== Idempotency =====

  public static String idempotencyKey(String accountId, String operation, String key) {
    String tid = req("account_id", accountId);
    String op = req("operation", operation);
    String k = req("key", key);
    return "/accounts/" + encode(tid) + "/idempotency/" + encode(op) + "/" + encode(k);
  }

  public static String idempotencyBlobUri(String accountId, String key) {
    String tid = req("account_id", accountId);
    String k = req("key", key);
    return "/accounts/" + encode(tid) + "/idempotency/" + encode(k) + "/idempotency.pb";
  }

  public static String idempotencyBlobUri(String accountId, String key, String suffix) {
    String tid = req("account_id", accountId);
    String k = req("key", key);
    String s = req("suffix", suffix);
    return "/accounts/"
        + encode(tid)
        + "/idempotency/"
        + encode(k)
        + "/idempotency-"
        + encode(s)
        + ".pb";
  }

  public static String idempotencyBlobPrefix(String accountId, String key) {
    String tid = req("account_id", accountId);
    String k = req("key", key);
    return "/accounts/" + encode(tid) + "/idempotency/" + encode(k) + "/";
  }

  public static String idempotencyBlobPrefixForPointerKey(String pointerKey) {
    String k = req("pointer_key", pointerKey);
    String normalized = k.startsWith("/") ? k : "/" + k;
    int accountsIdx = normalized.indexOf("/accounts/");
    if (accountsIdx < 0) {
      throw new IllegalArgumentException("pointer key missing /accounts/ segment");
    }
    int start = accountsIdx + "/accounts/".length();
    int idempIdx = normalized.indexOf("/idempotency/", start);
    if (idempIdx < 0) {
      throw new IllegalArgumentException("pointer key missing /idempotency/ segment");
    }
    String accountEncoded = normalized.substring(start, idempIdx);
    return "/accounts/" + accountEncoded + "/idempotency/" + encode(normalized) + "/";
  }

  public static String idempotencyPrefixAccount(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/idempotency/";
  }

  // ===== Reconcile Jobs =====

  public static String reconcileJobStateRowById(String accountId, String jobId) {
    return reconcileJobPointerById(accountId, jobId);
  }

  public static String reconcileJobStateRowByIdPrefix(String accountId) {
    return reconcileJobPointerByIdPrefix(accountId);
  }

  public static String reconcileJobPointerById(String accountId, String jobId) {
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/by-id/" + encode(jid);
  }

  public static String reconcileJobPointerByIdPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/by-id/";
  }

  public static String reconcileJobLookupPointerById(String jobId) {
    String jid = req("job_id", jobId);
    return "/accounts/by-id/reconcile/jobs/by-id/" + encode(jid);
  }

  public static String reconcileJobLookupPointerByIdPrefix() {
    return "/accounts/by-id/reconcile/jobs/by-id/";
  }

  public static String reconcileFinalizedSnapshotIdentityPointer(
      String accountId, String tableId, long snapshotId) {
    String tid = req("account_id", accountId);
    String table = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return "/accounts/"
        + encode(tid)
        + "/reconcile/finalized-snapshots/by-id/"
        + encode(table)
        + "/"
        + String.format("%019d", sid);
  }

  public static String reconcileDirtyParentPointerRootPrefix() {
    // This namespace deliberately does not descend from the legacy "dirty-parents/" prefix.
    // Older deployments scan that entire prefix and delete markers whose payload schema they do
    // not understand.
    return "/accounts/by-id/reconcile/jobs/dirty-parents-by-worker-affinity/";
  }

  public static String reconcileDirtyParentPointerPrefix(String workerAffinity) {
    String affinity = req("worker_affinity", workerAffinity);
    return reconcileDirtyParentPointerRootPrefix() + encode(affinity) + "/";
  }

  public static String reconcileDirtyParentPointer(
      String workerAffinity, String accountId, String parentJobId) {
    String affinity = req("worker_affinity", workerAffinity);
    String tid = req("account_id", accountId);
    String pid = req("parent_job_id", parentJobId);
    return reconcileDirtyParentPointerPrefix(affinity) + encode(tid) + "/" + encode(pid);
  }

  public static String reconcileCancellationCleanupPointerPrefix() {
    return "/accounts/by-id/reconcile/jobs/cancellation-cleanup/";
  }

  public static String reconcileCancellationCleanupPointer(String accountId, String rootJobId) {
    String tid = req("account_id", accountId);
    String jid = req("root_job_id", rootJobId);
    return reconcileCancellationCleanupPointerPrefix() + encode(tid) + "/" + encode(jid);
  }

  public static String reconcileJobProjectionPointer(String accountId, String jobId) {
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/projections/by-id/" + encode(jid);
  }

  public static String reconcileJobProjectionPointerPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/projections/by-id/";
  }

  public static String reconcileRootJobSummaryByAccountPointer(
      String accountId, String sortableJobToken) {
    String tid = req("account_id", accountId);
    String token = req("sortable_job_token", sortableJobToken);
    return "/accounts/"
        + encode(tid)
        + "/reconcile/jobs/root-summaries/by-account/"
        + encode(token);
  }

  public static String reconcileRootJobSummaryByAccountPointerPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/root-summaries/by-account/";
  }

  public static String reconcileRootJobSummaryByConnectorPointer(
      String accountId, String connectorId, String sortableJobToken) {
    String tid = req("account_id", accountId);
    String cid = req("connector_id", connectorId);
    String token = req("sortable_job_token", sortableJobToken);
    return "/accounts/"
        + encode(tid)
        + "/reconcile/jobs/root-summaries/by-connector/"
        + encode(cid)
        + "/"
        + encode(token);
  }

  public static String reconcileRootJobSummaryByConnectorPointerPrefix(
      String accountId, String connectorId) {
    String tid = req("account_id", accountId);
    String cid = req("connector_id", connectorId);
    return "/accounts/"
        + encode(tid)
        + "/reconcile/jobs/root-summaries/by-connector/"
        + encode(cid)
        + "/";
  }

  public static String reconcileRootJobSummaryByConnectorAccountPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/root-summaries/by-connector/";
  }

  public static String reconcileCanonicalQuarantinePointer(
      String accountId, String canonicalKeyHash) {
    String tid = req("account_id", accountId);
    String hash = req("canonical_key_hash", canonicalKeyHash);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/gc-quarantine/canonical/" + encode(hash);
  }

  public static String reconcileCanonicalQuarantinePointerPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/gc-quarantine/canonical/";
  }

  public static String reconcileJobByParentPointer(
      String accountId, String parentJobId, String jobId) {
    String tid = req("account_id", accountId);
    String pid = req("parent_job_id", parentJobId);
    String jid = req("job_id", jobId);
    return "/accounts/"
        + encode(tid)
        + "/reconcile/jobs/by-parent/"
        + encode(pid)
        + "/"
        + encode(jid);
  }

  public static String reconcileJobByParentPointerPrefix(String accountId, String parentJobId) {
    String tid = req("account_id", accountId);
    String pid = req("parent_job_id", parentJobId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/by-parent/" + encode(pid) + "/";
  }

  public static String reconcileJobByConnectorPointer(
      String accountId, String connectorId, String sortableJobToken) {
    String tid = req("account_id", accountId);
    String cid = req("connector_id", connectorId);
    String token = req("sortable_job_token", sortableJobToken);
    return "/accounts/"
        + encode(tid)
        + "/reconcile/jobs/by-connector/"
        + encode(cid)
        + "/"
        + encode(token);
  }

  public static String reconcileJobByConnectorPointerPrefix(String accountId, String connectorId) {
    String tid = req("account_id", accountId);
    String cid = req("connector_id", connectorId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/by-connector/" + encode(cid) + "/";
  }

  public static String reconcileJobByStatePointerPrefix() {
    return "/accounts/by-id/reconcile/jobs/by-state/";
  }

  public static String reconcileJobByStatePointerPrefix(String state) {
    String jobState = req("state", state);
    return reconcileJobByStatePointerPrefix() + encode(jobState) + "/";
  }

  public static String reconcileJobByStatePointer(
      String state, long sortableTimestampMs, String accountId, String jobId) {
    String jobState = req("state", state);
    long ts = reqNonNegative("sortable_timestamp_ms", sortableTimestampMs);
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    return String.format(
        "%s%019d/%s/%s", reconcileJobByStatePointerPrefix(jobState), ts, encode(tid), encode(jid));
  }

  public static String reconcileJobByAccountStatePointerPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/by-state/";
  }

  public static String reconcileJobByAccountStatePointerPrefix(String accountId, String state) {
    String jobState = req("state", state);
    return reconcileJobByAccountStatePointerPrefix(accountId) + encode(jobState) + "/";
  }

  public static String reconcileJobByAccountStatePointer(
      String accountId, String state, long sortableTimestampMs, String jobId) {
    long ts = reqNonNegative("sortable_timestamp_ms", sortableTimestampMs);
    String jid = req("job_id", jobId);
    return String.format(
        "%s%019d/%s", reconcileJobByAccountStatePointerPrefix(accountId, state), ts, encode(jid));
  }

  public static String reconcileTerminalRetentionPointerPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/terminal-retention/";
  }

  public static String reconcileTerminalRetentionPointer(
      String accountId, long terminalAtMs, String jobId) {
    String jid = req("job_id", jobId);
    long ts = reqNonNegative("terminal_at_ms", terminalAtMs);
    return String.format(
        "%s%019d/%s", reconcileTerminalRetentionPointerPrefix(accountId), ts, encode(jid));
  }

  public static String reconcileJobByConnectorStatePointerPrefix(
      String accountId, String connectorId) {
    String tid = req("account_id", accountId);
    String cid = req("connector_id", connectorId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/by-connector-state/" + encode(cid) + "/";
  }

  public static String reconcileJobByConnectorStatePointerPrefix(
      String accountId, String connectorId, String state) {
    String jobState = req("state", state);
    return reconcileJobByConnectorStatePointerPrefix(accountId, connectorId)
        + encode(jobState)
        + "/";
  }

  public static String reconcileJobByConnectorStatePointer(
      String accountId, String connectorId, String state, long sortableTimestampMs, String jobId) {
    long ts = reqNonNegative("sortable_timestamp_ms", sortableTimestampMs);
    String jid = req("job_id", jobId);
    return String.format(
        "%s%019d/%s",
        reconcileJobByConnectorStatePointerPrefix(accountId, connectorId, state), ts, encode(jid));
  }

  public static String reconcileJobBlobUri(String accountId, String jobId, String suffix) {
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    String s = req("suffix", suffix);
    return "/accounts/"
        + encode(tid)
        + "/reconcile/jobs/"
        + encode(jid)
        + "/job-"
        + encode(s)
        + ".json";
  }

  public static String reconcileJobLeasePointerById(String accountId, String jobId) {
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    return reconcileJobLeasePointerByIdPrefix(tid) + encode(jid);
  }

  public static String reconcileJobLeasePointerByIdPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return accountRootPrefix(tid) + "reconcile/job-leases/by-id/";
  }

  public static String reconcileJobLeaseExpiryPointerPrefix() {
    return "/accounts/by-id/reconcile/job-leases/by-expiry/";
  }

  public static String reconcileJobLeaseExpiryPointer(
      long expiresAtMs, String accountId, String jobId) {
    long expiresAt = reqPositive("expires_at_ms", expiresAtMs);
    return reconcileJobLeaseExpiryPointerPrefix()
        + String.format("%019d", expiresAt)
        + reconcileJobLeaseExpiryPointerSuffix(accountId, jobId);
  }

  public static String reconcileJobLeaseExpiryPointerSuffix(String accountId, String jobId) {
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    return "/accounts/" + encode(tid) + "/jobs/" + encode(jid);
  }

  public static String reconcileJobResultBlobUri(String accountId, String jobId, String suffix) {
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    String s = req("suffix", suffix);
    return "/accounts/"
        + encode(tid)
        + "/reconcile/jobs/"
        + encode(jid)
        + "/result-"
        + encode(s)
        + ".json";
  }

  public static String reconcileFileGroupResultPayloadUri(
      String accountId, String parentJobId, String jobId, String leaseEpoch) {
    String tid = req("account_id", accountId);
    String pid = req("parent_job_id", parentJobId);
    String jid = req("job_id", jobId);
    String epoch = req("lease_epoch", leaseEpoch);
    return reconcileJobBlobPrefix(tid, jid)
        + "result-payloads/v1/snapshot-plans/"
        + encode(pid)
        + "/executions/"
        + sha256Hex(epoch)
        + ".pb";
  }

  public static String reconcileFileGroupStatsObjectPrefix(
      String accountId,
      String tableId,
      long snapshotId,
      String parentJobId,
      String jobId,
      String leaseEpoch) {
    String generationId = "full-rescan-" + req("parent_job_id", parentJobId);
    return snapshotTargetStatsGenerationBlobPrefix(accountId, tableId, snapshotId, generationId)
        + "worker-uploads/"
        + encode(req("job_id", jobId))
        + "/"
        + sha256Hex(req("lease_epoch", leaseEpoch))
        + "/";
  }

  public static String reconcileSnapshotFinalizeStatsObjectPrefix(
      String accountId, String tableId, long snapshotId, String parentJobId) {
    String generationId = "full-rescan-" + req("parent_job_id", parentJobId);
    return snapshotTargetStatsGenerationBlobPrefix(accountId, tableId, snapshotId, generationId)
        + "finalizer-outputs/";
  }

  public static String reconcileSnapshotDurableCaptureManifestUri(
      String accountId,
      String tableId,
      long snapshotId,
      String parentJobId,
      byte[] manifestSha256) {
    if (manifestSha256 == null || manifestSha256.length != 32) {
      throw new IllegalArgumentException("manifest_sha256 must contain 32 bytes");
    }
    return reconcileSnapshotFinalizeStatsObjectPrefix(accountId, tableId, snapshotId, parentJobId)
        + "reuse-manifests/"
        + HexFormat.of().formatHex(manifestSha256)
        + ".pb";
  }

  private static String sha256Hex(String value) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  public static String reconcileReadyPointerPrefix() {
    // Keep ready-queue pointers in the global account directory partition so cross-account
    // schedulers can scan due jobs while still satisfying backends that require /accounts/* keys.
    return "/accounts/by-id/reconcile/jobs/ready/";
  }

  public static String reconcileReadyPointerByDue(
      long dueAtMs, String accountId, String laneKey, String jobId) {
    long due = reqNonNegative("due_at_ms", dueAtMs);
    String tid = req("account_id", accountId);
    String lane = req("lane_key", laneKey);
    String jid = req("job_id", jobId);
    return String.format(
        "/accounts/by-id/reconcile/jobs/ready/%019d/%s/%s/%s",
        due, encode(tid), encode(lane), encode(jid));
  }

  public static String reconcileReadyByExecutionClassPointerPrefix() {
    return "/accounts/by-id/reconcile/jobs/ready/by-execution-class/";
  }

  public static String reconcileReadyByExecutionClassPointerPrefix(String executionClass) {
    String executionClassValue = req("execution_class", executionClass);
    return reconcileReadyByExecutionClassPointerPrefix() + encode(executionClassValue) + "/";
  }

  public static String reconcileReadyByExecutionClassPointerByDue(
      long dueAtMs, String executionClass, String accountId, String jobId) {
    long due = reqNonNegative("due_at_ms", dueAtMs);
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    return String.format(
        "%s%019d/%s/%s",
        reconcileReadyByExecutionClassPointerPrefix(executionClass), due, encode(tid), encode(jid));
  }

  public static String reconcileReadyByExecutionLanePointerPrefix() {
    return "/accounts/by-id/reconcile/jobs/ready/by-execution-lane/";
  }

  public static String reconcileReadyByExecutionLanePointerPrefix(String executionLane) {
    String executionLaneValue = req("execution_lane", executionLane);
    return reconcileReadyByExecutionLanePointerPrefix() + encode(executionLaneValue) + "/";
  }

  public static String reconcileReadyByExecutionLanePointerByDue(
      long dueAtMs, String executionLane, String accountId, String jobId) {
    long due = reqNonNegative("due_at_ms", dueAtMs);
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    return String.format(
        "%s%019d/%s/%s",
        reconcileReadyByExecutionLanePointerPrefix(executionLane), due, encode(tid), encode(jid));
  }

  public static String reconcileReadyByPinnedExecutorPointerPrefix() {
    return "/accounts/by-id/reconcile/jobs/ready/by-pinned-executor/";
  }

  public static String reconcileReadyByPinnedExecutorPointerPrefix(String pinnedExecutorId) {
    String pinnedExecutorValue = req("pinned_executor_id", pinnedExecutorId);
    return reconcileReadyByPinnedExecutorPointerPrefix() + encode(pinnedExecutorValue) + "/";
  }

  public static String reconcileReadyByPinnedExecutorPointerByDue(
      long dueAtMs, String pinnedExecutorId, String accountId, String jobId) {
    long due = reqNonNegative("due_at_ms", dueAtMs);
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    return String.format(
        "%s%019d/%s/%s",
        reconcileReadyByPinnedExecutorPointerPrefix(pinnedExecutorId),
        due,
        encode(tid),
        encode(jid));
  }

  public static String reconcileReadyByJobKindPointerPrefix() {
    return "/accounts/by-id/reconcile/jobs/ready/by-job-kind/";
  }

  public static String reconcileReadyByJobKindPointerPrefix(String jobKind) {
    String jobKindValue = req("job_kind", jobKind);
    return reconcileReadyByJobKindPointerPrefix() + encode(jobKindValue) + "/";
  }

  public static String reconcileReadyByJobKindPointerByDue(
      long dueAtMs, String jobKind, String accountId, String jobId) {
    long due = reqNonNegative("due_at_ms", dueAtMs);
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    return String.format(
        "%s%019d/%s/%s",
        reconcileReadyByJobKindPointerPrefix(jobKind), due, encode(tid), encode(jid));
  }

  public static String reconcileDedupePointer(String accountId, String dedupeKeyHash) {
    String tid = req("account_id", accountId);
    String hash = req("dedupe_key_hash", dedupeKeyHash);
    return "/accounts/" + encode(tid) + "/reconcile/dedupe/" + encode(hash);
  }

  public static String reconcileDedupePointerPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/reconcile/dedupe/";
  }

  public static String reconcileSnapshotOwnershipPointer(
      String accountId, String tableId, long snapshotId) {
    String aid = req("account_id", accountId);
    String tid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return "/accounts/"
        + encode(aid)
        + "/reconcile/snapshot-owners/"
        + encode(tid)
        + "/"
        + String.format("%019d", sid);
  }

  public static String reconcileSnapshotCoverageClaimPointer(
      String accountId,
      String connectorId,
      String sourceNamespace,
      String sourceTable,
      String tableId,
      long snapshotId,
      String sourceRevision,
      String semanticsHash) {
    return "/accounts/"
        + encode(req("account_id", accountId))
        + "/reconcile/snapshot-coverage-claims/"
        + encode(req("connector_id", connectorId))
        + "/"
        + encode(req("source_namespace", sourceNamespace))
        + "/"
        + encode(req("source_table", sourceTable))
        + "/"
        + encode(req("table_id", tableId))
        + "/"
        + String.format("%019d", reqNonNegative("snapshot_id", snapshotId))
        + "/"
        + encode(req("source_revision", sourceRevision))
        + "/"
        + encode(req("semantics_hash", semanticsHash));
  }

  public static String reconcileSnapshotCoverageClaimPointerPrefix(String accountId) {
    return "/accounts/"
        + encode(req("account_id", accountId))
        + "/reconcile/snapshot-coverage-claims/";
  }

  public static String reconcileLaneLeasePointer(String accountId, String laneKey) {
    String tid = req("account_id", accountId);
    String lane = req("lane_key", laneKey);
    return "/accounts/" + encode(tid) + "/reconcile/lanes/" + encode(lane);
  }

  public static String reconcileJobBlobPrefix(String accountId, String jobId) {
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/" + encode(jid) + "/";
  }

  public static String reconcileJobBlobCleanupPointer(String accountId, String jobId) {
    String tid = req("account_id", accountId);
    String jid = req("job_id", jobId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/gc-blob-cleanup/" + encode(jid);
  }

  public static String reconcileJobBlobCleanupPointerPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return "/accounts/" + encode(tid) + "/reconcile/jobs/gc-blob-cleanup/";
  }

  /**
   * Recovers the table id from ANY snapshot-scoped pointer key ({@code
   * /accounts/{a}/tables/{t}/snapshots/...} — by-id, by-time, current, stats), or {@code null} when
   * the key has another shape. Used so a transaction touching any snapshot pointer schedules a root
   * resync, not only the current-snapshot pointer.
   */
  public static String tableIdFromSnapshotPointerKey(String pointerKey) {
    if (pointerKey == null) {
      return null;
    }
    int start = pointerKey.indexOf("/tables/");
    int end = pointerKey.indexOf("/snapshots/");
    if (start < 0 || end <= start + "/tables/".length()) {
      return null;
    }
    String encoded = pointerKey.substring(start + "/tables/".length(), end);
    return encoded.isBlank() ? null : percentDecode(encoded);
  }

  /**
   * Parses a per-target stats-generation pointer key produced by {@link
   * #snapshotTargetStatsGenerationPointer} into its (snapshot id, generation id), or {@code null}
   * when the key has another shape.
   */
  public static GenerationKey generationFromTargetPointerKey(String pointerKey) {
    if (pointerKey == null) {
      return null;
    }
    String marker = "/stats/target-generations/";
    int at = pointerKey.indexOf(marker);
    if (at < 0) {
      return null;
    }
    int sidStart = pointerKey.lastIndexOf('/', at - 1) + 1;
    long snapshotId;
    try {
      snapshotId = Long.parseLong(pointerKey.substring(sidStart, at));
    } catch (RuntimeException e) {
      return null;
    }
    int genStart = at + marker.length();
    int genEnd = pointerKey.indexOf('/', genStart);
    if (genEnd < 0) {
      return null;
    }
    return new GenerationKey(snapshotId, percentDecode(pointerKey.substring(genStart, genEnd)));
  }

  /** Recovers a generation identity from a generation-manifest blob URI. */
  public static GenerationKey generationFromManifestBlobUri(String manifestUri) {
    if (manifestUri == null) {
      return null;
    }
    String marker = "/target-stats/";
    int markerAt = manifestUri.indexOf(marker);
    if (markerAt < 0) {
      return null;
    }
    int snapshotStart = markerAt + marker.length();
    int snapshotEnd = manifestUri.indexOf("/manifests/", snapshotStart);
    if (snapshotEnd < 0) {
      return null;
    }
    long snapshotId;
    try {
      snapshotId = Long.parseLong(manifestUri.substring(snapshotStart, snapshotEnd));
    } catch (RuntimeException e) {
      return null;
    }
    int generationStart = snapshotEnd + "/manifests/".length();
    int generationEnd = manifestUri.endsWith(".pb") ? manifestUri.length() - 3 : -1;
    if (generationEnd <= generationStart || manifestUri.indexOf('/', generationStart) >= 0) {
      return null;
    }
    return new GenerationKey(
        snapshotId, percentDecode(manifestUri.substring(generationStart, generationEnd)));
  }

  /** Recovers a generation identity from a generation-owned target-stats blob URI. */
  public static GenerationKey generationFromTargetStatsBlobUri(String blobUri) {
    if (blobUri == null) {
      return null;
    }
    String marker = "/target-stats/";
    int markerAt = blobUri.indexOf(marker);
    if (markerAt < 0) {
      return null;
    }
    int snapshotStart = markerAt + marker.length();
    int snapshotEnd = blobUri.indexOf("/generations/", snapshotStart);
    if (snapshotEnd < 0) {
      return null;
    }
    long snapshotId;
    try {
      snapshotId = Long.parseLong(blobUri.substring(snapshotStart, snapshotEnd));
    } catch (RuntimeException e) {
      return null;
    }
    int generationStart = snapshotEnd + "/generations/".length();
    int generationEnd = blobUri.indexOf('/', generationStart);
    if (generationEnd <= generationStart) {
      return null;
    }
    return new GenerationKey(
        snapshotId, percentDecode(blobUri.substring(generationStart, generationEnd)));
  }

  /** One stats generation's identity within a table, as encoded in its pointer keys. */
  public record GenerationKey(long snapshotId, String generationId) {}

  // ===== Root resync re-drive =====

  /**
   * Durable marker: this table's post-transaction root resync failed and awaits re-drive by the
   * periodic transaction GC. A table only ever touched by REST transactions has no other writer to
   * converge its root, so the failure must leave a durable trace.
   */
  public static String rootResyncPendingPointer(String accountId, String tableId) {
    return rootResyncPendingPrefix(accountId) + encode(req("table_id", tableId));
  }

  public static String rootResyncPendingPrefix(String accountId) {
    return "/accounts/" + encode(req("account_id", accountId)) + "/root-resyncs/by-table/";
  }

  // ===== Markers =====

  public static String catalogChildrenMarker(String accountId, String catalogId) {
    String tid = req("account_id", accountId);
    String cid = req("catalog_id", catalogId);
    return "/accounts/" + encode(tid) + "/catalogs/" + encode(cid) + "/markers/children";
  }

  public static String namespaceChildrenMarker(String accountId, String namespaceId) {
    String tid = req("account_id", accountId);
    String nid = req("namespace_id", namespaceId);
    return "/accounts/" + encode(tid) + "/namespaces/" + encode(nid) + "/markers/children";
  }

  /**
   * Extracts the resource ID string from a blob URI following the standard pattern {@code
   * /accounts/{accountId}/{type-plural}/{resourceId}/...}. Returns empty string if the URI does not
   * match. Used as a fallback for legacy pointers that predate the {@code Pointer.resource_id}
   * field.
   *
   * <p>Example: {@code /accounts/x/tables/my-table-id/table/sha.pb} → {@code my-table-id}
   */
  public static String extractResourceIdFromBlobUri(String blobUri) {
    if (blobUri == null || blobUri.isEmpty()) {
      return "";
    }
    // Pattern: /accounts/{accountId}/{type}/{resourceId}/...
    int start = 0;
    int slashCount = 0;
    for (int i = 0; i < blobUri.length(); i++) {
      if (blobUri.charAt(i) == '/') {
        slashCount++;
        if (slashCount == 4) {
          start = i + 1;
        } else if (slashCount == 5) {
          return percentDecode(blobUri.substring(start, i));
        }
      }
    }
    return "";
  }

  /**
   * The pointer key that owns a content-addressed blob, derived from the blob key's shape, or
   * {@code null} when no single owning pointer is derivable. Accepts keys with or without the
   * leading slash (blob LISTs return them unslashed). Used by {@code CasBlobGc} to re-check, right
   * before deleting a candidate, that no pointer CAS re-targeted it after the mark phase.
   *
   * <p>Not derivable — {@code null} — for:
   *
   * <ul>
   *   <li>root manifest pages ({@code .../root/manifest/<sha>.pb}): referenced by the {@code
   *       TableRoot} blob's content, not by any pointer;
   *   <li>generation-scoped target stats records ({@code
   *       .../target-stats/<snapshot_id>/generations/<gen>/<target-hash>/<sha>.pb}): the bounded
   *       target hash does not encode the pointer's target id, so liveness is re-proven by the
   *       table-scoped stats-pointer re-mark instead;
   *   <li>generation-scoped index wrappers ({@code
   *       .../generations/<gen>/index-artifacts/<target-hash>/<sha>.pb}): the bounded target hash
   *       does not encode the pointer's target id, so liveness is re-proven by the table-scoped
   *       stats-pointer re-mark instead.
   * </ul>
   */
  public static String ownerPointerKeyForBlob(String blobKey) {
    if (blobKey == null || blobKey.isEmpty()) {
      return null;
    }
    String key = blobKey.startsWith("/") ? blobKey.substring(1) : blobKey;
    String[] seg = key.split("/", -1);
    if (seg.length < 4 || !"accounts".equals(seg[0])) {
      return null;
    }
    try {
      return ownerPointerKeyForSegments(seg);
    } catch (IllegalArgumentException e) {
      // A malformed key (blank segment, ...) has no derivable owner; the caller falls back to
      // treating the blob as unowned, exactly as before this mapping existed.
      return null;
    }
  }

  private static String ownerPointerKeyForSegments(String[] seg) {
    String account = percentDecode(seg[1]);
    return switch (seg[2]) {
      case "account" -> seg.length == 4 ? accountPointerById(account) : null;
      case "catalogs" ->
          seg.length == 6 && "catalog".equals(seg[4])
              ? catalogPointerById(account, percentDecode(seg[3]))
              : null;
      case "namespaces" ->
          seg.length == 6 && "namespace".equals(seg[4])
              ? namespacePointerById(account, percentDecode(seg[3]))
              : null;
      case "views" ->
          seg.length == 6 && "view".equals(seg[4])
              ? viewPointerById(account, percentDecode(seg[3]))
              : null;
      case "connectors" ->
          seg.length == 6 && "connector".equals(seg[4])
              ? connectorPointerById(account, percentDecode(seg[3]))
              : null;
      case "storage-authorities" ->
          seg.length == 6 && "storage-authority".equals(seg[4])
              ? storageAuthorityPointerById(account, percentDecode(seg[3]))
              : null;
      case "catalog-integrations" ->
          seg.length == 6 && "integration".equals(seg[4])
              ? catalogIntegrationPointerById(account, percentDecode(seg[3]))
              : null;
      case "catalog-overlays" ->
          seg.length == 6 && "overlay".equals(seg[4])
              ? catalogOverlayPointerById(account, percentDecode(seg[3]))
              : null;
      case "tables" -> seg.length >= 6 ? tableBlobOwner(account, seg) : null;
      default -> null;
    };
  }

  private static String tableBlobOwner(String account, String[] seg) {
    String table = percentDecode(seg[3]);
    switch (seg[4]) {
      case "table":
        return seg.length == 6 ? tablePointerById(account, table) : null;
      case "root":
        // root/<sha>.pb is owned by the current-root pointer; root/manifest/<sha>.pb pages are
        // referenced only from root blob content — no owning pointer.
        return seg.length == 6 ? tableRootByTable(account, table) : null;
      case "snapshots":
        {
          // snapshots/current/<sha>.pb
          if (seg.length == 7 && "current".equals(seg[5])) {
            return currentSnapshotPointerByTable(account, table);
          }
          // snapshots/<snapshot_id>/snapshot/<sha>.pb
          Long sid = parseSnapshotId(seg[5]);
          if (sid == null) {
            return null;
          }
          if (seg.length == 8 && "snapshot".equals(seg[6])) {
            return snapshotPointerById(account, table, sid);
          }
          // snapshots/<snapshot_id>/index-artifacts/capture-manifests/<sha>.pb
          if (seg.length == 9
              && "index-artifacts".equals(seg[6])
              && "capture-manifests".equals(seg[7])) {
            return snapshotIndexArtifactCaptureManifestPointer(account, table, sid);
          }
          return null;
        }
      case "constraints":
        {
          // constraints/<snapshot_id>/<sha>.pb
          Long sid = seg.length == 7 ? parseSnapshotId(seg[5]) : null;
          return sid == null ? null : snapshotConstraintsPointer(account, table, sid);
        }
      case "target-stats":
        {
          // target-stats/<snapshot_id>/manifests/<generation>.pb -> the active-generation pointer.
          // Generation record paths carry only a target hash, so their owner is not derivable.
          Long sid = parseSnapshotId(seg[5]);
          if (sid == null) {
            return null;
          }
          if (seg.length == 8 && "manifests".equals(seg[6])) {
            return snapshotTargetStatsManifestPointer(account, table, sid);
          }
          return null;
        }
      default:
        return null;
    }
  }

  private static Long parseSnapshotId(String segment) {
    try {
      long sid = Long.parseLong(segment);
      return sid < 0 ? null : sid;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Returns the last path segment of a pointer key, percent-decoded. Used as a fallback to extract
   * display_name from a by-name pointer key when the Pointer.display_name field is not set (e.g.
   * for pointers written before the topology fields were added).
   *
   * <p>Example: {@code /accounts/x/catalogs/c/namespaces/n/tables/by-name/my%20table} → {@code my
   * table}
   */
  public static String extractLastSegment(String key) {
    if (key == null || key.isEmpty()) {
      return key;
    }
    int lastSlash = key.lastIndexOf('/');
    String encoded = lastSlash >= 0 ? key.substring(lastSlash + 1) : key;
    return percentDecode(encoded);
  }

  /**
   * Returns the full namespace path encoded in a by-path namespace pointer key.
   *
   * <p>The by-path key is reversible because each namespace path segment is percent-encoded before
   * the segments are joined with {@code /}. Literal slashes in namespace names are encoded as
   * {@code %2F}, so splitting the suffix on {@code /} is delimiter-safe.
   */
  public static List<String> extractNamespacePathSegments(
      String accountId, String catalogId, String key) {
    if (key == null || key.isEmpty()) {
      return List.of();
    }
    String prefix = namespacePointerByPathPrefix(accountId, catalogId, List.of());
    if (!key.startsWith(prefix)) {
      return List.of();
    }
    String suffix = key.substring(prefix.length());
    if (suffix.isEmpty()) {
      return List.of();
    }
    String[] encodedSegments = suffix.split("/");
    java.util.ArrayList<String> segments = new java.util.ArrayList<>(encodedSegments.length);
    for (String encoded : encodedSegments) {
      if (!encoded.isEmpty()) {
        segments.add(percentDecode(encoded));
      }
    }
    return List.copyOf(segments);
  }

  private static String percentDecode(String encoded) {
    if (encoded.indexOf('%') < 0) {
      return encoded;
    }
    byte[] bytes = new byte[encoded.length()];
    int out = 0;
    for (int i = 0; i < encoded.length(); ) {
      char ch = encoded.charAt(i);
      if (ch == '%' && i + 2 < encoded.length()) {
        int hi = Character.digit(encoded.charAt(i + 1), 16);
        int lo = Character.digit(encoded.charAt(i + 2), 16);
        if (hi >= 0 && lo >= 0) {
          bytes[out++] = (byte) ((hi << 4) | lo);
          i += 3;
          continue;
        }
      }
      bytes[out++] = (byte) ch;
      i++;
    }
    return new String(bytes, 0, out, StandardCharsets.UTF_8);
  }
}
