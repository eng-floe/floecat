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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

public final class Keys {

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
  public static final String SEG_FILE_STATS = "/file-stats/";
  public static final String SEG_CONSTRAINTS = "/constraints/";
  public static final String SEG_NAMESPACE_BY_PATH = "/namespaces/by-path/";
  public static final String SEG_TABLES_BY_NAME = "/tables/by-name/";
  public static final String SEG_VIEWS_BY_NAME = "/views/by-name/";
  public static final String SEG_STATS = "/stats/";
  public static final String SEG_IDEMPOTENCY = "/idempotency/";
  public static final String SEG_MARKERS = "/markers/";
  public static final String SEG_TRANSACTIONS = "/transactions/";

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

  public static String accountBlobUri(String accountId, String sha256) {
    String tid = req("account_id", accountId);
    String sha = req("sha256", sha256);
    return String.format("/accounts/%s/account/%s.pb", encode(tid), encode(sha));
  }

  // ===== Transactions =====

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

  public static String snapshotTargetStatsDirectoryPointer(
      String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId) + "targets/";
  }

  public static String snapshotStatsPrefix(String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId);
  }

  public static String snapshotTargetStatsPointer(
      String accountId, String tableId, long snapshotId, String targetId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    String target = req("target_id", targetId);
    return snapshotTargetStatsDirectoryPointer(tid, tbid, sid) + encode(target);
  }

  public static String snapshotTargetStatsBlobUri(
      String accountId, String tableId, String targetId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    String target = req("target_id", targetId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/target-stats/%s/%s.pb",
        encode(tid), encode(tbid), encode(target), encode(sha));
  }

  public static String snapshotTargetStatsBlobPrefix(String accountId, String tableId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return String.format("/accounts/%s/tables/%s/target-stats/", encode(tid), encode(tbid));
  }

  public static String snapshotTargetStatsManifestPointer(
      String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId) + "targets-active";
  }

  /**
   * Pointer key for the latest snapshot that has committed stats for a table.
   *
   * <p>Updated whenever a new stats generation is first created; enables O(1) stale lookups without
   * scanning all snapshot manifest pointers.
   */
  public static String tableStatsLatestSnapshotPointer(String accountId, String tableId) {
    return "/accounts/"
        + encode(req("account_id", accountId))
        + "/tables/"
        + encode(req("table_id", tableId))
        + "/stats/latest-snapshot";
  }

  public static String tableStatsLatestSnapshotBlobUri(
      String accountId, String tableId, long snapshotId) {
    return String.format(
        "/accounts/%s/tables/%s/stats/latest-snapshot/%019d.pb",
        encode(req("account_id", accountId)),
        encode(req("table_id", tableId)),
        reqNonNegative("snapshot_id", snapshotId));
  }

  /**
   * Pointer key for the latest snapshot that has committed stats for a specific target (column).
   *
   * <p>Set on every write of a per-target stats record; enables O(1) per-column stale lookups for
   * targets absent from the table-level latest snapshot (partial snapshots, new columns).
   */
  public static String targetStatsLatestSnapshotPointer(
      String accountId, String tableId, String storageId) {
    return "/accounts/"
        + encode(req("account_id", accountId))
        + "/tables/"
        + encode(req("table_id", tableId))
        + "/stats/targets/"
        + encode(req("storage_id", storageId))
        + "/latest-snapshot";
  }

  public static String targetStatsLatestSnapshotBlobUri(
      String accountId, String tableId, String storageId, long snapshotId) {
    return String.format(
        "/accounts/%s/tables/%s/stats/targets/%s/latest-snapshot/%019d.pb",
        encode(req("account_id", accountId)),
        encode(req("table_id", tableId)),
        encode(req("storage_id", storageId)),
        reqNonNegative("snapshot_id", snapshotId));
  }

  public static String snapshotTargetStatsGenerationRootPointer(
      String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId) + "target-generations/";
  }

  public static String snapshotTargetStatsGenerationDirectoryPointer(
      String accountId, String tableId, long snapshotId, String generationId) {
    String generation = req("generation_id", generationId);
    return snapshotTargetStatsGenerationRootPointer(accountId, tableId, snapshotId)
        + encode(generation)
        + "/targets/";
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
    String generation = req("generation_id", generationId);
    return snapshotTargetStatsGenerationRootPointer(accountId, tableId, snapshotId)
        + encode(generation)
        + "/lifecycle";
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
        encode(tid), encode(tbid), sid, encode(generation), encode(target), encode(sha));
  }

  public static String snapshotTargetStatsBlobPrefix(
      String accountId, String tableId, long snapshotId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return String.format(
        "/accounts/%s/tables/%s/target-stats/%019d/", encode(tid), encode(tbid), sid);
  }

  public static String snapshotTargetStatsPrefix(
      String accountId, String tableId, long snapshotId) {
    return snapshotTargetStatsDirectoryPointer(accountId, tableId, snapshotId);
  }

  public static String snapshotTargetColumnStatsPrefix(
      String accountId, String tableId, long snapshotId, String columnTargetIdPrefix) {
    String prefix = req("column_target_id_prefix", columnTargetIdPrefix);
    return snapshotTargetStatsDirectoryPointer(accountId, tableId, snapshotId) + encode(prefix);
  }

  public static String snapshotIndexArtifactDirectoryPointer(
      String accountId, String tableId, long snapshotId) {
    return String.format(
        "/accounts/%s/tables/%s/snapshots/%019d/index-artifacts/",
        encode(req("account_id", accountId)),
        encode(req("table_id", tableId)),
        reqNonNegative("snapshot_id", snapshotId));
  }

  public static String snapshotIndexArtifactPointer(
      String accountId, String tableId, long snapshotId, String targetId) {
    String target = req("target_id", targetId);
    return snapshotIndexArtifactDirectoryPointer(accountId, tableId, snapshotId) + encode(target);
  }

  public static String snapshotIndexArtifactBlobUri(
      String accountId, String tableId, String targetId, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    String target = req("target_id", targetId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/index-artifacts/%s/%s.pb",
        encode(tid), encode(tbid), encode(target), encode(sha));
  }

  public static String snapshotIndexArtifactsPrefix(
      String accountId, String tableId, long snapshotId) {
    return snapshotIndexArtifactDirectoryPointer(accountId, tableId, snapshotId);
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

  public static String snapshotFileStatsDirectoryPointer(
      String accountId, String tableId, long snapshotId) {
    return snapshotStatsRootPointer(accountId, tableId, snapshotId) + "files/";
  }

  public static String snapshotFileStatsPointer(
      String accountId, String tableId, long snapshotId, String filePath) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    String fp = req("file_path", filePath);
    return snapshotFileStatsDirectoryPointer(tid, tbid, sid) + encode(fp);
  }

  public static String snapshotFileStatsBlobUri(
      String accountId, String tableId, String filePath, String sha256) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    String fp = req("file_path", filePath);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/tables/%s/file-stats/%s/%s.pb",
        encode(tid), encode(tbid), encode(fp), encode(sha));
  }

  public static String snapshotFileStatsBlobPrefix(String accountId, String tableId) {
    String tid = req("account_id", accountId);
    String tbid = req("table_id", tableId);
    return String.format("/accounts/%s/tables/%s/file-stats/", encode(tid), encode(tbid));
  }

  public static String snapshotFileStatsPrefix(String accountId, String tableId, long snapshotId) {
    return snapshotFileStatsDirectoryPointer(accountId, tableId, snapshotId);
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

  public static String reconcileDirtyParentPointerPrefix() {
    return "/accounts/by-id/reconcile/jobs/dirty-parents/";
  }

  public static String reconcileDirtyParentPointer(String accountId, String parentJobId) {
    String tid = req("account_id", accountId);
    String pid = req("parent_job_id", parentJobId);
    return reconcileDirtyParentPointerPrefix() + encode(tid) + "/" + encode(pid);
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
    return reconcileJobLeasePointerByIdPrefix(tid) + jid;
  }

  public static String reconcileJobLeasePointerByIdPrefix(String accountId) {
    String tid = req("account_id", accountId);
    return accountRootPrefix() + tid + "/reconcile/job-leases/by-id/";
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
    return "/accounts/" + tid + "/jobs/" + jid;
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

  public static String reconcileFileGroupStatsPayloadUri(
      String accountId, String parentJobId, String jobId, String leaseEpoch) {
    String resultUri =
        reconcileFileGroupResultPayloadUri(accountId, parentJobId, jobId, leaseEpoch);
    return resultUri.substring(0, resultUri.length() - ".pb".length()) + ".stats.pb";
  }

  public static String reconcileSnapshotFinalizeStatsPayloadUri(
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
        + ".stats.pb";
  }

  public static String reconcileSnapshotCaptureManifestUri(
      String accountId, String parentJobId, String jobId, String leaseEpoch) {
    String statsUri =
        reconcileSnapshotFinalizeStatsPayloadUri(accountId, parentJobId, jobId, leaseEpoch);
    return statsUri.substring(0, statsUri.length() - ".stats.pb".length()) + ".capture-manifest.pb";
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

  public static String reconcileSnapshotLeasePointer(String tableId, long snapshotId) {
    String tid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return "/accounts/by-id/reconcile/snapshot-leases/"
        + encode(tid)
        + "/"
        + String.format("%019d", sid);
  }

  public static String reconcileSnapshotLeasePointer(
      String accountId, String tableId, long snapshotId) {
    return reconcileSnapshotLeasePointer(tableId, snapshotId);
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
    return new GenerationKey(snapshotId, pointerKey.substring(genStart, genEnd));
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
   *   <li>the snapshot-id-less per-target stats records ({@code
   *       .../target-stats/<target>/<sha>.pb}) and file stats ({@code
   *       .../file-stats/<path>/<sha>.pb}): their owning pointers live under {@code
   *       .../snapshots/<snapshot_id>/stats/...} and the snapshot id is not part of the blob key.
   *       Note the generation-scoped target-stats shape ({@code
   *       .../target-stats/<snapshot_id>/generations/<gen>/<target>/<sha>.pb}) DOES carry the
   *       snapshot id and IS derivable — it takes the inline recheck path below, not this null
   *       branch.
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
          // snapshots/<snapshot_id>/snapshot/<sha>.pb
          Long sid = seg.length == 8 && "snapshot".equals(seg[6]) ? parseSnapshotId(seg[5]) : null;
          return sid == null ? null : snapshotPointerById(account, table, sid);
        }
      case "constraints":
        {
          // constraints/<snapshot_id>/<sha>.pb
          Long sid = seg.length == 7 ? parseSnapshotId(seg[5]) : null;
          return sid == null ? null : snapshotConstraintsPointer(account, table, sid);
        }
      case "target-stats":
        {
          // target-stats/<snapshot_id>/manifests/<generation>.pb -> the active-generation pointer;
          // target-stats/<snapshot_id>/generations/<gen>/<target>/<sha>.pb -> per-record pointer;
          // target-stats/<target>/<sha>.pb carries no snapshot id -> not derivable.
          Long sid = parseSnapshotId(seg[5]);
          if (sid == null) {
            return null;
          }
          if (seg.length == 8 && "manifests".equals(seg[6])) {
            return snapshotTargetStatsManifestPointer(account, table, sid);
          }
          if (seg.length == 10 && "generations".equals(seg[6])) {
            return snapshotTargetStatsGenerationPointer(
                account, table, sid, percentDecode(seg[7]), percentDecode(seg[8]));
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
