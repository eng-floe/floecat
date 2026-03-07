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
package ai.floedb.floecat.storage.kv;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public final class Keys {
  private Keys() {}

  public static final String SEP = "/";

  private static String req(String name, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("key arg '" + name + "' is null/blank");
    }
    return value;
  }

  private static long reqNonNegative(String name, long value) {
    if (value < 0) {
      throw new IllegalArgumentException("key arg '" + name + "' must be >= 0");
    }
    return value;
  }

  public static String encodeSegment(String value) {
    return URLEncoder.encode(req("segment", value), StandardCharsets.UTF_8).replace("+", "%20");
  }

  public static String join(String... parts) {
    return SEP + String.join(SEP, parts);
  }

  public static String prefix(String... parts) {
    return join(parts) + SEP;
  }

  public static KvStore.Key key(String partitionKey, String sortKey) {
    return new KvStore.Key(partitionKey, sortKey);
  }

  public static String snapshotPointerById(String accountId, String tableId, long snapshotId) {
    String aid = req("account_id", accountId);
    String tid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    return String.format(
        "/accounts/%s/tables/%s/snapshots/by-id/%019d",
        encodeSegment(aid), encodeSegment(tid), sid);
  }

  public static String snapshotPointerByTime(
      String accountId, String tableId, long snapshotId, long upstreamCreatedAtMs) {
    String aid = req("account_id", accountId);
    String tid = req("table_id", tableId);
    long sid = reqNonNegative("snapshot_id", snapshotId);
    long createdAtMs = reqNonNegative("upstream_created_at_ms", upstreamCreatedAtMs);
    long inverted = Long.MAX_VALUE - createdAtMs;
    return String.format(
        "/accounts/%s/tables/%s/snapshots/by-time/%019d-%019d",
        encodeSegment(aid), encodeSegment(tid), inverted, sid);
  }

  public static String tableCommitJournalPointer(String accountId, String tableId, String txId) {
    String aid = req("account_id", accountId);
    String tid = req("table_id", tableId);
    String xid = req("tx_id", txId);
    return String.format(
        "/accounts/%s/tables/%s/tx-journal/%s",
        encodeSegment(aid), encodeSegment(tid), encodeSegment(xid));
  }

  public static String tableCommitOutboxPendingScanPrefix() {
    return "/accounts/";
  }

  public static String tableCommitOutboxPendingPrefix(String accountId) {
    String aid = req("account_id", accountId);
    return String.format("/accounts/%s/tx-outbox/pending/", encodeSegment(aid));
  }

  public static String tableCommitOutboxPendingPointer(
      long createdAtMs, String accountId, String tableId, String txId) {
    long created = reqNonNegative("created_at_ms", createdAtMs);
    String aid = req("account_id", accountId);
    String tid = req("table_id", tableId);
    String xid = req("tx_id", txId);
    return String.format(
        "/accounts/%s/tx-outbox/pending/%019d/%s/%s",
        encodeSegment(aid), created, encodeSegment(tid), encodeSegment(xid));
  }
}
