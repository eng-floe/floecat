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
    String segment = req("segment", value);
    byte[] bytes = segment.getBytes(StandardCharsets.UTF_8);
    StringBuilder out = new StringBuilder(bytes.length * 3);
    for (byte b : bytes) {
      int ch = b & 0xFF;
      if (isUnreserved(ch)) {
        out.append((char) ch);
      } else {
        out.append('%');
        out.append(Character.toUpperCase(Character.forDigit((ch >>> 4) & 0x0F, 16)));
        out.append(Character.toUpperCase(Character.forDigit(ch & 0x0F, 16)));
      }
    }
    return out.toString();
  }

  public static String join(String... parts) {
    if (parts == null) {
      throw new IllegalArgumentException("key arg 'parts' is null");
    }
    if (parts.length == 0) {
      return SEP;
    }
    String[] encoded = new String[parts.length];
    for (int i = 0; i < parts.length; i++) {
      encoded[i] = encodeSegment(req("parts[" + i + "]", parts[i]));
    }
    return SEP + String.join(SEP, encoded);
  }

  public static String prefix(String... parts) {
    return join(parts) + SEP;
  }

  private static boolean isUnreserved(int ch) {
    return (ch >= 'A' && ch <= 'Z')
        || (ch >= 'a' && ch <= 'z')
        || (ch >= '0' && ch <= '9')
        || ch == '-'
        || ch == '.'
        || ch == '_'
        || ch == '~';
  }

  public static KvStore.Key key(String partitionKey, String sortKey) {
    return new KvStore.Key(partitionKey, sortKey);
  }

  public static String accountRootPrefix(String accountId) {
    String aid = req("account_id", accountId);
    return String.format("/accounts/%s/", encodeSegment(aid));
  }

  public static String transactionDeleteSentinelUri(
      String accountId, String txId, String targetPointerKey) {
    String aid = req("account_id", accountId);
    String xid = req("tx_id", txId);
    String key = req("target_pointer_key", targetPointerKey);
    return String.format(
        "/accounts/%s/transactions/%s/delete/%s",
        encodeSegment(aid), encodeSegment(xid), encodeSegment(key));
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
    long invertedSnapshotId = Long.MAX_VALUE - sid;
    return String.format(
        "/accounts/%s/tables/%s/snapshots/by-time/%019d-%019d",
        encodeSegment(aid), encodeSegment(tid), inverted, invertedSnapshotId);
  }

  public static String connectorPointerById(String accountId, String connectorId) {
    String aid = req("account_id", accountId);
    String cid = req("connector_id", connectorId);
    return String.format(
        "/accounts/%s/connectors/by-id/%s", encodeSegment(aid), encodeSegment(cid));
  }

  public static String connectorPointerByName(String accountId, String displayName) {
    String aid = req("account_id", accountId);
    String name = req("display_name", displayName);
    return String.format(
        "/accounts/%s/connectors/by-name/%s", encodeSegment(aid), encodeSegment(name));
  }

  public static String connectorBlobUri(String accountId, String connectorId, String sha256) {
    String aid = req("account_id", accountId);
    String cid = req("connector_id", connectorId);
    String sha = req("sha256", sha256);
    return String.format(
        "/accounts/%s/connectors/%s/connector/%s.pb",
        encodeSegment(aid), encodeSegment(cid), encodeSegment(sha));
  }
}
