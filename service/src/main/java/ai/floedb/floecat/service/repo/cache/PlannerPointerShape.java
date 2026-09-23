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

package ai.floedb.floecat.service.repo.cache;

import ai.floedb.floecat.service.repo.model.Keys;
import java.util.List;
import java.util.Set;

/**
 * The durable pointer shapes the planner index may hold.
 *
 * <p>This is an allowlist on purpose: a family missing from it is answered from durable KV, which
 * is slower and safe. A denylist gives the dangerous default, where an unclassified family becomes
 * resident heap.
 */
final class PlannerPointerShape {
  private static final Set<String> RESIDENT_FAMILIES =
      Set.of(
          "account",
          "catalog-integrations",
          "catalog-overlays",
          "catalogs",
          "connectors",
          "namespaces",
          "relations",
          "storage-authorities",
          "tables",
          "views");
  private static final Set<String> RESIDENT_TABLE_SUFFIXES =
      Set.of("root/current", "snapshots/current");
  private static final String GLOBAL = "<account-directory>";

  private PlannerPointerShape() {}

  static boolean isResidentKey(String key) {
    String partition = partitionFor(key);
    return key != null
        && partition != null
        && !GLOBAL.equals(partition)
        && residentNamespace(key, false) == Namespace.RESIDENT;
  }

  static boolean isResidentListPrefix(String prefix) {
    String partition = partitionFor(prefix);
    return prefix != null
        && partition != null
        && !GLOBAL.equals(partition)
        && residentNamespace(prefix, true) == Namespace.RESIDENT;
  }

  static boolean mutationPrefixTouchesResidentKeys(String prefix) {
    String[] segments = accountKeySegments(prefix);
    if (segments == null) {
      return false;
    }
    if (segments.length == 1 && segments[0].isEmpty()) {
      return true;
    }
    if (!RESIDENT_FAMILIES.contains(segments[0])) {
      return false;
    }
    if (!"tables".equals(segments[0])) {
      return true;
    }
    return tablePrefixTouchesResidentKeys(segments);
  }

  static List<String> loadPrefixes(String accountId) {
    if (GLOBAL.equals(accountId)) {
      return List.of(Keys.accountPointerByIdPrefix(), Keys.accountPointerByNamePrefix());
    }
    String root = Keys.accountRootPrefix(accountId);
    return RESIDENT_FAMILIES.stream()
        .sorted()
        // Tables are the one family whose resident rows are not a prefix: the table id sits above
        // root/current and snapshots/current, so those are fetched per table instead of scanned.
        .map(
            family ->
                "tables".equals(family)
                    ? Keys.tablePointerByIdPrefix(accountId)
                    : root + family + "/")
        .toList();
  }

  static List<String> perTableKeys(String accountId, String tableId) {
    return List.of(
        Keys.tableRootByTable(accountId, tableId),
        Keys.currentSnapshotPointerByTable(accountId, tableId));
  }

  static String partitionFor(String key) {
    if (key == null || !key.startsWith(Keys.accountRootPrefix())) return null;
    String remainder = key.substring(Keys.accountRootPrefix().length());
    int slash = remainder.indexOf('/');
    String encodedAccount = slash < 0 ? remainder : remainder.substring(0, slash);
    if (encodedAccount.isBlank()) return null;
    if (Keys.isReservedAccountDirectorySegment(encodedAccount)) return GLOBAL;
    return Keys.decodeSegment(encodedAccount);
  }

  private static Namespace residentNamespace(String key, boolean listPrefix) {
    if (key == null || !key.startsWith(Keys.accountRootPrefix())) {
      return Namespace.UNKNOWN;
    }
    String remainder = key.substring(Keys.accountRootPrefix().length());
    int slash = remainder.indexOf('/');
    String account = slash < 0 ? remainder : remainder.substring(0, slash);
    if (account.isBlank()) return Namespace.UNKNOWN;
    if (Keys.isReservedAccountDirectorySegment(account)) return Namespace.ACCOUNT_DIRECTORY;
    // An account segment with nothing beneath it names no family, so no load prefix covers it.
    if (slash < 0) return Namespace.DURABLE_ONLY;
    String[] segments = remainder.substring(slash + 1).split("/", -1);
    if (segments.length == 1) {
      return Namespace.DURABLE_ONLY;
    }
    if (!RESIDENT_FAMILIES.contains(segments[0]) || Keys.isMarkerKey(key)) {
      return Namespace.DURABLE_ONLY;
    }
    if ("tables".equals(segments[0]) && !isResidentTableKey(segments, listPrefix)) {
      return Namespace.DURABLE_ONLY;
    }
    return Namespace.RESIDENT;
  }

  private static boolean isResidentTableKey(String[] segments, boolean listPrefix) {
    return ("by-id".equals(segments[1])
            && segments.length == 3
            && (listPrefix || !segments[2].isEmpty()))
        || (segments.length == 4
            && RESIDENT_TABLE_SUFFIXES.contains(segments[2] + "/" + segments[3]));
  }

  private static boolean tablePrefixTouchesResidentKeys(String[] segments) {
    if (segments.length <= 2) {
      return true;
    }
    if ("by-id".equals(segments[1])) {
      return true;
    }
    if (segments.length == 3 && segments[2].isEmpty()) {
      return true;
    }
    if ("root".equals(segments[2])) {
      return segments.length <= 3 || "current".equals(segments[3]) || segments[3].isEmpty();
    }
    if ("snapshots".equals(segments[2])) {
      return segments.length <= 3 || "current".equals(segments[3]) || segments[3].isEmpty();
    }
    return false;
  }

  private static String[] accountKeySegments(String key) {
    if (key == null || !key.startsWith(Keys.accountRootPrefix())) return null;
    String remainder = key.substring(Keys.accountRootPrefix().length());
    int slash = remainder.indexOf('/');
    if (slash < 0) return null;
    return remainder.substring(slash + 1).split("/", -1);
  }

  private enum Namespace {
    RESIDENT,
    DURABLE_ONLY,
    ACCOUNT_DIRECTORY,
    UNKNOWN
  }
}
