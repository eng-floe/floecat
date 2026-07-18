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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import ai.floedb.floecat.service.repo.model.Keys;
import java.util.List;

final class JobIndexBackendSupport {
  static final String KIND_CANONICAL_JOB = "ReconcileJobCanonical";
  static final String KIND_LOOKUP = "ReconcileJobLookup";
  static final String KIND_DEDUPE = "ReconcileJobDedupe";
  static final String KIND_PARENT = "ReconcileJobParent";
  static final String KIND_CONNECTOR = "ReconcileJobConnector";
  static final String KIND_GLOBAL_STATE = "ReconcileJobState";
  static final String KIND_ACCOUNT_STATE = "ReconcileJobAccountState";
  static final String KIND_CONNECTOR_STATE = "ReconcileJobConnectorState";
  static final String ATTR_POINTER_KEY = "pointer_key";
  static final String ATTR_CANONICAL_POINTER_KEY = "canonical_pointer_key";
  static final String ATTR_ACCOUNT_ID = "account_id";
  static final String ATTR_JOB_ID = "job_id";
  static final String ATTR_STATE = "job_state";
  static final String ATTR_CONNECTOR_ID = "connector_id";
  static final String ATTR_PARENT_JOB_ID = "parent_job_id";
  static final String ATTR_DEDUPE_KEY_HASH = "dedupe_key_hash";
  static final String ATTR_BLOB_URI = "blob_uri";
  static final String ATTR_CLEANUP_INDEX_POINTER_KEYS = "cleanup_index_pointer_keys";
  static final String ATTR_CLEANUP_READY_POINTER_KEYS = "cleanup_ready_pointer_keys";
  static final String ATTR_CLEANUP_MANIFEST_COMPLETE = "cleanup_manifest_complete";
  private static final String ACCOUNT_SEGMENT_PLACEHOLDER = "__account__";
  private static final String PARENT_SEGMENT_PLACEHOLDER = "__parent__";
  private static final String CONNECTOR_SEGMENT_PLACEHOLDER = "__connector__";
  private static final String STATE_SEGMENT_PLACEHOLDER = "__state__";
  private static final String ACCOUNT_ROOT_PREFIX = stripLeadingSlash(Keys.accountRootPrefix());
  private static final String CANONICAL_JOB_MARKER =
      accountScopedMarker(Keys.reconcileJobPointerByIdPrefix(ACCOUNT_SEGMENT_PLACEHOLDER));
  private static final String LOOKUP_JOB_PREFIX =
      stripLeadingSlash(Keys.reconcileJobLookupPointerByIdPrefix());
  private static final String PARENT_JOB_MARKER =
      accountScopedMarkerBefore(
          Keys.reconcileJobByParentPointerPrefix(
              ACCOUNT_SEGMENT_PLACEHOLDER, PARENT_SEGMENT_PLACEHOLDER),
          PARENT_SEGMENT_PLACEHOLDER);
  private static final String CONNECTOR_JOB_MARKER =
      accountScopedMarkerBefore(
          Keys.reconcileJobByConnectorPointerPrefix(
              ACCOUNT_SEGMENT_PLACEHOLDER, CONNECTOR_SEGMENT_PLACEHOLDER),
          CONNECTOR_SEGMENT_PLACEHOLDER);
  private static final String GLOBAL_STATE_PREFIX =
      stripLeadingSlash(Keys.reconcileJobByStatePointerPrefix());
  private static final String ACCOUNT_STATE_MARKER =
      accountScopedMarkerBefore(
          Keys.reconcileJobByAccountStatePointerPrefix(
              ACCOUNT_SEGMENT_PLACEHOLDER, STATE_SEGMENT_PLACEHOLDER),
          STATE_SEGMENT_PLACEHOLDER);
  private static final String CONNECTOR_STATE_MARKER =
      accountScopedMarkerBefore(
          Keys.reconcileJobByConnectorStatePointerPrefix(
              ACCOUNT_SEGMENT_PLACEHOLDER,
              CONNECTOR_SEGMENT_PLACEHOLDER,
              STATE_SEGMENT_PLACEHOLDER),
          CONNECTOR_SEGMENT_PLACEHOLDER);
  private static final String DEDUPE_MARKER =
      accountScopedMarker(Keys.reconcileDedupePointerPrefix(ACCOUNT_SEGMENT_PLACEHOLDER));

  private JobIndexBackendSupport() {}

  static CanonicalJobKey parseCanonicalJobKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    if (!normalized.startsWith(ACCOUNT_ROOT_PREFIX)) {
      return null;
    }
    int markerIndex = normalized.indexOf(CANONICAL_JOB_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring(ACCOUNT_ROOT_PREFIX.length(), markerIndex);
    String jobSegment = normalized.substring(markerIndex + CANONICAL_JOB_MARKER.length());
    if (accountSegment.isBlank() || jobSegment.isBlank()) {
      return null;
    }
    if (isReservedAccountSegment(accountSegment)) {
      return null;
    }
    return new CanonicalJobKey(pointerKey, accountSegment, jobSegment);
  }

  static LookupKey parseLookupKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    if (!normalized.startsWith(LOOKUP_JOB_PREFIX)) {
      return null;
    }
    String jobSegment = normalized.substring(LOOKUP_JOB_PREFIX.length());
    if (jobSegment.isBlank()) {
      return null;
    }
    return new LookupKey(pointerKey, jobSegment);
  }

  static ParentKey parseParentKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    if (!normalized.startsWith(ACCOUNT_ROOT_PREFIX)) {
      return null;
    }
    int markerIndex = normalized.indexOf(PARENT_JOB_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring(ACCOUNT_ROOT_PREFIX.length(), markerIndex);
    String remainder = normalized.substring(markerIndex + PARENT_JOB_MARKER.length());
    int slash = remainder.indexOf('/');
    if (slash < 0) {
      return null;
    }
    String parentSegment = remainder.substring(0, slash);
    String jobSegment = remainder.substring(slash + 1);
    if (accountSegment.isBlank()
        || isReservedAccountSegment(accountSegment)
        || parentSegment.isBlank()
        || jobSegment.isBlank()) {
      return null;
    }
    return new ParentKey(pointerKey, accountSegment, parentSegment, jobSegment);
  }

  static ConnectorKey parseConnectorKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    if (!normalized.startsWith(ACCOUNT_ROOT_PREFIX)) {
      return null;
    }
    int markerIndex = normalized.indexOf(CONNECTOR_JOB_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring(ACCOUNT_ROOT_PREFIX.length(), markerIndex);
    String remainder = normalized.substring(markerIndex + CONNECTOR_JOB_MARKER.length());
    int slash = remainder.indexOf('/');
    if (slash < 0) {
      return null;
    }
    String connectorSegment = remainder.substring(0, slash);
    String token = remainder.substring(slash + 1);
    if (accountSegment.isBlank()
        || isReservedAccountSegment(accountSegment)
        || connectorSegment.isBlank()
        || token.isBlank()) {
      return null;
    }
    return new ConnectorKey(pointerKey, accountSegment, connectorSegment, token);
  }

  static GlobalStateKey parseGlobalStateKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    if (!normalized.startsWith(GLOBAL_STATE_PREFIX)) {
      return null;
    }
    String remainder = normalized.substring(GLOBAL_STATE_PREFIX.length());
    String[] parts = remainder.split("/", 4);
    if (parts.length != 4) {
      return null;
    }
    if (parts[0].isBlank() || parts[1].isBlank() || parts[2].isBlank() || parts[3].isBlank()) {
      return null;
    }
    return new GlobalStateKey(pointerKey, parts[0], parts[1], parts[2], parts[3]);
  }

  static AccountStateKey parseAccountStateKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    if (!normalized.startsWith(ACCOUNT_ROOT_PREFIX)) {
      return null;
    }
    int markerIndex = normalized.indexOf(ACCOUNT_STATE_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring(ACCOUNT_ROOT_PREFIX.length(), markerIndex);
    String remainder = normalized.substring(markerIndex + ACCOUNT_STATE_MARKER.length());
    String[] parts = remainder.split("/", 3);
    if (parts.length != 3) {
      return null;
    }
    if (accountSegment.isBlank()
        || isReservedAccountSegment(accountSegment)
        || parts[0].isBlank()
        || parts[1].isBlank()
        || parts[2].isBlank()) {
      return null;
    }
    return new AccountStateKey(pointerKey, accountSegment, parts[0], parts[1], parts[2]);
  }

  static ConnectorStateKey parseConnectorStateKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    if (!normalized.startsWith(ACCOUNT_ROOT_PREFIX)) {
      return null;
    }
    int markerIndex = normalized.indexOf(CONNECTOR_STATE_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring(ACCOUNT_ROOT_PREFIX.length(), markerIndex);
    String remainder = normalized.substring(markerIndex + CONNECTOR_STATE_MARKER.length());
    String[] parts = remainder.split("/", 4);
    if (parts.length != 4) {
      return null;
    }
    if (accountSegment.isBlank()
        || isReservedAccountSegment(accountSegment)
        || parts[0].isBlank()
        || parts[1].isBlank()
        || parts[2].isBlank()
        || parts[3].isBlank()) {
      return null;
    }
    return new ConnectorStateKey(
        pointerKey, accountSegment, parts[0], parts[1], parts[2], parts[3]);
  }

  static DedupeKey parseDedupeKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    if (!normalized.startsWith(ACCOUNT_ROOT_PREFIX)) {
      return null;
    }
    int markerIndex = normalized.indexOf(DEDUPE_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring(ACCOUNT_ROOT_PREFIX.length(), markerIndex);
    String hashSegment = normalized.substring(markerIndex + DEDUPE_MARKER.length());
    if (accountSegment.isBlank()
        || isReservedAccountSegment(accountSegment)
        || hashSegment.isBlank()) {
      return null;
    }
    return new DedupeKey(pointerKey, accountSegment, hashSegment);
  }

  static DedupeKey parseDedupePrefix(String prefix) {
    String normalized = stripLeadingSlash(prefix);
    if (!normalized.startsWith(ACCOUNT_ROOT_PREFIX) || !normalized.endsWith("/")) {
      return null;
    }
    int markerIndex = normalized.indexOf(DEDUPE_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring(ACCOUNT_ROOT_PREFIX.length(), markerIndex);
    if (accountSegment.isBlank() || isReservedAccountSegment(accountSegment)) {
      return null;
    }
    return new DedupeKey(prefix + "__hash__", accountSegment, "__hash__");
  }

  static String canonicalPartitionKey(String accountId) {
    var key = parseCanonicalPrefix(Keys.reconcileJobPointerByIdPrefix(accountId));
    return key == null ? "" : canonicalPartitionKey(key);
  }

  static String parentPartitionKey(String accountId, String parentJobId) {
    var key = parseParentPrefix(Keys.reconcileJobByParentPointerPrefix(accountId, parentJobId));
    return key == null ? "" : parentPartitionKey(key);
  }

  static String connectorPartitionKey(String accountId, String connectorId) {
    var key =
        parseConnectorPrefix(Keys.reconcileJobByConnectorPointerPrefix(accountId, connectorId));
    return key == null ? "" : connectorPartitionKey(key);
  }

  static String globalStatePartitionKey(String state) {
    var key = parseGlobalStatePrefix(Keys.reconcileJobByStatePointerPrefix(state));
    return key == null ? "" : globalStatePartitionKey(key);
  }

  static String accountStatePartitionKey(String accountId, String state) {
    var key =
        parseAccountStatePrefix(Keys.reconcileJobByAccountStatePointerPrefix(accountId, state));
    return key == null ? "" : accountStatePartitionKey(key);
  }

  static String connectorStatePartitionKey(String accountId, String connectorId, String state) {
    var key =
        parseConnectorStatePrefix(
            Keys.reconcileJobByConnectorStatePointerPrefix(accountId, connectorId, state));
    return key == null ? "" : connectorStatePartitionKey(key);
  }

  static String dedupePartitionKey(String accountId) {
    var key = parseDedupePrefix(Keys.reconcileDedupePointerPrefix(accountId));
    return key == null ? "" : dedupePartitionKey(key);
  }

  static String canonicalPartitionKey(CanonicalJobKey key) {
    return "reconcile-job/" + key.accountSegment();
  }

  static String canonicalSortKey(CanonicalJobKey key) {
    return "job/" + key.jobSegment();
  }

  static String lookupPartitionKey() {
    return "reconcile-job-lookup";
  }

  static String legacyLookupPartitionKey() {
    return "reconcile-job/by-id";
  }

  static String lookupSortKey(LookupKey key) {
    return "job/" + key.jobSegment();
  }

  static LookupStorageKey currentLookupStorageKey(LookupKey key) {
    return new LookupStorageKey(lookupPartitionKey(), lookupSortKey(key));
  }

  static LookupStorageKey legacyLookupStorageKey(LookupKey key) {
    return new LookupStorageKey(legacyLookupPartitionKey(), lookupSortKey(key));
  }

  static List<LookupStorageKey> lookupReadStorageKeys(LookupKey key) {
    return List.of(currentLookupStorageKey(key), legacyLookupStorageKey(key));
  }

  static String parentPartitionKey(ParentKey key) {
    return "reconcile-job-parent/" + key.accountSegment() + "/" + key.parentJobSegment();
  }

  static String parentSortKey(ParentKey key) {
    return "job/" + key.jobSegment();
  }

  static String connectorPartitionKey(ConnectorKey key) {
    return "reconcile-job-connector/" + key.accountSegment() + "/" + key.connectorSegment();
  }

  static String connectorSortKey(ConnectorKey key) {
    return key.token();
  }

  static String globalStatePartitionKey(GlobalStateKey key) {
    return "reconcile-job-state/" + key.stateSegment();
  }

  static String globalStateSortKey(GlobalStateKey key) {
    return key.timestampSegment() + "/" + key.accountSegment() + "/" + key.jobSegment();
  }

  static String accountStatePartitionKey(AccountStateKey key) {
    return "reconcile-job-account-state/" + key.accountSegment() + "/" + key.stateSegment();
  }

  static String accountStateSortKey(AccountStateKey key) {
    return key.timestampSegment() + "/" + key.jobSegment();
  }

  static String connectorStatePartitionKey(ConnectorStateKey key) {
    return "reconcile-job-connector-state/"
        + key.accountSegment()
        + "/"
        + key.connectorSegment()
        + "/"
        + key.stateSegment();
  }

  static String connectorStateSortKey(ConnectorStateKey key) {
    return key.timestampSegment() + "/" + key.jobSegment();
  }

  static String dedupePartitionKey(DedupeKey key) {
    return "reconcile-job-dedupe/" + key.accountSegment();
  }

  static String dedupeSortKey(DedupeKey key) {
    return "hash/" + key.hashSegment();
  }

  static CanonicalJobKey parseCanonicalPrefix(String prefix) {
    String normalized = stripLeadingSlash(prefix);
    if (!normalized.endsWith("/")) {
      return null;
    }
    if (!normalized.startsWith(ACCOUNT_ROOT_PREFIX)) {
      return null;
    }
    int markerIndex = normalized.indexOf(CANONICAL_JOB_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring(ACCOUNT_ROOT_PREFIX.length(), markerIndex);
    if (accountSegment.isBlank() || isReservedAccountSegment(accountSegment)) {
      return null;
    }
    return new CanonicalJobKey(prefix + "__job__", accountSegment, "__job__");
  }

  private static boolean isReservedAccountSegment(String accountSegment) {
    return Keys.isReservedAccountDirectorySegment(accountSegment);
  }

  static ParentKey parseParentPrefix(String prefix) {
    String normalized = stripLeadingSlash(prefix);
    if (!normalized.endsWith("/")) {
      return null;
    }
    if (!normalized.startsWith(ACCOUNT_ROOT_PREFIX)) {
      return null;
    }
    int markerIndex = normalized.indexOf(PARENT_JOB_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring(ACCOUNT_ROOT_PREFIX.length(), markerIndex);
    String parentSegment =
        normalized.substring(markerIndex + PARENT_JOB_MARKER.length(), normalized.length() - 1);
    if (accountSegment.isBlank()
        || isReservedAccountSegment(accountSegment)
        || parentSegment.isBlank()) {
      return null;
    }
    return new ParentKey(prefix + "__job__", accountSegment, parentSegment, "__job__");
  }

  static ConnectorKey parseConnectorPrefix(String prefix) {
    String normalized = stripLeadingSlash(prefix);
    if (!normalized.endsWith("/")) {
      return null;
    }
    if (!normalized.startsWith(ACCOUNT_ROOT_PREFIX)) {
      return null;
    }
    int markerIndex = normalized.indexOf(CONNECTOR_JOB_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring(ACCOUNT_ROOT_PREFIX.length(), markerIndex);
    String connectorSegment =
        normalized.substring(markerIndex + CONNECTOR_JOB_MARKER.length(), normalized.length() - 1);
    if (accountSegment.isBlank()
        || isReservedAccountSegment(accountSegment)
        || connectorSegment.isBlank()) {
      return null;
    }
    return new ConnectorKey(prefix + "__token__", accountSegment, connectorSegment, "__token__");
  }

  static GlobalStateKey parseGlobalStatePrefix(String prefix) {
    String normalized = stripLeadingSlash(prefix);
    if (!normalized.startsWith(GLOBAL_STATE_PREFIX) || !normalized.endsWith("/")) {
      return null;
    }
    String stateSegment =
        normalized.substring(GLOBAL_STATE_PREFIX.length(), normalized.length() - 1);
    if (stateSegment.isBlank()) {
      return null;
    }
    return new GlobalStateKey(
        prefix + "0000000000000000000/__acct__/__job__",
        stateSegment,
        "0000000000000000000",
        "__acct__",
        "__job__");
  }

  static AccountStateKey parseAccountStatePrefix(String prefix) {
    String normalized = stripLeadingSlash(prefix);
    if (!normalized.startsWith(ACCOUNT_ROOT_PREFIX) || !normalized.endsWith("/")) {
      return null;
    }
    int markerIndex = normalized.indexOf(ACCOUNT_STATE_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring(ACCOUNT_ROOT_PREFIX.length(), markerIndex);
    String stateSegment =
        normalized.substring(markerIndex + ACCOUNT_STATE_MARKER.length(), normalized.length() - 1);
    if (accountSegment.isBlank()
        || isReservedAccountSegment(accountSegment)
        || stateSegment.isBlank()) {
      return null;
    }
    return new AccountStateKey(
        prefix + "0000000000000000000/__job__",
        accountSegment,
        stateSegment,
        "0000000000000000000",
        "__job__");
  }

  static ConnectorStateKey parseConnectorStatePrefix(String prefix) {
    String normalized = stripLeadingSlash(prefix);
    if (!normalized.startsWith(ACCOUNT_ROOT_PREFIX) || !normalized.endsWith("/")) {
      return null;
    }
    int markerIndex = normalized.indexOf(CONNECTOR_STATE_MARKER);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring(ACCOUNT_ROOT_PREFIX.length(), markerIndex);
    String remainder =
        normalized.substring(
            markerIndex + CONNECTOR_STATE_MARKER.length(), normalized.length() - 1);
    String[] parts = remainder.split("/", 2);
    if (accountSegment.isBlank()
        || isReservedAccountSegment(accountSegment)
        || parts.length != 2
        || parts[0].isBlank()
        || parts[1].isBlank()) {
      return null;
    }
    return new ConnectorStateKey(
        prefix + "0000000000000000000/__job__",
        accountSegment,
        parts[0],
        parts[1],
        "0000000000000000000",
        "__job__");
  }

  private static String accountScopedMarker(String keysPrefix) {
    String normalized = stripLeadingSlash(keysPrefix);
    String accountPrefix = stripLeadingSlash(Keys.accountRootPrefix(ACCOUNT_SEGMENT_PLACEHOLDER));
    if (!normalized.startsWith(accountPrefix)) {
      throw new IllegalArgumentException("not an account-scoped Keys prefix: " + keysPrefix);
    }
    return "/" + normalized.substring(accountPrefix.length());
  }

  private static String accountScopedMarkerBefore(String keysPrefix, String segmentPlaceholder) {
    String marker = accountScopedMarker(keysPrefix);
    String placeholderPathElement = segmentPlaceholder + "/";
    int placeholderIndex = marker.indexOf(placeholderPathElement);
    if (placeholderIndex < 0) {
      throw new IllegalArgumentException(
          "Keys prefix does not contain placeholder " + segmentPlaceholder + ": " + keysPrefix);
    }
    return marker.substring(0, placeholderIndex);
  }

  private static String stripLeadingSlash(String key) {
    if (key == null || key.isBlank()) {
      return "";
    }
    return key.startsWith("/") ? key.substring(1) : key;
  }

  record CanonicalJobKey(String pointerKey, String accountSegment, String jobSegment) {}

  record LookupKey(String pointerKey, String jobSegment) {}

  record LookupStorageKey(String partitionKey, String sortKey) {}

  record ParentKey(
      String pointerKey, String accountSegment, String parentJobSegment, String jobSegment) {}

  record ConnectorKey(
      String pointerKey, String accountSegment, String connectorSegment, String token) {}

  record GlobalStateKey(
      String pointerKey,
      String stateSegment,
      String timestampSegment,
      String accountSegment,
      String jobSegment) {}

  record AccountStateKey(
      String pointerKey,
      String accountSegment,
      String stateSegment,
      String timestampSegment,
      String jobSegment) {}

  record ConnectorStateKey(
      String pointerKey,
      String accountSegment,
      String connectorSegment,
      String stateSegment,
      String timestampSegment,
      String jobSegment) {}

  record DedupeKey(String pointerKey, String accountSegment, String hashSegment) {}
}
