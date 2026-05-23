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

  private JobIndexBackendSupport() {}

  static CanonicalJobKey parseCanonicalJobKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    if (!normalized.startsWith("accounts/")) {
      return null;
    }
    String marker = "/reconcile/jobs/by-id/";
    int markerIndex = normalized.indexOf(marker);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring("accounts/".length(), markerIndex);
    String jobSegment = normalized.substring(markerIndex + marker.length());
    if (accountSegment.isBlank() || jobSegment.isBlank()) {
      return null;
    }
    return new CanonicalJobKey(pointerKey, accountSegment, jobSegment);
  }

  static LookupKey parseLookupKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    String prefix = "accounts/by-id/reconcile/jobs/by-id/";
    if (!normalized.startsWith(prefix)) {
      return null;
    }
    String jobSegment = normalized.substring(prefix.length());
    if (jobSegment.isBlank()) {
      return null;
    }
    return new LookupKey(pointerKey, jobSegment);
  }

  static ParentKey parseParentKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    if (!normalized.startsWith("accounts/")) {
      return null;
    }
    String marker = "/reconcile/jobs/by-parent/";
    int markerIndex = normalized.indexOf(marker);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring("accounts/".length(), markerIndex);
    String remainder = normalized.substring(markerIndex + marker.length());
    int slash = remainder.indexOf('/');
    if (slash < 0) {
      return null;
    }
    String parentSegment = remainder.substring(0, slash);
    String jobSegment = remainder.substring(slash + 1);
    if (accountSegment.isBlank() || parentSegment.isBlank() || jobSegment.isBlank()) {
      return null;
    }
    return new ParentKey(pointerKey, accountSegment, parentSegment, jobSegment);
  }

  static ConnectorKey parseConnectorKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    if (!normalized.startsWith("accounts/")) {
      return null;
    }
    String marker = "/reconcile/jobs/by-connector/";
    int markerIndex = normalized.indexOf(marker);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring("accounts/".length(), markerIndex);
    String remainder = normalized.substring(markerIndex + marker.length());
    int slash = remainder.indexOf('/');
    if (slash < 0) {
      return null;
    }
    String connectorSegment = remainder.substring(0, slash);
    String token = remainder.substring(slash + 1);
    if (accountSegment.isBlank() || connectorSegment.isBlank() || token.isBlank()) {
      return null;
    }
    return new ConnectorKey(pointerKey, accountSegment, connectorSegment, token);
  }

  static GlobalStateKey parseGlobalStateKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    String prefix = "accounts/by-id/reconcile/jobs/by-state/";
    if (!normalized.startsWith(prefix)) {
      return null;
    }
    String remainder = normalized.substring(prefix.length());
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
    if (!normalized.startsWith("accounts/")) {
      return null;
    }
    String marker = "/reconcile/jobs/by-state/";
    int markerIndex = normalized.indexOf(marker);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring("accounts/".length(), markerIndex);
    String remainder = normalized.substring(markerIndex + marker.length());
    String[] parts = remainder.split("/", 3);
    if (parts.length != 3) {
      return null;
    }
    if (accountSegment.isBlank()
        || parts[0].isBlank()
        || parts[1].isBlank()
        || parts[2].isBlank()) {
      return null;
    }
    return new AccountStateKey(pointerKey, accountSegment, parts[0], parts[1], parts[2]);
  }

  static ConnectorStateKey parseConnectorStateKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    if (!normalized.startsWith("accounts/")) {
      return null;
    }
    String marker = "/reconcile/jobs/by-connector-state/";
    int markerIndex = normalized.indexOf(marker);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring("accounts/".length(), markerIndex);
    String remainder = normalized.substring(markerIndex + marker.length());
    String[] parts = remainder.split("/", 4);
    if (parts.length != 4) {
      return null;
    }
    if (accountSegment.isBlank()
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
    if (!normalized.startsWith("accounts/")) {
      return null;
    }
    String marker = "/reconcile/dedupe/";
    int markerIndex = normalized.indexOf(marker);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring("accounts/".length(), markerIndex);
    String hashSegment = normalized.substring(markerIndex + marker.length());
    if (accountSegment.isBlank() || hashSegment.isBlank()) {
      return null;
    }
    return new DedupeKey(pointerKey, accountSegment, hashSegment);
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

  static String lookupSortKey(LookupKey key) {
    return "job/" + key.jobSegment();
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
    if (!normalized.startsWith("accounts/")) {
      return null;
    }
    String marker = "/reconcile/jobs/by-id/";
    int markerIndex = normalized.indexOf(marker);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring("accounts/".length(), markerIndex);
    if (accountSegment.isBlank()) {
      return null;
    }
    return new CanonicalJobKey(prefix + "__job__", accountSegment, "__job__");
  }

  static ParentKey parseParentPrefix(String prefix) {
    String normalized = stripLeadingSlash(prefix);
    if (!normalized.endsWith("/")) {
      return null;
    }
    if (!normalized.startsWith("accounts/")) {
      return null;
    }
    String marker = "/reconcile/jobs/by-parent/";
    int markerIndex = normalized.indexOf(marker);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring("accounts/".length(), markerIndex);
    String parentSegment =
        normalized.substring(markerIndex + marker.length(), normalized.length() - 1);
    if (accountSegment.isBlank() || parentSegment.isBlank()) {
      return null;
    }
    return new ParentKey(prefix + "__job__", accountSegment, parentSegment, "__job__");
  }

  static ConnectorKey parseConnectorPrefix(String prefix) {
    String normalized = stripLeadingSlash(prefix);
    if (!normalized.endsWith("/")) {
      return null;
    }
    if (!normalized.startsWith("accounts/")) {
      return null;
    }
    String marker = "/reconcile/jobs/by-connector/";
    int markerIndex = normalized.indexOf(marker);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring("accounts/".length(), markerIndex);
    String connectorSegment =
        normalized.substring(markerIndex + marker.length(), normalized.length() - 1);
    if (accountSegment.isBlank() || connectorSegment.isBlank()) {
      return null;
    }
    return new ConnectorKey(prefix + "__token__", accountSegment, connectorSegment, "__token__");
  }

  static GlobalStateKey parseGlobalStatePrefix(String prefix) {
    String normalized = stripLeadingSlash(prefix);
    String marker = "accounts/by-id/reconcile/jobs/by-state/";
    if (!normalized.startsWith(marker) || !normalized.endsWith("/")) {
      return null;
    }
    String stateSegment = normalized.substring(marker.length(), normalized.length() - 1);
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
    if (!normalized.startsWith("accounts/") || !normalized.endsWith("/")) {
      return null;
    }
    String marker = "/reconcile/jobs/by-state/";
    int markerIndex = normalized.indexOf(marker);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring("accounts/".length(), markerIndex);
    String stateSegment =
        normalized.substring(markerIndex + marker.length(), normalized.length() - 1);
    if (accountSegment.isBlank() || stateSegment.isBlank()) {
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
    if (!normalized.startsWith("accounts/") || !normalized.endsWith("/")) {
      return null;
    }
    String marker = "/reconcile/jobs/by-connector-state/";
    int markerIndex = normalized.indexOf(marker);
    if (markerIndex < 0) {
      return null;
    }
    String accountSegment = normalized.substring("accounts/".length(), markerIndex);
    String remainder = normalized.substring(markerIndex + marker.length(), normalized.length() - 1);
    String[] parts = remainder.split("/", 2);
    if (accountSegment.isBlank() || parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
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

  private static String stripLeadingSlash(String key) {
    if (key == null || key.isBlank()) {
      return "";
    }
    return key.startsWith("/") ? key.substring(1) : key;
  }

  record CanonicalJobKey(String pointerKey, String accountSegment, String jobSegment) {}

  record LookupKey(String pointerKey, String jobSegment) {}

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
