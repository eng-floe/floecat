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

final class LeaseBackendSupport {
  private static final String ACCOUNT_SEGMENT_PLACEHOLDER = "__account__";
  private static final String JOB_SEGMENT_PLACEHOLDER = "__job__";
  static final String LEASE_POINTER_PREFIX = Keys.accountRootPrefix();
  static final String LEASE_POINTER_MARKER =
      accountScopedMarker(Keys.reconcileJobLeasePointerByIdPrefix(ACCOUNT_SEGMENT_PLACEHOLDER));
  static final String LEASE_EXPIRY_POINTER_PREFIX = Keys.reconcileJobLeaseExpiryPointerPrefix();
  private static final String LEASE_EXPIRY_SUFFIX_MARKER =
      Keys.reconcileJobLeaseExpiryPointerSuffix(
          ACCOUNT_SEGMENT_PLACEHOLDER, JOB_SEGMENT_PLACEHOLDER);
  private static final String LEASE_EXPIRY_JOBS_MARKER =
      leaseExpiryJobsMarker(LEASE_EXPIRY_SUFFIX_MARKER);
  static final String LEASE_PARTITION_PREFIX = "reconcile-lease/";
  static final String LEASE_SORT_PREFIX = "job/";
  static final String LEASE_OWNER_PARTITION_PREFIX = "reconcile-lease-owner/";
  static final String LEASE_OWNER_SORT_KEY = "owner";
  static final String LEASE_EXPIRY_PARTITION_KEY = "reconcile-lease-expiry";
  static final String LEASE_EXPIRY_PAGE_TOKEN_PREFIX = "rjlx:";
  static final String KIND_LEASE_ENTRY = "ReconcileJobLease";
  static final String KIND_LEASE_OWNER_ENTRY = "ReconcileJobLeaseOwner";
  static final String KIND_LEASE_EXPIRY_ENTRY = "ReconcileJobLeaseExpiry";
  static final String ATTR_POINTER_KEY = "pointer_key";
  static final String ATTR_CANONICAL_POINTER_KEY = "canonical_pointer_key";

  private LeaseBackendSupport() {}

  static LeasePointerKey parseLeasePointerKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    String normalizedPrefix = stripLeadingSlash(LEASE_POINTER_PREFIX);
    if (!normalized.startsWith(normalizedPrefix)) {
      return null;
    }
    int marker = normalized.indexOf(LEASE_POINTER_MARKER);
    if (marker < 0) {
      return null;
    }
    String accountSegment = normalized.substring(normalizedPrefix.length(), marker);
    String jobSegment = normalized.substring(marker + LEASE_POINTER_MARKER.length());
    if (accountSegment.isBlank()
        || Keys.isReservedAccountDirectorySegment(accountSegment)
        || jobSegment.isBlank()) {
      return null;
    }
    return new LeasePointerKey(pointerKey, accountSegment, jobSegment);
  }

  static LeaseExpiryPointerKey parseLeaseExpiryPointerKey(String pointerKey) {
    String normalized = stripLeadingSlash(pointerKey);
    String normalizedPrefix = stripLeadingSlash(LEASE_EXPIRY_POINTER_PREFIX);
    if (!normalized.startsWith(normalizedPrefix)) {
      return null;
    }
    String remainder = normalized.substring(normalizedPrefix.length());
    int slash = remainder.indexOf('/');
    if (slash < 0) {
      return null;
    }
    String expiryToken = remainder.substring(0, slash);
    String suffix = remainder.substring(slash);
    int jobsMarker = suffix.indexOf(LEASE_EXPIRY_JOBS_MARKER);
    if (!suffix.startsWith(Keys.accountRootPrefix()) || jobsMarker < 0) {
      return null;
    }
    String accountSegment = suffix.substring(Keys.accountRootPrefix().length(), jobsMarker);
    String jobSegment = suffix.substring(jobsMarker + LEASE_EXPIRY_JOBS_MARKER.length());
    if (expiryToken.isBlank()
        || accountSegment.isBlank()
        || Keys.isReservedAccountDirectorySegment(accountSegment)
        || jobSegment.isBlank()) {
      return null;
    }
    return new LeaseExpiryPointerKey(pointerKey, expiryToken, accountSegment, jobSegment);
  }

  static String leasePartitionKey(LeasePointerKey key) {
    return LEASE_PARTITION_PREFIX + key.accountSegment();
  }

  static String leaseSortKey(LeasePointerKey key) {
    return LEASE_SORT_PREFIX + key.jobSegment();
  }

  static String leasePointerKey(String accountId, String jobId) {
    if (accountId == null || accountId.isBlank() || jobId == null || jobId.isBlank()) {
      return "";
    }
    return Keys.reconcileJobLeasePointerById(accountId, jobId);
  }

  static String leaseExpirySortKey(LeaseExpiryPointerKey key) {
    String normalized = stripLeadingSlash(key.pointerKey());
    String normalizedPrefix = stripLeadingSlash(LEASE_EXPIRY_POINTER_PREFIX);
    if (normalized.startsWith(normalizedPrefix)) {
      return normalized.substring(normalizedPrefix.length());
    }
    return key.expiryToken()
        + Keys.reconcileJobLeaseExpiryPointerSuffix(key.accountSegment(), key.jobSegment());
  }

  static String ownerPartitionKey(String ownerKey) {
    return LEASE_OWNER_PARTITION_PREFIX + stripLeadingSlash(ownerKey);
  }

  static String encodeLeaseExpiryPageToken(String sortKey) {
    if (sortKey == null || sortKey.isBlank()) {
      return "";
    }
    return LEASE_EXPIRY_PAGE_TOKEN_PREFIX
        + java.util.Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(sortKey.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  static String decodeLeaseExpiryPageToken(String pageToken) {
    if (pageToken == null || pageToken.isBlank()) {
      return "";
    }
    if (!pageToken.startsWith(LEASE_EXPIRY_PAGE_TOKEN_PREFIX)) {
      LeaseExpiryPointerKey parsed = parseLeaseExpiryPointerKey(pageToken);
      return parsed == null ? "" : leaseExpirySortKey(parsed);
    }
    try {
      return new String(
          java.util.Base64.getUrlDecoder()
              .decode(pageToken.substring(LEASE_EXPIRY_PAGE_TOKEN_PREFIX.length())),
          java.nio.charset.StandardCharsets.UTF_8);
    } catch (RuntimeException e) {
      return "";
    }
  }

  record LeasePointerKey(String pointerKey, String accountSegment, String jobSegment) {}

  record LeaseExpiryPointerKey(
      String pointerKey, String expiryToken, String accountSegment, String jobSegment) {}

  private static String accountScopedMarker(String keysPrefix) {
    String normalized = stripLeadingSlash(keysPrefix);
    String accountPrefix = stripLeadingSlash(Keys.accountRootPrefix(ACCOUNT_SEGMENT_PLACEHOLDER));
    if (!normalized.startsWith(accountPrefix)) {
      throw new IllegalArgumentException("not an account-scoped Keys prefix: " + keysPrefix);
    }
    return "/" + normalized.substring(accountPrefix.length());
  }

  private static String leaseExpiryJobsMarker(String suffixMarker) {
    int accountPlaceholderIndex = suffixMarker.indexOf(ACCOUNT_SEGMENT_PLACEHOLDER);
    int jobPlaceholderIndex = suffixMarker.indexOf(JOB_SEGMENT_PLACEHOLDER);
    if (accountPlaceholderIndex < 0 || jobPlaceholderIndex < 0) {
      throw new IllegalArgumentException("invalid lease expiry suffix marker: " + suffixMarker);
    }
    return suffixMarker.substring(
        accountPlaceholderIndex + ACCOUNT_SEGMENT_PLACEHOLDER.length(), jobPlaceholderIndex);
  }

  private static String stripLeadingSlash(String value) {
    if (value == null || value.isBlank()) {
      return "";
    }
    return value.startsWith("/") ? value.substring(1) : value;
  }
}
