/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package ai.floedb.floecat.catalog.access;

/**
 * Prefix comparison for object-store locations.
 *
 * <p>One implementation, shared by the providers that pick a vended credential by scope and by the
 * callers that check a vended scope against the location they are about to read. A second copy is
 * how the two ends come to disagree about what a credential covers, and that disagreement only
 * shows up as a 403 partway through a scan.
 *
 * <p>Deliberately <em>not</em> the same rule as {@code
 * StorageAuthorityResolver.matchesLocationPrefix}, which looks like it answers the same question
 * and does not. That one decides what floecat authorizes or advertises, so it demands a path
 * boundary: {@code s3://w/orders} must not reach {@code s3://w/orders-archive}. This one decides
 * whether a credential the upstream catalog already minted applies to a location, and there the
 * catalog's own consumer sets the contract -- Iceberg's {@code S3FileIO.clientForStoragePath}
 * selects the longest {@code storagePath.startsWith(storagePrefix)}, with no boundary at all.
 *
 * <p>The rule that keeps the two apart is directional: <b>never be stricter than the component that
 * will actually use the credential.</b> Being stricter throws away a credential that would have
 * read, and on the vend path there is nothing to fall back to -- it is reached only once no storage
 * authority matched -- so a refusal here is a guaranteed failure where the catalog's own client
 * would have tried and either succeeded or been told no by S3, which enforces the real grant
 * whatever we believed. Being <em>more</em> permissive costs nothing for the same reason, which is
 * why the normalizations below are safe and a boundary check is not.
 */
public final class StorageLocations {
  private StorageLocations() {}

  /**
   * Whether {@code prefix} covers {@code location}.
   *
   * <p>Coverage is textual, as the upstream contract defines it: {@code s3://warehouse/orders}
   * covers {@code s3://warehouse/orders/data/p0}, the prefix itself, and also the sibling {@code
   * s3://warehouse/orders-archive/p0}. That last one is not an oversight -- Iceberg would select
   * this credential for that path, and if the credential does not actually reach it, S3 says so.
   * Refusing here instead would only turn a read that might have worked into one that cannot.
   *
   * <p>Two deviations, both deliberately more permissive than a literal comparison, because a
   * catalog's spelling of its own scope varies and a spelling mismatch costs a whole read: leading
   * and trailing whitespace is ignored, and a scope written with a trailing slash still names its
   * own root, so {@code s3://b/tbl/} covers {@code s3://b/tbl}. Neither widens the scope onto a
   * sibling: {@code s3://b/tbl/} still does not cover {@code s3://b/tblX}.
   *
   * <p>An absent or blank prefix covers every location, an absent one included: that is what a
   * catalog means by vending a credential with no scope, and there is no narrower answer to give.
   * Under any other prefix an absent location is covered by nothing, since there is nothing to
   * compare.
   */
  public static boolean covers(String prefix, String location) {
    if (prefix == null) {
      return true;
    }
    String normalizedPrefix = normalizeScheme(prefix.trim());
    if (normalizedPrefix.isEmpty()) {
      return true;
    }
    if (location == null) {
      return false;
    }
    String normalizedLocation = normalizeScheme(location.trim());
    // The upstream rule, verbatim.
    if (normalizedLocation.startsWith(normalizedPrefix)) {
      return true;
    }
    // Plus the one case a literal comparison gets wrong for our purposes: a catalog that scopes to
    // "s3://b/tbl/" means the table, and the table's own location is spelled without the slash.
    // Compared by equality rather than by stripping the slash and matching a prefix, which would
    // reach "s3://b/tblX" as well.
    return normalizedLocation.equals(stripTrailingSlash(normalizedPrefix));
  }

  /**
   * Rewrites s3a:// and s3n:// to s3://.
   *
   * <p>They address the same object store, so a credential scoped with one scheme covers a location
   * written with another. Any other scheme is returned untouched, and {@code null} in gives {@code
   * null} out -- callers hand this raw provider values, and the sibling {@link #covers} accepts a
   * null on either side.
   */
  public static String normalizeScheme(String location) {
    if (location == null) {
      return null;
    }
    int schemeEnd = location.indexOf("://");
    if (schemeEnd < 0) {
      return location;
    }
    String scheme = location.substring(0, schemeEnd);
    if ("s3".equalsIgnoreCase(scheme)
        || "s3a".equalsIgnoreCase(scheme)
        || "s3n".equalsIgnoreCase(scheme)) {
      return "s3" + location.substring(schemeEnd);
    }
    return location;
  }

  /**
   * The value without trailing slashes.
   *
   * <p>A catalog that scopes a credential to {@code s3://bucket/db/tbl/} and one that scopes it to
   * {@code s3://bucket/db/tbl} mean the same thing, and both spellings occur -- so a comparison
   * that keeps the slash silently covers nothing at all.
   */
  public static String stripTrailingSlash(String value) {
    if (value == null || value.isEmpty()) {
      return "";
    }
    int end = value.length();
    while (end > 0 && value.charAt(end - 1) == '/') {
      end--;
    }
    return value.substring(0, end);
  }
}
