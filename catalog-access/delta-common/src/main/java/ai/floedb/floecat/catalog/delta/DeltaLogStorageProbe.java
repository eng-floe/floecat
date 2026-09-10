/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.catalog.delta;

import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import java.util.Locale;
import java.util.Objects;

@FunctionalInterface
public interface DeltaLogStorageProbe {

  void validate(String tableLocation, VendedStorageCredentials credentials);

  /**
   * Whether the S3 probe can address this location at all.
   *
   * <p>Part of the contract rather than an internal: a provider that vends AWS credentials needs to
   * know before it reconciles a table whether the location it was given is one this can read. An
   * {@code abfss://} or {@code gs://} table would otherwise load, reconcile, and fail at every
   * scan.
   */
  static boolean s3Serves(String tableLocation) {
    return S3DeltaLogProbe.s3Location(tableLocation) != null;
  }

  /**
   * The form of a location a provider should publish, vend and compare.
   *
   * <p>An S3 object key may hold a space and {@code java.net.URI} may not. This probe encodes one
   * before parsing, so it answers for {@code s3://bucket/db/my table} -- and a provider that then
   * publishes the raw string reconciles a table the read path cannot open, because that path calls
   * {@code URI.create} on the location at every site. Before the probe accepted such a location it
   * was refused outright, which was at least a recorded failure; accepting it on one side without
   * canonicalising on the other turns that into a table that validates clean and fails every scan.
   *
   * <p>Here rather than in either provider because both need the same answer, which is the reason
   * this module exists. Encoded rather than refused because the read path derives its key with
   * {@code URI.getPath}, which decodes, so S3 is asked for the key that was written. Idempotent,
   * since only a literal space is replaced: a location that already carries {@code %20} is
   * unchanged and nothing is double-encoded.
   *
   * <p>Other characters {@code URI} rejects and S3 permits are still refused by {@link #s3Serves}.
   * A space is the one that occurs.
   */
  static String canonicalLocation(String tableLocation) {
    if (tableLocation == null) {
      return null;
    }
    // The scheme too, not only the space. s3Serves accepts s3, s3a and s3n in any case, while the
    // reader's resolvePath matches a literal lowercase s3:// or s3a:// and throws on anything
    // else -- so publishing the spelling the server used meant s3n://bucket/table and
    // S3://bucket/table validated, reconciled, and failed every scan on an unsupported path.
    // Servability and publication have to agree about what a location is, and the reader decides.
    // No trim. An S3 object key may end in a space, and the probe does not trim either -- so
    // trimming here published a different prefix from the one validation had just read, which is
    // the divergence this method exists to prevent. Whatever the server sent is the key; the only
    // changes made to it are the ones the reader requires.
    return publishableScheme(tableLocation).replace(" ", "%20");
  }

  /**
   * The scheme folded only where the reader cannot parse it.
   *
   * <p>{@code StorageLocations.normalizeScheme} folds s3, s3a and s3n alike, and folding everything
   * to s3 put the stored location in a different scheme namespace from the prefix an operator
   * registers: {@code StorageAuthorityResolver.matchesLocationPrefix} is a scheme-sensitive {@code
   * startsWith}, and {@code S3Location.parse} explicitly accepts an {@code s3a://} authority prefix
   * -- so a catalog reporting s3a, an authority registered as s3a, and a location stored as s3
   * never match.
   *
   * <p>So only the spellings the reader genuinely cannot read are changed: {@code s3n} and any
   * uppercase form, since {@code S3V2FileSystemClient.resolvePath} matches a literal lowercase
   * {@code s3://} or {@code s3a://}. What the source reported is otherwise what gets stored.
   */
  private static String publishableScheme(String location) {
    int schemeEnd = location.indexOf("://");
    if (schemeEnd < 0) {
      return location;
    }
    String scheme = location.substring(0, schemeEnd).toLowerCase(Locale.ROOT);
    if ("s3a".equals(scheme)) {
      return scheme + location.substring(schemeEnd);
    }
    return "s3".equals(scheme) || "s3n".equals(scheme)
        ? "s3" + location.substring(schemeEnd)
        : location;
  }

  /**
   * A probe that lists and reads one object under the table's Delta log with the vended
   * credentials.
   *
   * <p>Listing alone would pass on a grant that cannot read, so the probe reads a byte. What it
   * proves is that the credential the provider vended actually reaches the storage it named, which
   * is the question a validation run is asking on the operator's behalf.
   *
   * @param subject how the catalog is named in a refusal, such as {@code "Unity Catalog"}
   */
  static DeltaLogStorageProbe s3(String subject) {
    Objects.requireNonNull(subject, "subject");
    return (tableLocation, credentials) ->
        S3DeltaLogProbe.validate(subject, tableLocation, credentials);
  }
}
