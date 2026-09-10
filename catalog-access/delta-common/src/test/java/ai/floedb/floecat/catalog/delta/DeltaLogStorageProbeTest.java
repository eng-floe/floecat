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

package ai.floedb.floecat.catalog.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The parts of storage validation that can be decided without S3.
 *
 * <p>{@code validateS3} builds its own {@code S3Client}, so the request itself is only exercised by
 * the Docker smoke. The two decisions that are easy to get wrong and expensive to debug from a
 * smoke failure -- which key prefix the Delta log is looked for under, and what an S3 error means
 * about the credential -- are pure, and asserted here.
 */
class DeltaLogStorageProbeTest {

  /** Every refusal names its caller, so the tests assert against one name. */
  private static final String SUBJECT = "Test Catalog";

  /**
   * A table at the bucket root has an empty path, and a leading slash would make the prefix
   * "/_delta_log/" -- an S3 key prefix that matches nothing, so a table whose log is present would
   * be reported as having none.
   */
  @Test
  void deltaLogPrefixNeverLeadsWithASlash() {
    record Case(String name, String path, String expected) {}
    for (Case c :
        List.of(
            new Case("bucket root, empty path", "", "_delta_log/"),
            new Case("bucket root, slash only", "/", "_delta_log/"),
            new Case("table prefix", "/tpch/orders", "tpch/orders/_delta_log/"),
            new Case("trailing slash", "/tpch/orders/", "tpch/orders/_delta_log/"),
            new Case("no leading slash", "tpch/orders", "tpch/orders/_delta_log/"),
            new Case("null path", null, "_delta_log/"))) {
      assertThat(S3DeltaLogProbe.deltaLogPrefix(c.path())).as(c.name()).isEqualTo(c.expected());
    }
  }

  /**
   * The classification is a statement about whose problem it is. An expired or refused credential
   * is a fact about the credential; anything else is a fact about reaching the bucket, and saying
   * "unavailable" for a refusal would send an operator looking at the wrong thing.
   */
  @Test
  void storageFailureCodeSeparatesCredentialFaultsFromReachability() {
    record Case(String name, String errorCode, int status, CatalogAccessException.Code expected) {}
    for (Case c :
        List.of(
            new Case(
                "expired token",
                "ExpiredToken",
                400,
                CatalogAccessException.Code.CREDENTIAL_EXPIRED),
            new Case(
                "forbidden", "AccessDenied", 403, CatalogAccessException.Code.PERMISSION_DENIED),
            new Case("unauthorized", null, 401, CatalogAccessException.Code.PERMISSION_DENIED),
            new Case("server error", "InternalError", 500, CatalogAccessException.Code.UNAVAILABLE),
            new Case("no detail", null, 0, CatalogAccessException.Code.UNAVAILABLE),
            // Expiry wins over the status code: an expired token is commonly reported as a 400,
            // which would otherwise read as an unreachable bucket.
            new Case(
                "expired reported as 400",
                "ExpiredToken",
                400,
                CatalogAccessException.Code.CREDENTIAL_EXPIRED))) {
      assertThat(S3DeltaLogProbe.storageFailureCode(c.errorCode(), c.status()))
          .as(c.name())
          .isEqualTo(c.expected());
    }
  }

  /**
   * A bucket name with an underscore parses with a null host -- getHost applies RFC reg-name rules
   * -- and such buckets are legal for us-east-1 creations before the 2018 naming rules. Reporting
   * one as "not an S3 location" would point an operator at their storage backend rather than at a
   * parser rule, and a storage-access failure fails the whole Integration.
   */
  @Test
  void bucketOfFallsBackToTheAuthorityWhenTheHostIsUnparseable() {
    record Case(String name, String location, String expected) {}
    for (Case c :
        List.of(
            new Case("ordinary bucket", "s3://my-bucket/tpch/orders", "my-bucket"),
            new Case("legacy underscore", "s3://my_bucket/tpch/orders", "my_bucket"),
            new Case("underscore, no path", "s3://my_bucket", "my_bucket"),
            new Case("bucket root", "s3://my-bucket/", "my-bucket"),
            // Neither belongs in an S3 location, so an authority carrying one is not a bucket name.
            new Case("userinfo", "s3://user@my_bucket/x", null),
            new Case("port", "s3://my_bucket:9000/x", null))) {
      assertThat(S3DeltaLogProbe.bucketOf(java.net.URI.create(c.location())))
          .as(c.name())
          .isEqualTo(c.expected());
    }
  }

  /**
   * s3a and s3n address the same store, and the coverage rule folds them -- so a location written
   * with either vends and must validate too. Comparing the scheme literally failed such a table as
   * "not an S3 location", and the storage check has no per-table skip, so that reported the whole
   * integration invalid for reads that would have worked.
   */
  @Test
  void s3BucketFoldsTheSchemeAliasesTheCoverageRuleFolds() {
    record Case(String name, String location, String expected) {}
    for (Case c :
        List.of(
            new Case("s3", "s3://warehouse/orders", "warehouse"),
            new Case("s3a", "s3a://warehouse/orders", "warehouse"),
            new Case("s3n", "s3n://warehouse/orders", "warehouse"),
            new Case("mixed case", "S3A://warehouse/orders", "warehouse"),
            new Case("legacy underscore over s3a", "s3a://my_bucket/orders", "my_bucket"),
            // Still refused: these are not the same store under another spelling.
            new Case("gcs", "gs://warehouse/orders", null),
            new Case("abfss", "abfss://c@a.dfs.core.windows.net/o", null),
            new Case("no scheme", "/warehouse/orders", null),
            new Case("blank", "  ", null))) {
      assertThat(S3DeltaLogProbe.s3Bucket(c.location())).as(c.name()).isEqualTo(c.expected());
    }
  }

  /**
   * A key holding a character URI rejects reads as "not addressable", not as a thrown parse error.
   * The parse runs inside the guard that reports it, so such a key is refused for what it is --
   * rather than surfacing as "storage access validation configuration is invalid", which describes
   * neither the location nor anything an operator set.
   */
  @Test
  void anUnparseableKeyIsRefusedRatherThanThrowingPastTheGuard() {
    for (String location : List.of("s3://warehouse/db/a|b", "s3://warehouse/db/a\"b")) {
      assertThat(S3DeltaLogProbe.s3Location(location)).as(location).isNull();
      assertThat(S3DeltaLogProbe.s3Bucket(location)).as(location).isNull();
    }
  }

  /**
   * An S3 object key may end in a space, and the probe does not trim -- so trimming in the
   * canonical form published a different prefix from the one validation had just probed. Both
   * providers use this value for publication, scope comparison and validation, so such a table
   * either failed to read or was pointed at the trimmed name when something else lived there.
   */
  @Test
  void theCanonicalFormKeepsASignificantTrailingSpace() {
    assertThat(DeltaLogStorageProbe.canonicalLocation("s3://warehouse/table "))
        .isEqualTo("s3://warehouse/table%20");
    // The same key the probe addresses, which is the agreement that matters.
    assertThat(S3DeltaLogProbe.s3Location("s3://warehouse/table ").getPath()).isEqualTo("/table ");
  }

  /**
   * A location carrying a secret is not one this will serve. bucketOf refuses an authority holding
   * userinfo, but only on its fallback branch -- getHost() answers "bucket" here, so that check
   * never ran and a provider stored the whole string as the table's storage_location, putting a
   * password or a signed query into catalog metadata that every reader of the table can see.
   */
  @Test
  void aLocationCarryingUserinfoQueryFragmentOrPortIsRefused() {
    for (String location :
        List.of(
            "s3://user:password@warehouse/table",
            "s3://warehouse/table?X-Amz-Signature=deadbeef",
            "s3://warehouse/table#fragment",
            "s3://warehouse:9000/table")) {
      assertThat(S3DeltaLogProbe.s3Location(location)).as(location).isNull();
      assertThat(DeltaLogStorageProbe.s3Serves(location)).as(location).isFalse();
    }
  }

  /**
   * The canonical form is the spelling the reader accepts. s3Serves folds s3, s3a and s3n in any
   * case, while the reader matches a literal lowercase s3:// or s3a:// and throws on the rest -- so
   * publishing the server's spelling let s3n:// and S3:// validate, reconcile, and fail every scan.
   */
  @Test
  void theCanonicalFormNormalisesTheSchemeTheReaderAccepts() {
    // Only the spellings the reader cannot parse are changed. resolvePath matches a literal
    // lowercase s3:// or s3a://, so s3n and any uppercase form fold and s3a is left alone --
    // folding it too would put the stored location in a different scheme namespace from the
    // s3a:// prefix an operator may register, which matchesLocationPrefix compares literally.
    assertThat(DeltaLogStorageProbe.canonicalLocation("s3n://warehouse/table"))
        .isEqualTo("s3://warehouse/table");
    assertThat(DeltaLogStorageProbe.canonicalLocation("S3://warehouse/table"))
        .isEqualTo("s3://warehouse/table");
    assertThat(DeltaLogStorageProbe.canonicalLocation("S3A://warehouse/table"))
        .isEqualTo("s3a://warehouse/table");
    assertThat(DeltaLogStorageProbe.canonicalLocation("s3a://warehouse/my table"))
        .isEqualTo("s3a://warehouse/my%20table");
    // Everything servable is publishable, in a spelling the reader accepts.
    for (String location :
        List.of("s3://w/t", "s3a://w/t", "s3n://w/t", "S3://w/t", "s3://w/my table")) {
      assertThat(DeltaLogStorageProbe.s3Serves(location)).as(location).isTrue();
      String canonical = DeltaLogStorageProbe.canonicalLocation(location);
      assertThat(canonical.startsWith("s3://") || canonical.startsWith("s3a://"))
          .as(location)
          .isTrue();
    }
  }

  /**
   * A space is legal in an S3 object key and illegal in {@code java.net.URI}, so it is encoded
   * before parsing rather than read as a location with no addressable bucket. The key has to
   * survive the round trip: the probe asks S3 for the object it names, so an encoded space reaching
   * the request would look for a key nothing wrote.
   */
  @Test
  void aSpaceInAnObjectKeyIsAddressableAndKeepsItsKey() {
    for (String location : List.of("s3://warehouse/db/my table", "s3://warehouse/db/x y/z")) {
      assertThat(S3DeltaLogProbe.s3Location(location)).as(location).isNotNull();
      assertThat(S3DeltaLogProbe.s3Bucket(location)).as(location).isEqualTo("warehouse");
    }
    assertThat(S3DeltaLogProbe.s3Location("s3://warehouse/db/my table").getPath())
        .isEqualTo("/db/my table");
  }

  /**
   * An S3 object key may begin with a slash, so {@code s3://bucket//table} names the key {@code
   * /table}. The read path removes one leading slash and addresses that key; removing every one
   * probed a different prefix, so validation either rejected a readable table or read whatever
   * Delta log sat at the stripped prefix and reported success for a location no scan would reach.
   */
  @Test
  void aKeyBeginningWithASlashKeepsIt() {
    // The URI path, which is what the caller passes: getPath() on s3://warehouse//table is
    // //table, and on s3://warehouse/table is /table.
    assertThat(S3DeltaLogProbe.deltaLogPrefix("//table")).isEqualTo("/table/_delta_log/");
    assertThat(S3DeltaLogProbe.deltaLogPrefix("/table")).isEqualTo("table/_delta_log/");
  }

  /** Only S3 is validated, and a location that is not one says so rather than failing obscurely. */
  @Test
  void refusesALocationThatIsNotS3() {
    var credentials =
        new VendedStorageCredentials(
            Map.of("s3.access-key-id", "AKIA", "s3.secret-access-key", "secret"),
            "s3://warehouse/orders",
            Optional.empty());
    for (String location : List.of("gs://warehouse/orders", "abfss://c@a.dfs.core.windows.net/o")) {
      assertThatThrownBy(
              () -> DeltaLogStorageProbe.s3(SUBJECT).validate(location, credentials), location)
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED));
    }
  }

  /**
   * A credential missing half its key pair cannot be used to probe anything, and the failure names
   * the field rather than surfacing as an SDK error from inside the client build.
   */
  @Test
  void refusesCredentialsMissingTheKeyPair() {
    record Case(String name, Map<String, String> properties) {}
    for (Case c :
        List.of(
            new Case("no secret", Map.of("s3.access-key-id", "AKIA")),
            new Case("no access key", Map.of("s3.secret-access-key", "secret")),
            new Case(
                "blank access key",
                Map.of("s3.access-key-id", "  ", "s3.secret-access-key", "s")))) {
      var credentials =
          new VendedStorageCredentials(c.properties(), "s3://warehouse/orders", Optional.empty());
      assertThatThrownBy(
              () -> DeltaLogStorageProbe.s3(SUBJECT).validate("s3://warehouse/orders", credentials),
              c.name())
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code())
                      .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
    }
  }

  /**
   * maxKeys is a request hint and s3.endpoint is tenant-supplied, so an endpoint that ignores it
   * can answer with a listing of any size -- and the synchronous client materialises every Contents
   * entry before the first key is read. The ranged read below it already streams and aborts on this
   * premise; the listing had no matching bound.
   *
   * <p>The interceptor that installs this on the listing needs a built S3Client, which this module
   * cannot construct in test scope, so what is pinned here is the bound itself.
   */
  @Test
  void aListingBodyIsRefusedOnceItPassesTheCap() throws Exception {
    byte[] oversized = new byte[(int) S3DeltaLogProbe.MAX_LISTING_RESPONSE_BYTES + 64];
    try (var bounded =
        S3DeltaLogProbe.limited(
            new java.io.ByteArrayInputStream(oversized),
            S3DeltaLogProbe.MAX_LISTING_RESPONSE_BYTES)) {
      assertThatThrownBy(() -> bounded.readAllBytes())
          .isInstanceOf(java.io.IOException.class)
          .hasMessageContaining("ignored max-keys");
    }

    // A listing that respects max-keys is a few hundred bytes and must read through untouched,
    // byte-at-a-time reads included -- the XML parser does both.
    byte[] ordinary =
        "<ListBucketResult><Contents><Key>a</Key></Contents></ListBucketResult>"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
    try (var bounded =
        S3DeltaLogProbe.limited(
            new java.io.ByteArrayInputStream(ordinary),
            S3DeltaLogProbe.MAX_LISTING_RESPONSE_BYTES)) {
      assertThat(bounded.readAllBytes()).isEqualTo(ordinary);
    }
    try (var bounded = S3DeltaLogProbe.limited(new java.io.ByteArrayInputStream(ordinary), 4L)) {
      assertThat(bounded.read()).isEqualTo(ordinary[0]);
      assertThatThrownBy(
              () -> {
                for (int i = 0; i < ordinary.length; i++) {
                  bounded.read();
                }
              })
          .isInstanceOf(java.io.IOException.class);
    }
  }
}
