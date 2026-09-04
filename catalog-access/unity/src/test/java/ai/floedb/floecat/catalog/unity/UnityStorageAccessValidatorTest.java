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

package ai.floedb.floecat.catalog.unity;

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
class UnityStorageAccessValidatorTest {

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
      assertThat(UnityStorageAccessValidator.deltaLogPrefix(c.path()))
          .as(c.name())
          .isEqualTo(c.expected());
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
      assertThat(UnityStorageAccessValidator.storageFailureCode(c.errorCode(), c.status()))
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
      assertThat(UnityStorageAccessValidator.bucketOf(java.net.URI.create(c.location())))
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
      assertThat(UnityStorageAccessValidator.s3Bucket(c.location()))
          .as(c.name())
          .isEqualTo(c.expected());
    }
  }

  /**
   * A key holding a character URI rejects reads as "not addressable", not as a thrown parse error.
   * The parse used to run ahead of the guard meant to report it, so an object key with a space --
   * legal in S3 -- surfaced as "storage access validation configuration is invalid", describing
   * neither the location nor anything an operator had configured.
   */
  @Test
  void anUnparseableKeyIsRefusedRatherThanThrowingPastTheGuard() {
    for (String location :
        List.of("s3://warehouse/db/my table", "s3://warehouse/db/a|b", "s3://warehouse/db/x y/z")) {
      assertThat(UnityStorageAccessValidator.s3Location(location)).as(location).isNull();
      assertThat(UnityStorageAccessValidator.s3Bucket(location)).as(location).isNull();
    }
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
              () -> UnityStorageAccessValidator.s3().validate(location, credentials), location)
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
              () -> UnityStorageAccessValidator.s3().validate("s3://warehouse/orders", credentials),
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
    byte[] oversized = new byte[(int) UnityStorageAccessValidator.MAX_LISTING_RESPONSE_BYTES + 64];
    try (var bounded =
        UnityStorageAccessValidator.limited(
            new java.io.ByteArrayInputStream(oversized),
            UnityStorageAccessValidator.MAX_LISTING_RESPONSE_BYTES)) {
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
        UnityStorageAccessValidator.limited(
            new java.io.ByteArrayInputStream(ordinary),
            UnityStorageAccessValidator.MAX_LISTING_RESPONSE_BYTES)) {
      assertThat(bounded.readAllBytes()).isEqualTo(ordinary);
    }
    try (var bounded =
        UnityStorageAccessValidator.limited(new java.io.ByteArrayInputStream(ordinary), 4L)) {
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
