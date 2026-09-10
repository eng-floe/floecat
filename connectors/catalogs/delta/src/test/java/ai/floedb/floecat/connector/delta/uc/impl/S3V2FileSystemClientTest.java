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
package ai.floedb.floecat.connector.delta.uc.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.net.URI;
import org.junit.jupiter.api.Test;

/** Paths handed back from a listing have to be paths this client can parse again. */
class S3V2FileSystemClientTest {

  /**
   * A listed key holding a space becomes a path {@code URI.create} accepts, and decodes back to the
   * key that was written.
   *
   * <p>These paths come back through {@code resolvePath}, which parses them. Built by concatenating
   * the raw key, a table under {@code db/my table} listed its Delta log and then threw when the
   * next entry was opened -- so the location could be encoded by the caller and a listing would
   * lose it again on every entry after the first.
   */
  @Test
  void aListedKeyWithASpaceRoundTrips() {
    String path = S3V2FileSystemClient.s3Uri("warehouse", "db/my table/_delta_log/000.json");

    assertThat(path).isEqualTo("s3://warehouse/db/my%20table/_delta_log/000.json");
    assertThatCode(() -> URI.create(path)).doesNotThrowAnyException();
    assertThat(URI.create(path).getPath()).isEqualTo("/db/my table/_delta_log/000.json");
  }

  /**
   * A bucket name {@code java.net.URI} will not parse as a host. S3 permitted an underscore in
   * older regions, and for such a name {@code getHost} answers null while the authority carries it.
   * The shared probe accepts such a name, so this client has to address it: a table the probe
   * validates and the reader cannot open is one that reconciles and then fails every scan.
   */
  @Test
  void aBucketNameThatIsNotAHostnameIsStillAddressed() {
    assertThat(S3V2FileSystemClient.bucketOf(URI.create("s3://legacy_bucket/db/table")))
        .isEqualTo("legacy_bucket");
    assertThat(S3V2FileSystemClient.bucketOf(URI.create("s3://warehouse/db/table")))
        .isEqualTo("warehouse");
  }

  /**
   * And the call sites use it. {@code mkdirs} is the one path that needs no S3 call to reach the
   * bucket derivation, so it is where the wiring can be asserted rather than only the helper:
   * deriving from {@code getHost} alone rejects a legacy bucket name outright.
   */
  @Test
  void aLegacyBucketNameIsAddressableThroughTheClient() throws Exception {
    var client = new S3V2FileSystemClient(null);

    assertThat(client.mkdirs("s3://legacy_bucket/db/table")).isTrue();
    assertThat(client.mkdirs("s3://warehouse/db/table")).isTrue();
  }

  /**
   * The fallback accepts a plain authority and nothing else, which is the probe's rule too.
   *
   * <p>Only the fallback is guarded, because {@code getHost} answers the host for a location
   * carrying userinfo or a port -- it is not part of the host -- so those never reach it. Refusing
   * them is the location gate's job, and it does refuse them before anything is published.
   */
  @Test
  void theFallbackRefusesAnAuthorityCarryingMoreThanAName() {
    assertThat(S3V2FileSystemClient.bucketOf(URI.create("s3://user:pw@legacy_bucket/db/t")))
        .isNull();
    assertThat(S3V2FileSystemClient.bucketOf(URI.create("s3://legacy_bucket/db/t")))
        .isEqualTo("legacy_bucket");
  }

  /**
   * A bucket name {@code URI} will not parse as a host still gets an encoded path. Passing the
   * bucket as a host makes {@code URI} reject such a name, and the fallback then emits the raw key
   * -- leaving the space this method exists to encode, for exactly the bucket shape {@code
   * bucketOf} was added to support.
   */
  @Test
  void aLegacyBucketNameStillGetsAnEncodedPath() {
    String path = S3V2FileSystemClient.s3Uri("legacy_bucket", "db/my table/_delta_log/000.json");

    assertThat(path).isEqualTo("s3://legacy_bucket/db/my%20table/_delta_log/000.json");
    assertThat(URI.create(path).getPath()).isEqualTo("/db/my table/_delta_log/000.json");
    assertThat(S3V2FileSystemClient.bucketOf(URI.create(path))).isEqualTo("legacy_bucket");
  }

  /**
   * A key beginning with a slash keeps it. S3 permits such a key, and the location gate publishes
   * the {@code s3://bucket//table} shape that produces one, so a listing under it reaches here.
   *
   * <p>Encoding the key as one path handed {@code URI} a string opening "//", which it parsed as an
   * authority: {@code getRawPath} answered without the first segment and threw nothing, so the
   * fallback never ran and every listed entry addressed a key missing its leading name.
   */
  @Test
  void aKeyBeginningWithASlashKeepsIt() {
    assertThat(S3V2FileSystemClient.s3Uri("warehouse", "/table/_delta_log/000.json"))
        .isEqualTo("s3://warehouse//table/_delta_log/000.json");
    assertThat(S3V2FileSystemClient.s3Uri("warehouse", "/my table/000.json"))
        .isEqualTo("s3://warehouse//my%20table/000.json");
    assertThat(URI.create(S3V2FileSystemClient.s3Uri("warehouse", "/my table/000.json")).getPath())
        .isEqualTo("//my table/000.json");
  }

  /**
   * And a key holding nothing {@code URI} objects to is byte-identical to plain concatenation, so
   * only a path that would otherwise fail to parse is affected at all.
   */
  @Test
  void anOrdinaryKeyIsUnchanged() {
    for (String key : new String[] {"db/orders/_delta_log/000.json", "a/b/c.parquet", "x"}) {
      assertThat(S3V2FileSystemClient.s3Uri("warehouse", key))
          .as(key)
          .isEqualTo("s3://warehouse/" + key);
    }
  }
}
