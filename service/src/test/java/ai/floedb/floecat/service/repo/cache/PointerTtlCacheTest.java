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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import ai.floedb.floecat.common.rpc.MutationMeta;
import org.junit.jupiter.api.Test;

/** The shared hardened pointer-cache contract, verified once for every pointer family using it. */
class PointerTtlCacheTest {

  private static MutationMeta meta(long version) {
    return MutationMeta.newBuilder()
        .setPointerVersion(version)
        .setBlobUri("s3://t/root-" + version + ".pb")
        .build();
  }

  @Test
  void aStalePopulateCannotOverwriteAFresherEntry() {
    // The read-your-writes race: reader read v1 pre-commit; writer committed v2 and repopulated;
    // the reader's straggling v1 populate lands LAST — and must lose.
    var cache = new PointerTtlCache<String>(60);
    cache.putIfFresher("t", meta(2));
    cache.putIfFresher("t", meta(1));
    assertEquals(2, cache.getIfPresent("t").getPointerVersion());
  }

  @Test
  void ttlZeroDisablesEntirely() {
    var cache = new PointerTtlCache<String>(0);
    assertFalse(cache.enabled());
    cache.putIfFresher("t", meta(1));
    assertNull(cache.getIfPresent("t"));
  }

  @Test
  void invalidateEvicts() {
    var cache = new PointerTtlCache<String>(60);
    cache.putIfFresher("t", meta(3));
    cache.invalidate("t");
    assertNull(cache.getIfPresent("t"));
  }
}
