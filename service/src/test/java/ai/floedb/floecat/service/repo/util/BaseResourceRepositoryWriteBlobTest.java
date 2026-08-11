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

package ai.floedb.floecat.service.repo.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.AccountKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.StringValue;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Re-referencing an existing content-addressed blob must refresh its LastModified — the write path
 * always PUTs, even when an identical blob exists. CasBlobGc's min-age fence is anchored on
 * LastModified at pass start: with a PUT-skip, a pointer CAS onto an old identical blob leaves it
 * looking unreferenced AND old, i.e. sweepable mid-pass (eng-floe/core#1904).
 */
class BaseResourceRepositoryWriteBlobTest {

  private static final class CountingBlobStore extends InMemoryBlobStore {
    final Map<String, Integer> puts = new HashMap<>();

    @Override
    public void put(String uri, byte[] bytes, String contentType) {
      puts.merge(uri, 1, Integer::sum);
      super.put(uri, bytes, contentType);
    }
  }

  private static GenericResourceRepository<Account, AccountKey> repo(CountingBlobStore blobs) {
    return new GenericResourceRepository<>(
        new InMemoryPointerStore(),
        blobs,
        Schemas.ACCOUNT,
        Account::parseFrom,
        Account::toByteArray,
        "application/x-protobuf");
  }

  private static final class ProjectingRepository extends BaseResourceRepository<StringValue> {
    private ProjectingRepository(
        InMemoryPointerStore pointers, InMemoryBlobStore blobs, ImmutableBlobCache blobCache) {
      super(
          pointers,
          blobs,
          StringValue::parseFrom,
          StringValue::toByteArray,
          "application/x-protobuf",
          blobCache);
    }

    @Override
    protected boolean blobsImmutable() {
      return true;
    }

    @Override
    protected StringValue parseReferencedBlob(String pointerKey, String blobUri, byte[] bytes) {
      return StringValue.of(pointerKey);
    }
  }

  @Test
  void putBlobRePutsWhenAnIdenticalBlobAlreadyExists() {
    var blobs = new CountingBlobStore();
    var repo = repo(blobs);
    var value = Account.newBuilder().setDisplayName("alpha").build();
    String uri = Keys.accountBlobUri("acct-1", "sha-1");

    repo.putBlob(uri, value);
    repo.putBlob(uri, value);

    assertEquals(2, blobs.puts.get(uri), "an identical re-write must still PUT");
  }

  @Test
  void putBlobStrictBytesRePutsWhenAnIdenticalBlobAlreadyExists() {
    var blobs = new CountingBlobStore();
    var repo = repo(blobs);
    byte[] bytes = "same".getBytes(StandardCharsets.UTF_8);
    String uri = Keys.accountBlobUri("acct-1", "sha-2");

    repo.putBlobStrictBytes(uri, bytes);
    repo.putBlobStrictBytes(uri, bytes);

    assertEquals(2, blobs.puts.get(uri), "an identical strict re-write must still PUT");
  }

  @Test
  void sharedBundleProjectionsAreCachedByPointerIdentity() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var cache = new ImmutableBlobCache(true, 1024 * 1024, Duration.ofMinutes(5));
    var repository = new ProjectingRepository(pointers, blobs, cache);
    String uri = "/stats/reuse-bundles/shared.pb";
    blobs.put(uri, StringValue.of("bundle").toByteArray(), "application/x-protobuf");
    pointers.compareAndSet(
        "pointer-one", 0L, PointerReferences.blobPointer("pointer-one", uri, 1L));
    pointers.compareAndSet(
        "pointer-two", 0L, PointerReferences.blobPointer("pointer-two", uri, 1L));

    assertEquals("pointer-one", repository.get("pointer-one").orElseThrow().getValue());
    assertEquals("pointer-two", repository.get("pointer-two").orElseThrow().getValue());
  }
}
