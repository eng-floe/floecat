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

package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** In-memory blob store that counts its reads, heads and puts. */
public final class CountingBlobStore extends InMemoryBlobStore {
  private final AtomicInteger pointGets = new AtomicInteger();
  private final AtomicInteger batchGets = new AtomicInteger();
  private final AtomicInteger batchedBodies = new AtomicInteger();
  private final AtomicInteger rangeGets = new AtomicInteger();
  private final AtomicInteger heads = new AtomicInteger();
  private final Map<String, Integer> puts = new ConcurrentHashMap<>();

  @Override
  public byte[] get(String uri) {
    pointGets.incrementAndGet();
    return super.get(uri);
  }

  @Override
  public Map<String, byte[]> getBatch(List<String> uris) {
    batchGets.incrementAndGet();
    batchedBodies.addAndGet(uris.size());
    return super.getBatch(uris);
  }

  /** Counted as a range read only: the inherited default would also count a point read. */
  @Override
  public byte[] getRange(String uri, long offset, int length) {
    rangeGets.incrementAndGet();
    byte[] bytes = super.get(uri);
    if (bytes == null) {
      return null;
    }
    return Arrays.copyOfRange(bytes, Math.toIntExact(offset), Math.toIntExact(offset) + length);
  }

  @Override
  public Optional<BlobHeader> head(String uri) {
    heads.incrementAndGet();
    return super.head(uri);
  }

  @Override
  public void put(String uri, byte[] bytes, String contentType) {
    puts.merge(uri, 1, Integer::sum);
    super.put(uri, bytes, contentType);
  }

  public int pointGets() {
    return pointGets.get();
  }

  public int batchGets() {
    return batchGets.get();
  }

  public int rangeGets() {
    return rangeGets.get();
  }

  /** Bodies requested by point and batch reads. */
  public int bodies() {
    return pointGets.get() + batchedBodies.get();
  }

  public int heads() {
    return heads.get();
  }

  public int puts(String uri) {
    return puts.getOrDefault(uri, 0);
  }

  /** Zeroes the read and head counters; puts are kept. */
  public void resetReads() {
    pointGets.set(0);
    batchGets.set(0);
    batchedBodies.set(0);
    rangeGets.set(0);
    heads.set(0);
  }
}
