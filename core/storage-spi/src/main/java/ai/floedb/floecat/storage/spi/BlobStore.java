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

package ai.floedb.floecat.storage.spi;

import ai.floedb.floecat.common.rpc.BlobHeader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface BlobStore {
  byte[] get(String uri);

  /**
   * Reads exactly {@code length} bytes beginning at the zero-based {@code offset}.
   *
   * <p>Stores with native range support should override this method. The default preserves
   * compatibility for simple implementations by slicing a full read.
   */
  default byte[] getRange(String uri, long offset, int length) {
    if (offset < 0L || length < 0) {
      throw new IllegalArgumentException("blob range offset and length must be non-negative");
    }
    byte[] bytes = get(uri);
    long end = Math.addExact(offset, length);
    if (bytes == null || offset > bytes.length || end > bytes.length) {
      throw new IllegalArgumentException("blob range is outside the object");
    }
    return Arrays.copyOfRange(bytes, Math.toIntExact(offset), Math.toIntExact(end));
  }

  void put(String uri, byte[] bytes, String contentType);

  Optional<BlobHeader> head(String uri);

  boolean delete(String uri);

  /**
   * Whether {@link #delete(String, String)} acts on immutable version identities. {@code false}
   * means a version-targeted delete cannot be trusted to leave a concurrent re-write intact — e.g.
   * an S3 bucket whose versioning status is not {@code Enabled}: unversioned and suspended buckets
   * overwrite the {@code "null"} version in place — so callers deleting for correctness must skip
   * deleting instead. The default is {@code false}: fail closed.
   */
  default boolean supportsVersionedDeletes() {
    return false;
  }

  /**
   * Deletes only the blob version that {@code versionId} names (as observed via {@link
   * BlobHeader#getVersionId()}). A different version — in particular one written concurrently after
   * the caller's {@link #head} — must survive, so a check-then-delete caller acts on exactly the
   * object it checked. Returns true when the named version was deleted (or already absent); false
   * when the store can tell the blob has moved past it and nothing was deleted.
   *
   * <p>Callers must gate on {@link #supportsVersionedDeletes()}; a blank {@code versionId} is a
   * caller bug and is rejected. There is deliberately NO fallback to the unconditional {@link
   * #delete(String)} — that would silently reintroduce the delete-after-re-reference race this
   * method exists to close.
   */
  default boolean delete(String uri, String versionId) {
    throw new UnsupportedOperationException("versioned deletes not supported by this store");
  }

  void deletePrefix(String prefix);

  default Map<String, byte[]> getBatch(List<String> uris) {
    Map<String, byte[]> out = new HashMap<>(uris.size());
    for (String u : uris) out.put(u, get(u));
    return out;
  }

  interface Page {
    List<String> keys();

    String nextToken();
  }

  Page list(String prefix, int limit, String pageToken);
}
