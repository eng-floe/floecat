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
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface BlobStore {
  byte[] get(String uri);

  /** Reads the inclusive byte range {@code [offset, offset + length)}. */
  default byte[] getRange(String uri, long offset, int length) {
    if (offset < 0L || length < 0) {
      throw new IllegalArgumentException("blob range is invalid");
    }
    byte[] bytes = get(uri);
    if (offset > bytes.length || (long) length > bytes.length - offset) {
      throw new IllegalArgumentException("blob range exceeds the object");
    }
    return java.util.Arrays.copyOfRange(
        bytes, Math.toIntExact(offset), Math.toIntExact(offset + length));
  }

  void put(String uri, byte[] bytes, String contentType);

  /**
   * Writes content whose URI is immutable and content-derived. Implementations may skip the
   * read-before-write metadata preservation performed by ordinary mutable writes.
   */
  default void putImmutable(String uri, byte[] bytes, String contentType) {
    put(uri, bytes, contentType);
  }

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

  /**
   * Deletes objects whose keys begin with {@code prefix}.
   *
   * <p>Returns the number of object deletions the implementation reports as successful. Prefix
   * deletion is not atomic: an exception may be raised after earlier batches have already been
   * deleted. Implementation-specific housekeeping, such as removal of an object-store directory
   * marker, is not included in the count.
   */
  int deletePrefix(String prefix);

  /**
   * Read several blobs. A uri with no blob is <b>omitted</b> from the result rather than raising:
   * the caller is resolving a page, and one entry naming a blob that was superseded and swept is
   * that row's problem, not the page's. Every other fault still throws.
   */
  default Map<String, byte[]> getBatch(List<String> uris) {
    Map<String, byte[]> out = new LinkedHashMap<>(uris.size());
    for (String uri : uris) {
      try {
        byte[] bytes = get(uri);
        if (bytes != null) {
          out.put(uri, bytes);
        }
      } catch (StorageNotFoundException ignored) {
        // Missing members are intentionally omitted; all other storage failures still propagate.
      }
    }
    return out;
  }

  record Range(String uri, long offset, int length) {
    public Range {
      if (uri == null || uri.isBlank() || offset < 0L || length < 0) {
        throw new IllegalArgumentException("blob range is invalid");
      }
    }
  }

  /** Reads independent ranges; returned keys are the requested range descriptors. */
  default Map<Range, byte[]> getRanges(List<Range> ranges) {
    Map<Range, byte[]> out = new LinkedHashMap<>(ranges.size());
    for (Range range : ranges) {
      out.put(range, getRange(range.uri(), range.offset(), range.length()));
    }
    return out;
  }

  interface Page {
    List<String> keys();

    String nextToken();
  }

  Page list(String prefix, int limit, String pageToken);

  /**
   * Lists immediate child prefixes below {@code prefix} without enumerating every object below each
   * child. Object-store implementations should use their native delimiter support.
   */
  default Page listPrefixes(String prefix, int limit, String pageToken) {
    throw new UnsupportedOperationException("common-prefix listing is not supported");
  }
}
