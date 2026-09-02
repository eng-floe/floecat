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

import ai.floedb.floecat.common.rpc.Pointer;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface PointerStore {
  sealed interface CasOp
      permits CasUpsert, UnconditionalUpsert, CasDelete, CasCheck, CasCheckAbsent {
    /**
     * The pointer this operation acts on. Every variant already carries it; declaring it here lets
     * a caller assembling a mixed batch check for duplicate keys without matching on the variant.
     */
    String key();
  }

  record CasUpsert(String key, long expectedVersion, Pointer next) implements CasOp {
    public CasUpsert {
      if (key == null || key.isBlank()) {
        throw new IllegalArgumentException("key must be non-blank");
      }
      if (expectedVersion < 0) {
        throw new IllegalArgumentException("expectedVersion must be >= 0");
      }
      if (next == null) {
        throw new IllegalArgumentException("next pointer must be set");
      }
    }
  }

  /**
   * Unconditional pointer replacement within an atomic batch.
   *
   * <p>The caller supplies the opaque version token written with the pointer. Consumers can later
   * use that token in a conditional delete, which makes this suitable for coalescing work markers:
   * concurrent producers never contend on a previously read version, while a consumer cannot delete
   * a marker replaced after it was observed.
   */
  record UnconditionalUpsert(String key, Pointer next) implements CasOp {
    public UnconditionalUpsert {
      if (key == null || key.isBlank()) {
        throw new IllegalArgumentException("key must be non-blank");
      }
      if (next == null) {
        throw new IllegalArgumentException("next pointer must be set");
      }
      if (next.getVersion() <= 0L) {
        throw new IllegalArgumentException("next pointer version must be > 0");
      }
    }
  }

  record CasDelete(String key, long expectedVersion) implements CasOp {
    public CasDelete {
      if (key == null || key.isBlank()) {
        throw new IllegalArgumentException("key must be non-blank");
      }
      if (expectedVersion <= 0) {
        throw new IllegalArgumentException("expectedVersion must be > 0");
      }
    }
  }

  record CasCheck(String key, long expectedVersion) implements CasOp {
    public CasCheck {
      if (key == null || key.isBlank()) {
        throw new IllegalArgumentException("key must be non-blank");
      }
      if (expectedVersion <= 0) {
        throw new IllegalArgumentException("expectedVersion must be > 0");
      }
    }
  }

  record CasCheckAbsent(String key) implements CasOp {
    public CasCheckAbsent {
      if (key == null || key.isBlank()) {
        throw new IllegalArgumentException("key must be non-blank");
      }
    }
  }

  Optional<Pointer> get(String key);

  /**
   * Reads multiple pointers. Backends should override with a native batch operation; the default
   * preserves compatibility for decorators and specialized test stores.
   */
  default Map<String, Pointer> getBatch(List<String> keys) {
    Map<String, Pointer> out = new LinkedHashMap<>();
    for (String key : keys == null ? List.<String>of() : keys) {
      get(key).ifPresent(pointer -> out.put(key, pointer));
    }
    return Map.copyOf(out);
  }

  /**
   * Reads multiple pointers from the authoritative store view. Backends with a native consistent
   * batch operation should override; the default preserves correctness for decorators and test
   * stores by composing the mandatory single-key consistent read.
   */
  default Map<String, Pointer> getBatchConsistent(List<String> keys) {
    Map<String, Pointer> out = new LinkedHashMap<>();
    for (String key : keys == null ? List.<String>of() : keys) {
      getConsistent(key).ifPresent(pointer -> out.put(key, pointer));
    }
    return Map.copyOf(out);
  }

  /**
   * The pointer as the store has it, never from a cache in front of it.
   *
   * <p>The prefix reads have had this distinction since caching was only a prefix concern; a
   * single-key form is needed for the same reason -- a CAS expected-version, a liveness probe and a
   * GC emptiness verdict are all questions a cache cannot answer, because what they ask about is
   * precisely what the cache might be behind on.
   *
   * <p><b>Not a default.</b> A store that is the source implements it as an ordinary read, and a
   * decorator has something to bypass -- but which of those an implementation is, only it knows. As
   * a default delegating to {@link #get} it read as free, and every implementation that never
   * thought about it inherited "consistent" as a synonym for "whatever get does". That holds only
   * while every read below is consistent: make one of them cheap and eventually consistent, and
   * every caller asking this question is silently answered by the wrong read, with no signature
   * moving and nothing failing.
   */
  Optional<Pointer> getConsistent(String key);

  boolean compareAndSet(String key, long expectedVersion, Pointer next);

  boolean delete(String key);

  boolean compareAndDelete(String key, long expectedVersion);

  boolean compareAndSetBatch(List<CasOp> ops);

  List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut);

  List<Pointer> listPointersByPrefixConsistent(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut);

  /**
   * Returns a page token that resumes a {@link #listPointersByPrefix} scan immediately after the
   * given pointer key, as if a previous page had ended exactly at that key. This lets callers that
   * post-filter scanned rows emit a cursor at the last row they actually consumed rather than at
   * the end of an over-fetched batch.
   *
   * <p>Stores that serve filtered pagination must override this with their native token encoding.
   * The default throws, so legacy or test stores that never need it are unaffected.
   */
  default String pageTokenAfterKey(String key) {
    throw new UnsupportedOperationException("pageTokenAfterKey is not supported by this store");
  }

  int deleteByPrefix(String prefix);

  /**
   * Deletes every pointer under {@code prefix} except the exact {@code excludedKey}.
   *
   * <p>The exclusion is continuous: implementations must never delete and recreate the excluded
   * key. Account teardown uses this to sweep an account partition while preserving its durable
   * deletion fence.
   */
  default int deleteByPrefixExcluding(String prefix, String excludedKey) {
    int deleted = 0;
    String token = "";
    var seenTokens = new HashSet<String>();
    do {
      var next = new StringBuilder();
      List<Pointer> page = listPointersByPrefixConsistent(prefix, 500, token, next);
      for (Pointer pointer : page) {
        if (!pointer.getKey().equals(excludedKey) && delete(pointer.getKey())) {
          deleted++;
        }
      }
      token = next.toString();
      if (!token.isBlank() && !seenTokens.add(token)) {
        throw new IllegalStateException("stagnant pointer scan token: " + token);
      }
    } while (!token.isBlank());
    return deleted;
  }

  int countByPrefix(String prefix);

  int countByPrefixConsistent(String prefix);

  boolean isEmpty();

  default void dump(String header) {}
}
