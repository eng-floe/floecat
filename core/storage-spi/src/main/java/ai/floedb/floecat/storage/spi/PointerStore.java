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
import java.util.List;
import java.util.Optional;

public interface PointerStore {
  sealed interface CasOp
      permits CasUpsert, UnconditionalUpsert, CasDelete, CasCheck, CasCheckAbsent {}

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

  boolean compareAndSet(String key, long expectedVersion, Pointer next);

  boolean delete(String key);

  boolean compareAndDelete(String key, long expectedVersion);

  boolean compareAndSetBatch(List<CasOp> ops);

  List<Pointer> listPointersByPrefix(
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

  int countByPrefix(String prefix);

  boolean isEmpty();

  default void dump(String header) {}
}
