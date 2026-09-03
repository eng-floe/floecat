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

import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/** Read-only storage seam used by resource repositories. */
public record RepositoryReads(Pointers pointers, Blobs blobs) {

  /** Require both read capabilities so a repository cannot be partially composed. */
  public RepositoryReads {
    Objects.requireNonNull(pointers, "pointers");
    Objects.requireNonNull(blobs, "blobs");
  }

  /** Build read capabilities that invoke each supplied store on the calling thread. */
  public static RepositoryReads direct(PointerStore pointers, BlobStore blobs) {
    return bind(pointers, blobs, directPolicy());
  }

  /** Build an execution policy that invokes a metadata operation on the calling thread. */
  public static ReadPolicy directPolicy() {
    return new ReadPolicy() {
      @Override
      public <T> T read(Supplier<T> operation) {
        return operation.get();
      }
    };
  }

  /**
   * Build both store adapters under one execution policy. Every backend read passes through {@code
   * policy} exactly once; cache placement remains the repository's responsibility.
   */
  public static RepositoryReads bind(PointerStore pointers, BlobStore blobs, ReadPolicy policy) {
    Objects.requireNonNull(pointers, "pointers");
    Objects.requireNonNull(blobs, "blobs");
    Objects.requireNonNull(policy, "policy");
    return new RepositoryReads(
        new Pointers() {
          @Override
          public Optional<Pointer> get(String key) {
            return policy.read(() -> pointers.get(key));
          }

          @Override
          public List<Pointer> list(
              String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
            return policy.read(
                () -> pointers.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut));
          }

          @Override
          public int count(String prefix) {
            return policy.read(() -> pointers.countByPrefix(prefix));
          }

          @Override
          public String pageTokenAfterKey(String key) {
            return policy.read(() -> pointers.pageTokenAfterKey(key));
          }
        },
        new Blobs() {
          @Override
          public byte[] get(String uri) {
            return policy.read(() -> blobs.get(uri));
          }

          @Override
          public Map<String, byte[]> getBatch(List<String> uris) {
            return policy.read(() -> blobs.getBatch(uris));
          }

          @Override
          public Optional<BlobHeader> head(String uri) {
            return policy.read(() -> blobs.head(uri));
          }
        });
  }

  /** Defines how one metadata read operation is executed and how its failure reaches the caller. */
  public interface ReadPolicy {
    /** Execute one backend operation, preserving its result type and runtime failure. */
    <T> T read(Supplier<T> operation);
  }

  /** Pointer-store read operations exposed to resource repositories. */
  public interface Pointers {
    /** Read one pointer by its canonical storage key. */
    Optional<Pointer> get(String key);

    /** Read one ordered page and append the continuation token to {@code nextTokenOut}. */
    List<Pointer> list(String prefix, int limit, String pageToken, StringBuilder nextTokenOut);

    /** Count pointers below one storage prefix. */
    int count(String prefix);

    /** Build a continuation token that resumes immediately after one pointer key. */
    String pageTokenAfterKey(String key);
  }

  /** Blob-store read operations exposed to resource repositories. */
  public interface Blobs {
    /** Read one blob body by URI. */
    byte[] get(String uri);

    /** Read a batch of blob bodies keyed by URI. */
    Map<String, byte[]> getBatch(List<String> uris);

    /** Read one blob's metadata without loading its body. */
    Optional<BlobHeader> head(String uri);
  }
}
