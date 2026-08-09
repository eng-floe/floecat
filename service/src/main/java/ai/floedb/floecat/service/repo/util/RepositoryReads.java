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

/** Read-only storage seam used by resource repositories. */
public record RepositoryReads(Pointers pointers, Blobs blobs) {

  public RepositoryReads {
    Objects.requireNonNull(pointers, "pointers");
    Objects.requireNonNull(blobs, "blobs");
  }

  /** Direct reads for repository families that are outside metadata admission. */
  public static RepositoryReads direct(PointerStore pointers, BlobStore blobs) {
    Objects.requireNonNull(pointers, "pointers");
    Objects.requireNonNull(blobs, "blobs");
    return new RepositoryReads(
        new Pointers() {
          @Override
          public Optional<Pointer> get(String key) {
            return pointers.get(key);
          }

          @Override
          public List<Pointer> list(
              String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
            return pointers.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
          }

          @Override
          public int count(String prefix) {
            return pointers.countByPrefix(prefix);
          }
        },
        new Blobs() {
          @Override
          public byte[] get(String uri) {
            return blobs.get(uri);
          }

          @Override
          public Map<String, byte[]> getBatch(List<String> uris) {
            return blobs.getBatch(uris);
          }

          @Override
          public Optional<BlobHeader> head(String uri) {
            return blobs.head(uri);
          }
        });
  }

  /** Pointer-store operations that cannot mutate durable state. */
  public interface Pointers {
    Optional<Pointer> get(String key);

    List<Pointer> list(String prefix, int limit, String pageToken, StringBuilder nextTokenOut);

    int count(String prefix);
  }

  /** Blob-store operations that cannot mutate durable state. */
  public interface Blobs {
    byte[] get(String uri);

    Map<String, byte[]> getBatch(List<String> uris);

    Optional<BlobHeader> head(String uri);
  }
}
