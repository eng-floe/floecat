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

import java.util.function.Supplier;

/**
 * Repository composition policy for metadata reads and immutable-blob cache misses.
 *
 * <p>The policy owns the execution lifetime of each supplied operation. Implementations may apply
 * admission and cancellation, but must return the operation's value or propagate its failure. The
 * default cache-load path applies the same policy only after the cache selects its single-flight
 * loader, leaving cache probes, hits, and followers outside that lifetime.
 */
@FunctionalInterface
public interface MetadataReadPolicy extends BlobLoadPolicy {

  /** Execute metadata reads and cache misses directly on the calling thread. */
  MetadataReadPolicy DIRECT =
      new MetadataReadPolicy() {
        @Override
        public <T> T read(Supplier<T> reader) {
          return reader.get();
        }
      };

  /** Execute one complete metadata repository read according to this policy's semantics. */
  <T> T read(Supplier<T> reader);

  /** Apply the same read policy to a cold immutable-blob cache load. */
  @Override
  default <T> T load(Supplier<T> loader) {
    return read(loader);
  }
}
