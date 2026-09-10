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

package ai.floedb.floecat.cache;

/** Common cache events plus the disk tier's lifecycle signals. */
public interface BlobCacheEvents extends CacheEvents {

  /** A local entry failed its envelope validation and was discarded before source fallback. */
  default void corrupted(long bytes) {}

  /** One completed sweep. */
  default void swept(BlobCache.SweepResult result) {}

  /** A valid body could not be admitted because of the disk budget or a live mapping. */
  default void admissionRejected() {}

  static BlobCacheEvents none() {
    return new BlobCacheEvents() {};
  }
}
