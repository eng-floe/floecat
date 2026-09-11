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

import java.time.Duration;

/**
 * The common cache events. Implementations report them; the container names the metrics. A cache
 * with extra lifecycle events may expose a more specific interface alongside this baseline.
 */
public interface CacheEvents {

  /**
   * Returns the same event contract scoped to an account. Implementations that do not publish an
   * account dimension can keep the default; callers do not need parallel metric APIs.
   */
  default CacheEvents forAccount(String accountId) {
    return this;
  }

  /**
   * Served without going to the source, after {@code served}. A hit is not always immediate: a
   * caller arriving during another's load waits for it, then is served from the map. Without the
   * duration a stampede of such followers reads as a high hit rate.
   */
  default void hit(Duration served) {}

  /**
   * Not held, whoever ends up loading it. Bulk reads report one per distinct missing key, so their
   * path cannot fall out of the ratio. That ratio is this against {@link #hit(Duration)} and
   * nothing else.
   */
  default void miss() {}

  /**
   * How long a loader invocation took. A bulk invocation reports one duration for all of its misses
   * rather than one duration per key.
   */
  default void loadTime(Duration elapsed) {}

  /**
   * A load threw; the caller still sees the exception. Its {@link #miss()} is raised too, so a
   * store failing every read does not report as a cache nobody uses.
   */
  default void loadFailed(Duration elapsed, RuntimeException error) {}

  /** Records a load whose result was not admitted to the cache. */
  default void loadDiscarded() {}

  /** Records a value rejected by the cache admission budget. */
  default void admissionRejected() {}

  /** Result of publishing a durable pointer mutation into the local pointer view. */
  enum WriteThroughResult {
    APPLIED,
    SKIPPED
  }

  /** Records whether a durable mutation was reflected in the local pointer view. */
  default void writeThrough(WriteThroughResult result) {}

  /**
   * An entry dropped to stay within budget, releasing {@code weightBytes}. Nothing expires, so this
   * is the signal a budget is too small; the weight tells many small evictions from a few large.
   *
   * <p>Raised while the implementation holds its eviction lock, and an implementation may swallow
   * anything thrown here. Do no work in it that can block.
   */
  default void evicted(long weightBytes) {}

  /** Records nothing. For tests and for callers that do not publish metrics. */
  static CacheEvents none() {
    return new CacheEvents() {};
  }
}
