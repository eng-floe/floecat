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

package ai.floedb.floecat.types;

/**
 * A count (of values, NULLs, NaNs) over several contributors: their sum when every contributor
 * reports one, and unknown once one does not, since its rows may hold any number.
 */
public final class MergedCount {
  private long sum;
  private boolean known = true;
  private boolean counted;

  /**
   * Folds in one contributor's count, null when it reports none. A negative count, or a sum past
   * {@code Long.MAX_VALUE}, is not a count, so it leaves the merge unknown.
   */
  public void add(Long count) {
    if (count == null || count < 0 || sum > Long.MAX_VALUE - count) {
      known = false;
      return;
    }
    sum += count;
    counted = true;
  }

  /** The count, or null when it is unknown or no contributor reported one. */
  public Long value() {
    return known && counted ? sum : null;
  }
}
