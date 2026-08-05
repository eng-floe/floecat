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
package ai.floedb.floecat.service.concurrent;

/** Unwrapping helpers for failures surfaced from async tasks. */
public final class Futures {

  private Futures() {}

  /**
   * The {@link RuntimeException} to {@code throw} for an unwrapped async failure: the original
   * unchecked exception, or an {@link IllegalStateException} wrapping an (impossible) checked one.
   * An {@link Error} cannot be wrapped without losing its type, so it is rethrown <em>directly</em>
   * from here rather than returned — on that one path the caller's own leading {@code throw} is
   * unreachable, which is harmless. Always invoke as {@code throw Futures.propagate(...)}.
   */
  public static RuntimeException propagate(Throwable failure, String checkedFailureMessage) {
    if (failure instanceof RuntimeException runtime) {
      return runtime;
    }
    if (failure instanceof Error error) {
      throw error;
    }
    return new IllegalStateException(checkedFailureMessage, failure);
  }
}
