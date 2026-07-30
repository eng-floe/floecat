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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Helpers for joining {@link CompletableFuture}s. */
public final class Futures {

  private Futures() {}

  /**
   * Join {@code future}, unwrapping the {@link CompletionException} so the caller sees the task's
   * original {@link RuntimeException} or {@link Error} rather than a wrapper. A checked-exception
   * cause is a should-never-happen (the tasks here throw only unchecked) and surfaces as an {@link
   * IllegalStateException}.
   */
  public static <T> T join(CompletableFuture<T> future) {
    try {
      return future.join();
    } catch (CompletionException ce) {
      throw propagate(ce.getCause(), "unexpected checked exception from async task");
    }
  }

  /** Preserve unchecked failures and wrap an impossible checked asynchronous failure. */
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
