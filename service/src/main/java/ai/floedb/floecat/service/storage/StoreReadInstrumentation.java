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

package ai.floedb.floecat.service.storage;

import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/** Owns the success/error protocol once so both store decorators cannot drift. */
final class StoreReadInstrumentation {
  private StoreReadInstrumentation() {}

  static <T> T observe(
      StoreReadObserver observer,
      StoreReadObserver.ReadCall call,
      Supplier<T> body,
      ToLongFunction<T> bytes) {
    StoreReadObserver.Observation observation = observer.begin(call);
    try {
      T result = body.get();
      observation.success(bytes.applyAsLong(result));
      return result;
    } catch (RuntimeException | Error failure) {
      observation.failure(failure);
      throw failure;
    } finally {
      observation.close();
    }
  }
}
