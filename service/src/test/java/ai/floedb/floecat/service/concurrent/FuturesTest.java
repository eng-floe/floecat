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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Pins the unwrapping contract the fan-out relies on to surface a task's original failure. */
class FuturesTest {

  @Test
  void propagateReturnsTheOriginalUncheckedFailureForTheCallerToThrow() {
    IllegalStateException original = new IllegalStateException("store error");

    assertThatThrownBy(
            () -> {
              throw Futures.propagate(original, "unused");
            })
        .isSameAs(original);
  }

  @Test
  void propagateWrapsAnImpossibleCheckedCauseAsIllegalState() {
    IOException checked = new IOException("impossible here: fan-out tasks throw only unchecked");

    assertThatThrownBy(
            () -> {
              throw Futures.propagate(checked, "unexpected checked exception from fan-out");
            })
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("unexpected checked exception from fan-out")
        .hasCause(checked);
  }

  @Test
  void propagateRethrowsAnErrorDirectlyRatherThanWrappingIt() {
    // An Error cannot be wrapped without losing its type, so propagate throws it from inside rather
    // than returning it — the documented asymmetry that makes callers' leading `throw` unreachable
    // on this one path.
    OutOfMemoryError error = new OutOfMemoryError("heap");

    assertThatThrownBy(() -> Futures.propagate(error, "unused")).isSameAs(error);
  }
}
