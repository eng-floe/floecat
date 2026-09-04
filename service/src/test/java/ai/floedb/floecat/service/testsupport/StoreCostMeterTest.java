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

package ai.floedb.floecat.service.testsupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class StoreCostMeterTest {
  @Test
  void interruptedSettlingFailsInsteadOfMeasuringAnUnstableWindow() {
    StoreCostMeter meter = new StoreCostMeter(new RecordingStoreReadObserver());
    AtomicBoolean bodyRan = new AtomicBoolean();

    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> meter.measure(() -> bodyRan.set(true)))
          .isInstanceOf(AssertionError.class)
          .hasMessageContaining("interrupted")
          .hasCauseInstanceOf(InterruptedException.class);
      assertThat(bodyRan).isFalse();
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }
}
