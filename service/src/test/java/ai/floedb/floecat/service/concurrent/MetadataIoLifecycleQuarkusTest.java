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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.quarkus.arc.Arc;
import io.quarkus.arc.InstanceHandle;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the shutdown observer does not close metadata I/O before CDI teardown consumers.
 */
@QuarkusTest
class MetadataIoLifecycleQuarkusTest {

  private static final CancellableCallRunner.FailureMessages FAILURES =
      new CancellableCallRunner.FailureMessages("cancelled", "interrupted");

  @BeforeEach
  void reset() {
    MetadataIoTeardownConsumer.reset();
  }

  @Test
  void aTeardownConsumerCanUseMetadataIoAfterShutdownEvent() {
    // Quarkus sends ShutdownEvent before Arc invokes @PreDestroy. Destroying a dependent bean gives
    // the test a container-managed teardown boundary without stopping the test application itself.
    InstanceHandle<MetadataIoTeardownConsumer> consumer =
        Arc.container().instance(MetadataIoTeardownConsumer.class);
    assertNotNull(consumer.get());
    Arc.container().beanManager().getEvent().fire(new ShutdownEvent());
    consumer.destroy();

    assertEquals("teardown", MetadataIoTeardownConsumer.result.get());
    assertNull(MetadataIoTeardownConsumer.failure.get());
  }

  /** Records whether a container-managed teardown callback can complete admitted metadata I/O. */
  @Dependent
  static class MetadataIoTeardownConsumer {
    private static final AtomicReference<String> result = new AtomicReference<>();
    private static final AtomicReference<Throwable> failure = new AtomicReference<>();

    static void reset() {
      result.set(null);
      failure.set(null);
    }

    @PreDestroy
    void close() {
      try {
        result.set(new MetadataIoRunner().callWithoutCancellation(() -> "teardown", FAILURES));
      } catch (Throwable thrown) {
        failure.set(thrown);
      }
    }
  }
}
