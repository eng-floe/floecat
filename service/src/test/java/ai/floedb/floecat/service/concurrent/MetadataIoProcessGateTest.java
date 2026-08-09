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
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetadataIoProcessGateTest {

  @BeforeEach
  @AfterEach
  void clearIdleProcessGate() {
    MetadataIoProcessGate.State current = MetadataIoProcessGate.resolve(1);
    MetadataIoProcessGate.clearIfIdle(current.permits());
  }

  @Test
  void aReloadedApplicationClassReusesTheExistingGate() throws Exception {
    URL classes = MetadataIoProcessGate.class.getProtectionDomain().getCodeSource().getLocation();
    try (URLClassLoader reloaded = new URLClassLoader(new URL[] {classes}, null)) {
      Class<?> reloadedType = Class.forName(MetadataIoProcessGate.class.getName(), true, reloaded);
      Method resolve = reloadedType.getDeclaredMethod("resolve", int.class);
      resolve.setAccessible(true);
      Object reloadedState = resolve.invoke(null, 3);
      Method permitsMethod = reloadedState.getClass().getDeclaredMethod("permits");
      permitsMethod.setAccessible(true);
      Semaphore reloadedPermits = (Semaphore) permitsMethod.invoke(reloadedState);

      MetadataIoProcessGate.State current = MetadataIoProcessGate.resolve(7);
      try {
        assertEquals(3, current.capacity());
        assertSame(reloadedPermits, current.permits());
      } finally {
        MetadataIoProcessGate.clearIfIdle(current.permits());
      }
    }
  }
}
