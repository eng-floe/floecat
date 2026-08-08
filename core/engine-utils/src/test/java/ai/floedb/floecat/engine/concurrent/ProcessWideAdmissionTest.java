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
package ai.floedb.floecat.engine.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.Test;

class ProcessWideAdmissionTest {

  @Test
  void gateCreatedByAnotherApplicationClassloaderIsReused() throws Exception {
    URL classes = ProcessWideAdmission.class.getProtectionDomain().getCodeSource().getLocation();
    try (URLClassLoader reloaded = new URLClassLoader(new URL[] {classes}, null)) {
      Class<?> reloadedType = Class.forName(ProcessWideAdmission.class.getName(), true, reloaded);
      Method resolve = reloadedType.getMethod("resolve", int.class);
      Object reloadedState = resolve.invoke(null, 3);
      Semaphore reloadedPermits =
          (Semaphore) reloadedState.getClass().getMethod("permits").invoke(reloadedState);

      ProcessWideAdmission.State current = ProcessWideAdmission.resolve(7);
      try {
        assertThat(current.capacity()).isEqualTo(3);
        assertThat(current.permits()).isSameAs(reloadedPermits);
      } finally {
        ProcessWideAdmission.clearIfIdle(current.permits());
      }
    }
  }
}
