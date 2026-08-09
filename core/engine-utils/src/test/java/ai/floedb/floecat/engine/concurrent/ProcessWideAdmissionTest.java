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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Verifies that admission state remains stable across reloadable caller classloaders. */
class ProcessWideAdmissionTest {

  @BeforeEach
  @AfterEach
  void clearIdleGate() {
    ProcessWideAdmission.resetForTests();
  }

  @Test
  void aReloadedCallerUsesTheParentLoadedGateWhileOldWorkHoldsTheOnlyPermit() throws Exception {
    ProcessWideAdmission.State oldGeneration = ProcessWideAdmission.resolve(1);
    assertTrue(oldGeneration.permits().tryAcquire(1, java.util.concurrent.TimeUnit.MILLISECONDS));
    try (URLClassLoader reloaded =
        new ReloadingCallerClassLoader(testClasses(), getClass().getClassLoader())) {
      Class<?> caller =
          Class.forName(ReloadedProcessWideAdmissionCaller.class.getName(), true, reloaded);
      assertNotSame(getClass().getClassLoader(), caller.getClassLoader());
      Method resolve = caller.getMethod("resolve", int.class);
      ProcessWideAdmission.State reloadedGeneration =
          (ProcessWideAdmission.State) resolve.invoke(null, 3);

      assertEquals(1, reloadedGeneration.capacity());
      assertSame(oldGeneration.permits(), reloadedGeneration.permits());
      assertFalse(
          reloadedGeneration.permits().tryAcquire(1, java.util.concurrent.TimeUnit.MILLISECONDS));
    } finally {
      oldGeneration.permits().release();
    }
  }

  @Test
  void resolvingTheGateLeavesSystemPropertiesSerializable() {
    ProcessWideAdmission.resolve(1);

    assertDoesNotThrow(() -> System.getProperties().store(new StringWriter(), "test"));
  }

  private static URL testClasses() {
    return ReloadedProcessWideAdmissionCaller.class
        .getProtectionDomain()
        .getCodeSource()
        .getLocation();
  }

  /** Reloads only the test caller while delegating the admission holder to its parent. */
  private static final class ReloadingCallerClassLoader extends URLClassLoader {
    ReloadingCallerClassLoader(URL classes, ClassLoader parent) {
      super(new URL[] {classes}, parent);
    }

    /** Reload only the caller fixture while delegating all production classes to the parent. */
    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve)
        throws ClassNotFoundException {
      if (!name.equals(ReloadedProcessWideAdmissionCaller.class.getName())) {
        return super.loadClass(name, resolve);
      }
      Class<?> loaded = findLoadedClass(name);
      if (loaded == null) {
        loaded = findClass(name);
      }
      if (resolve) {
        resolveClass(loaded);
      }
      return loaded;
    }
  }
}
