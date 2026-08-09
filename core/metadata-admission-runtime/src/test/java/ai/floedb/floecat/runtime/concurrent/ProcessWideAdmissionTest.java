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
package ai.floedb.floecat.runtime.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Verifies that reloadable callers retain the parent-loaded process gate. */
class ProcessWideAdmissionTest {

  @Test
  void reloadedCallerUsesTheExistingGateAndCapacity() throws Exception {
    ProcessWideAdmission.State original = ProcessWideAdmission.resolve(1);
    assertThat(original.permits().tryAcquire(1, TimeUnit.SECONDS)).isTrue();
    try (URLClassLoader reloaded =
        new ReloadingCallerClassLoader(testClasses(), getClass().getClassLoader())) {
      Class<?> caller = Class.forName(ReloadedAdmissionCaller.class.getName(), true, reloaded);
      Method resolve = caller.getMethod("resolve", int.class);
      ProcessWideAdmission.State fromReloadedCaller =
          (ProcessWideAdmission.State) resolve.invoke(null, 7);

      assertThat(caller.getClassLoader()).isNotSameAs(getClass().getClassLoader());
      assertThat(fromReloadedCaller).isSameAs(original);
      assertThat(fromReloadedCaller.capacity()).isEqualTo(1);
      assertThat(fromReloadedCaller.permits().tryAcquire()).isFalse();
    } finally {
      original.permits().release();
    }
  }

  private static URL testClasses() {
    return ReloadedAdmissionCaller.class.getProtectionDomain().getCodeSource().getLocation();
  }

  private static final class ReloadingCallerClassLoader extends URLClassLoader {
    ReloadingCallerClassLoader(URL classes, ClassLoader parent) {
      super(new URL[] {classes}, parent);
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve)
        throws ClassNotFoundException {
      if (!name.equals(ReloadedAdmissionCaller.class.getName())) {
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
