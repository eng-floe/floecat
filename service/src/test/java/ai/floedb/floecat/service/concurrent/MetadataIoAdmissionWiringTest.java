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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * Proves the CDI wiring, not just the interceptor logic: in the real container, a {@link
 * BoundMetadataIo} method is actually wrapped in admission. The probe method does no admission
 * itself, so a true reading can only come from the interceptor having fired.
 */
@QuarkusTest
class MetadataIoAdmissionWiringTest {

  @ApplicationScoped
  static class AdmissionProbe {
    @BoundMetadataIo
    public boolean observedAdmission() {
      return MetadataIoRunner.isRunningAdmittedOperation();
    }
  }

  @Inject AdmissionProbe probe;
  @Inject MetadataIoCacheMissAdmission cacheMissAdmission;

  @Test
  void containerWrapsBoundMetadataIoMethodsInAdmission() {
    assertFalse(MetadataIoRunner.isRunningAdmittedOperation());
    assertTrue(
        probe.observedAdmission(),
        "the @BoundMetadataIo method must run under admission — the interceptor did not fire");
    assertFalse(MetadataIoRunner.isRunningAdmittedOperation());
  }

  @Test
  void everyExposedMetadataReadHasAdmissionAtItsStoreBoundary() throws Exception {
    for (Class<?> repository :
        Set.of(
            CatalogRepository.class,
            NamespaceRepository.class,
            TableRepository.class,
            ViewRepository.class)) {
      assertAllPublicReadsAreBoundOrCacheFronted(repository);
      assertCacheFrontedReadUsesBoundMissLoader(repository);
    }

    assertTrue(
        MetadataIoCacheMissAdmission.class
            .getMethod("load", Supplier.class)
            .isAnnotationPresent(BoundMetadataIo.class),
        "the cache-miss loader must remain the admitted store boundary");
    assertTrue(
        cacheMissAdmission.load(MetadataIoRunner::isRunningAdmittedOperation),
        "the cache-miss loader must run under admission in the container");
  }

  private static void assertAllPublicReadsAreBoundOrCacheFronted(Class<?> type) {
    for (Method method : type.getDeclaredMethods()) {
      if (!Modifier.isPublic(method.getModifiers())
          || method.isSynthetic()
          || isWrite(method)
          || isCacheFrontedRead(method)) {
        continue;
      }
      assertTrue(
          method.isAnnotationPresent(BoundMetadataIo.class),
          () -> type.getSimpleName() + "." + method + " bypasses metadata-I/O admission");
    }
  }

  private static void assertCacheFrontedReadUsesBoundMissLoader(Class<?> type) throws Exception {
    Method cacheFronted = type.getMethod("getByBlobUri", String.class);
    assertFalse(
        cacheFronted.isAnnotationPresent(BoundMetadataIo.class),
        () -> type.getSimpleName() + ".getByBlobUri must admit only its cache miss");
  }

  private static boolean isWrite(Method method) {
    return Set.of("create", "update", "delete", "deleteWithPrecondition")
        .contains(method.getName());
  }

  private static boolean isCacheFrontedRead(Method method) {
    return method.getName().equals("getByBlobUri")
        && method.getParameterCount() == 1
        && method.getParameterTypes()[0] == String.class;
  }
}
