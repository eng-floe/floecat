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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import com.google.protobuf.Timestamp;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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

  @Test
  void containerWrapsBoundMetadataIoMethodsInAdmission() {
    assertFalse(MetadataIoRunner.isRunningAdmittedOperation());
    assertTrue(
        probe.observedAdmission(),
        "the @BoundMetadataIo method must run under admission — the interceptor did not fire");
    assertFalse(MetadataIoRunner.isRunningAdmittedOperation());
  }

  @Test
  void queryPathPointerReadsRemainBoundAtTheRepositoryBoundary() throws Exception {
    assertBound(
        TableRepository.class,
        "relationNameClaim",
        String.class,
        String.class,
        String.class,
        String.class);
    assertBound(CatalogRepository.class, "pointerMetaForSafe", ResourceId.class);
    assertBound(NamespaceRepository.class, "pointerMetaForSafe", ResourceId.class);
    assertBound(TableRepository.class, "pointerMetaForSafe", ResourceId.class);
    assertBound(ViewRepository.class, "pointerMetaForSafe", ResourceId.class);
    assertBound(TableRepository.class, "blobEtag", String.class);
    assertMetadataReadsBound(CatalogRepository.class);
    assertMetadataReadsBound(NamespaceRepository.class);
    assertMetadataReadsBound(TableRepository.class);
    assertMetadataReadsBound(ViewRepository.class);
  }

  private static void assertMetadataReadsBound(Class<?> type) throws Exception {
    assertBound(type, "metaFor", ResourceId.class);
    assertBound(type, "metaFor", ResourceId.class, Timestamp.class);
    assertBound(type, "metaForSafe", ResourceId.class);
  }

  private static void assertBound(Class<?> type, String method, Class<?>... parameterTypes)
      throws Exception {
    assertTrue(
        type.getMethod(method, parameterTypes).isAnnotationPresent(BoundMetadataIo.class),
        () -> type.getSimpleName() + "." + method + " bypasses metadata-I/O admission");
  }
}
