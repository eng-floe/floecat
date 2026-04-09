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

package ai.floedb.floecat.service.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import ai.floedb.floecat.reconciler.impl.GrpcReconcilerBackend;
import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;

class RuntimeSelectedReconcilerBackendTest {

  @Test
  void selectsGrpcBackendWhenConfiguredRemote() {
    DirectReconcilerBackend direct = mock(DirectReconcilerBackend.class);
    GrpcReconcilerBackend grpc = mock(GrpcReconcilerBackend.class);

    RuntimeSelectedReconcilerBackend backend =
        new RuntimeSelectedReconcilerBackend(direct, grpc, "remote");

    assertThat(delegateOf(backend)).isSameAs(grpc);
  }

  @Test
  void defaultsToDirectBackend() {
    DirectReconcilerBackend direct = mock(DirectReconcilerBackend.class);
    GrpcReconcilerBackend grpc = mock(GrpcReconcilerBackend.class);

    RuntimeSelectedReconcilerBackend backend =
        new RuntimeSelectedReconcilerBackend(direct, grpc, "local");

    assertThat(delegateOf(backend)).isSameAs(direct);
  }

  private static Object delegateOf(RuntimeSelectedReconcilerBackend backend) {
    try {
      Field field = RuntimeSelectedReconcilerBackend.class.getDeclaredField("delegate");
      field.setAccessible(true);
      return field.get(backend);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }
}
