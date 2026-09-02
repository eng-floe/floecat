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

package ai.floedb.floecat.service.repo.cache;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class AuthoritativePointerStoreTest {

  @Test
  void ordinaryReadsUseTheConsistentDoor() {
    String key = "/test/pointer";
    Pointer stale = pointer(key, "s3://stale", 1L);
    Pointer fresh = pointer(key, "s3://fresh", 2L);
    var delegate =
        new InMemoryPointerStore() {
          @Override
          public Optional<Pointer> get(String ignored) {
            return Optional.of(stale);
          }

          @Override
          public Optional<Pointer> getConsistent(String ignored) {
            return Optional.of(fresh);
          }
        };

    assertThat(AuthoritativePointerStore.of(delegate).get(key)).contains(fresh);
  }

  @Test
  void everyStoreOperationIsExplicitlyRoutedByTheView() {
    Set<String> routed =
        java.util.Arrays.stream(AuthoritativePointerStore.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(Method::getName)
            .collect(Collectors.toSet());
    Set<String> storeOperations =
        java.util.Arrays.stream(PointerStore.class.getMethods())
            .filter(method -> !Modifier.isStatic(method.getModifiers()))
            .map(Method::getName)
            .collect(Collectors.toSet());

    assertThat(routed)
        .as("a new PointerStore operation must choose authoritative-read or forwarding semantics")
        .containsAll(storeOperations);
  }

  private static Pointer pointer(String key, String uri, long version) {
    return Pointer.newBuilder().setKey(key).setBlobUri(uri).setVersion(version).build();
  }
}
