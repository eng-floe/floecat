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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.CatalogKey;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.MetadataRepositoryFactory;
import ai.floedb.floecat.service.testsupport.ConcurrentTestSupport;
import ai.floedb.floecat.storage.spi.PointerStore;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** Verifies CDI composes every admitted repository family through the one repository factory. */
@QuarkusTest
class MetadataIoAdmissionWiringTest {

  @Inject MetadataIoRunner admission;
  @Inject PointerStore pointers;
  @Inject CatalogRepository catalogs;

  @Test
  void everyRepositoryFamilyInjectsTheAdmittedFactory() {
    for (Class<?> repository :
        List.of(
            CatalogRepository.class,
            NamespaceRepository.class,
            TableRepository.class,
            ViewRepository.class)) {
      assertThat(injectedConstructors(repository))
          .as(repository.getSimpleName())
          .singleElement()
          .satisfies(
              constructor ->
                  assertThat(constructor.getParameterTypes())
                      .contains(MetadataRepositoryFactory.class));
    }
  }

  @Test
  void repositoryPointerAndBlobReadsWaitForMetadataAdmission() throws Exception {
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("wiring-" + UUID.randomUUID())
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    Catalog expected =
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("admitted").build();
    catalogs.create(expected);
    CatalogKey key = new CatalogKey(catalogId.getAccountId(), catalogId.getId());
    String blobUri =
        pointers.get(Schemas.CATALOG.canonicalPointerForKey.apply(key)).orElseThrow().getBlobUri();

    CountDownLatch entered = new CountDownLatch(admission.capacity());
    CountDownLatch release = new CountDownLatch(1);
    var failures =
        new CancellableCallRunner.FailureMessages("cancelled", "interrupted while waiting");
    try (ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor()) {
      List<CompletableFuture<Void>> holders =
          IntStream.range(0, admission.capacity())
              .mapToObj(
                  ignored ->
                      CompletableFuture.runAsync(
                          () ->
                              admission.callWithoutCancellation(
                                  () -> {
                                    entered.countDown();
                                    ConcurrentTestSupport.awaitUninterruptibly(release);
                                    return null;
                                  },
                                  failures),
                          workers))
              .toList();
      try {
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        CompletableFuture<Catalog> pointerRead =
            CompletableFuture.supplyAsync(() -> catalogs.getById(catalogId).orElseThrow(), workers);
        CompletableFuture<Catalog> blobRead =
            CompletableFuture.supplyAsync(
                () -> catalogs.getByBlobUri(blobUri).orElseThrow(), workers);
        ConcurrentTestSupport.await(() -> admission.admissionWaiters() == 2, Duration.ofSeconds(5));
        assertThat(pointerRead).isNotDone();
        assertThat(blobRead).isNotDone();

        release.countDown();
        assertThat(pointerRead.get(5, TimeUnit.SECONDS)).isEqualTo(expected);
        assertThat(blobRead.get(5, TimeUnit.SECONDS)).isEqualTo(expected);
      } finally {
        release.countDown();
        CompletableFuture.allOf(holders.toArray(CompletableFuture[]::new)).get(5, TimeUnit.SECONDS);
      }
      ConcurrentTestSupport.await(() -> admission.permitsInUse() == 0, Duration.ofSeconds(5));
    }
  }

  private static List<Constructor<?>> injectedConstructors(Class<?> repository) {
    return java.util.Arrays.stream(repository.getConstructors())
        .filter(constructor -> constructor.isAnnotationPresent(Inject.class))
        .toList();
  }
}
