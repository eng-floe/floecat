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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.util.MetadataReadPolicy;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/** Verifies CDI and repository composition route metadata reads through explicit admission. */
@QuarkusTest
class MetadataIoAdmissionWiringTest {

  @Inject MetadataResourceReader metadataReads;

  @Test
  void containerProvidesAnAdmittedMetadataReadPolicy() {
    assertFalse(MetadataIoRunner.isRunningAdmittedOperation());
    assertTrue(
        metadataReads.read(MetadataIoRunner::isRunningAdmittedOperation),
        "the CDI metadata-read policy must establish admission around its operation");
    assertFalse(MetadataIoRunner.isRunningAdmittedOperation());
  }

  @Test
  void everyAdmittedRepositoryUsesOnePolicyForReadsAndColdBlobLoads() {
    CountingMetadataReadPolicy policy = new CountingMetadataReadPolicy();
    ImmutableBlobCache cache = new ImmutableBlobCache(true, 1024 * 1024, Duration.ofMinutes(5));
    InMemoryPointerStore pointers = new InMemoryPointerStore();
    InMemoryBlobStore blobs = new InMemoryBlobStore();

    CatalogRepository catalogs = new CatalogRepository(pointers, blobs, cache, policy);
    assertPolicyInvoked(
        "CatalogRepository", () -> catalogs.count("acct"), catalogs::getByBlobUri, policy);

    NamespaceRepository namespaces = new NamespaceRepository(pointers, blobs, cache, policy);
    assertPolicyInvoked(
        "NamespaceRepository",
        () -> namespaces.count("acct", "catalog", List.of()),
        namespaces::getByBlobUri,
        policy);

    TableRepository tables = new TableRepository(pointers, blobs, cache, policy);
    assertPolicyInvoked(
        "TableRepository",
        () -> tables.count("acct", "catalog", "namespace"),
        tables::getByBlobUri,
        policy);

    ViewRepository views = new ViewRepository(pointers, blobs, cache, policy);
    assertPolicyInvoked(
        "ViewRepository",
        () -> views.count("acct", "catalog", "namespace"),
        views::getByBlobUri,
        policy);
  }

  /** Assert one outer read and one cache-miss load use the repository's selected policy. */
  private static void assertPolicyInvoked(
      String repository,
      Supplier<?> read,
      Function<String, Optional<?>> getByBlobUri,
      CountingMetadataReadPolicy policy) {
    read.get();
    assertEquals(1, policy.reads.getAndSet(0), repository + " must route reads through its policy");

    assertTrue(getByBlobUri.apply("blob://test/missing-" + repository).isEmpty());
    assertEquals(
        1,
        policy.loads.getAndSet(0),
        repository + " must route cold blob loads through its policy");
    assertEquals(0, policy.reads.get(), repository + " must not admit the cache probe itself");
  }

  /** Policy that records outer reads and cold blob loads independently. */
  private static final class CountingMetadataReadPolicy implements MetadataReadPolicy {
    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicInteger loads = new AtomicInteger();

    @Override
    public <T> T read(Supplier<T> reader) {
      reads.incrementAndGet();
      return reader.get();
    }

    @Override
    public <T> T load(Supplier<T> loader) {
      loads.incrementAndGet();
      return loader.get();
    }
  }
}
