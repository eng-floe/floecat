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

package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.*;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.util.Timestamps;
import java.time.Clock;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetaForBehaviorTest {

  final Clock clock = Clock.systemUTC();

  private CatalogRepository catalogRepo;
  private TableRepository tableRepo;
  private PointerStore ptr;
  private BlobStore blobs;

  @BeforeEach
  void setUp() {
    ptr = new InMemoryPointerStore();
    blobs = new InMemoryBlobStore();
    catalogRepo = new CatalogRepository(ptr, blobs);
    tableRepo = new TableRepository(ptr, blobs);
  }

  @Test
  void metaForExceptionTest() {
    var tblId =
        ResourceId.newBuilder()
            .setAccountId("t-1")
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();

    assertThrows(BaseResourceRepository.NotFoundException.class, () -> tableRepo.metaFor(tblId));

    assertDoesNotThrow(() -> tableRepo.metaForSafe(tblId));
  }

  @Test
  void metaForVersionTest() {
    var catId =
        ResourceId.newBuilder()
            .setAccountId("t-1")
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    catalogRepo.create(
        Catalog.newBuilder()
            .setResourceId(catId)
            .setDisplayName("sales")
            .setCreatedAt(Timestamps.fromMillis(clock.millis()))
            .build());

    var update =
        Catalog.newBuilder()
            .setResourceId(catId)
            .setDisplayName("sales_new")
            .setCreatedAt(Timestamps.fromMillis(clock.millis()))
            .build();

    var m1 = catalogRepo.metaFor(catId);
    catalogRepo.update(update, m1.getPointerVersion());
    var m2 = catalogRepo.metaFor(catId);

    assertTrue(m2.getPointerVersion() > m1.getPointerVersion(), "pointer version must increase");
  }
}
