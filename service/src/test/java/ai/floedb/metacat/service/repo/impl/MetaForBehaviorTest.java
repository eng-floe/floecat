package ai.floedb.metacat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.common.rpc.*;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository;
import ai.floedb.metacat.storage.InMemoryBlobStore;
import ai.floedb.metacat.storage.InMemoryPointerStore;
import com.google.protobuf.util.Timestamps;
import java.time.Clock;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class MetaForBehaviorTest {

  final Clock clock = Clock.systemUTC();

  @Test
  void metaForExceptionTest() {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var tbls = new TableRepository(ptr, blobs);

    var tblId =
        ResourceId.newBuilder()
            .setTenantId("t-1")
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();

    assertThrows(BaseResourceRepository.NotFoundException.class, () -> tbls.metaFor(tblId));

    assertDoesNotThrow(() -> tbls.metaForSafe(tblId));
  }

  @Test
  void metaForVersionTest() {
    var ptr = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    var cats = new CatalogRepository(ptr, blobs);

    var catId =
        ResourceId.newBuilder()
            .setTenantId("t-1")
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    cats.create(
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

    var m1 = cats.metaFor(catId);
    cats.update(update, m1.getPointerVersion());
    var m2 = cats.metaFor(catId);

    assertTrue(m2.getPointerVersion() > m1.getPointerVersion(), "pointer version must increase");
  }
}
