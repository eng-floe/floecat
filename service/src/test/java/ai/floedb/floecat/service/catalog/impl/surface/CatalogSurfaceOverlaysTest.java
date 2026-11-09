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
package ai.floedb.floecat.service.catalog.impl.surface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext.CatalogOverlayEntry;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogSurfaceOverlaysTest {

  private CatalogOverlayRepository overlayRepo;
  private CatalogSurfaceOverlays surface;

  @BeforeEach
  void setup() {
    overlayRepo = mock(CatalogOverlayRepository.class);
    surface = new CatalogSurfaceOverlays(overlayRepo);
  }

  @Test
  void overlayListingMapsEachOverlay() {
    var overlayId = id("ov-1", ResourceKind.RK_CATALOG_OVERLAY);
    var integrationId = id("int-1", ResourceKind.RK_CATALOG_INTEGRATION);
    var catalogId = id("cat-1", ResourceKind.RK_CATALOG);
    when(overlayRepo.list(eq("acct"), anyInt(), eq(""), any(StringBuilder.class)))
        .thenReturn(
            List.of(
                CatalogOverlay.newBuilder()
                    .setResourceId(overlayId)
                    .setDisplayName("sales")
                    .setIntegrationId(integrationId)
                    .setCatalogId(catalogId)
                    .setCreatedAt(Timestamp.newBuilder().setSeconds(100).setNanos(5))
                    .setUpdatedAt(Timestamp.newBuilder().setSeconds(200))
                    .build()));

    List<CatalogOverlayEntry> entries = surface.overlayListing("acct").listCatalogOverlays();

    assertEquals(
        List.of(
            new CatalogOverlayEntry(
                overlayId,
                "sales",
                integrationId,
                catalogId,
                Instant.ofEpochSecond(100, 5),
                Instant.ofEpochSecond(200))),
        entries);
  }

  @Test
  void overlayListingLeavesUnsetFieldsNull() {
    var overlayId = id("ov-1", ResourceKind.RK_CATALOG_OVERLAY);
    when(overlayRepo.list(eq("acct"), anyInt(), eq(""), any(StringBuilder.class)))
        .thenReturn(
            List.of(
                CatalogOverlay.newBuilder()
                    .setResourceId(overlayId)
                    .setDisplayName("sales")
                    .build()));

    CatalogOverlayEntry entry = surface.overlayListing("acct").listCatalogOverlays().get(0);

    assertNull(entry.integrationId());
    assertNull(entry.catalogId());
    assertNull(entry.createdAt());
    assertNull(entry.updatedAt());
  }

  @Test
  void overlayListingReadsEveryPage() {
    when(overlayRepo.list(eq("acct"), anyInt(), eq(""), any(StringBuilder.class)))
        .thenAnswer(
            invocation -> {
              invocation.<StringBuilder>getArgument(3).append("page-2");
              return List.of(overlay("ov-1", "first"));
            });
    when(overlayRepo.list(eq("acct"), anyInt(), eq("page-2"), any(StringBuilder.class)))
        .thenReturn(List.of(overlay("ov-2", "second")));

    List<String> names =
        surface.overlayListing("acct").listCatalogOverlays().stream()
            .map(CatalogOverlayEntry::name)
            .toList();

    assertEquals(List.of("first", "second"), names);
  }

  @Test
  void overlayListingReadsNothingUntilTheScanAsks() {
    surface.overlayListing("acct");

    verifyNoInteractions(overlayRepo);
  }

  private static CatalogOverlay overlay(String id, String name) {
    return CatalogOverlay.newBuilder()
        .setResourceId(id(id, ResourceKind.RK_CATALOG_OVERLAY))
        .setDisplayName(name)
        .build();
  }

  private static ResourceId id(String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("acct").setId(id).setKind(kind).build();
  }
}
