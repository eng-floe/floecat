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

import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext.CatalogOverlayEntry;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext.CatalogOverlayListingProvider;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** The catalog overlays of an account, as sys.catalog_overlay lists them. */
public final class CatalogSurfaceOverlays {
  private static final int SYSTEM_SCAN_PAGE_SIZE = 1_000;

  private final CatalogOverlayRepository overlayRepo;

  public CatalogSurfaceOverlays(CatalogOverlayRepository overlayRepo) {
    this.overlayRepo = Objects.requireNonNull(overlayRepo, "overlayRepo");
  }

  /** The overlays {@code accountId} owns, read when the scan asks for them. */
  public CatalogOverlayListingProvider overlayListing(String accountId) {
    return () -> listOverlayEntries(accountId);
  }

  List<CatalogOverlayEntry> listOverlayEntries(String accountId) {
    List<CatalogOverlayEntry> entries = new ArrayList<>();
    String token = "";
    do {
      var next = new StringBuilder();
      for (CatalogOverlay overlay :
          overlayRepo.list(accountId, SYSTEM_SCAN_PAGE_SIZE, token, next)) {
        entries.add(overlayEntry(overlay));
      }
      token = next.toString();
    } while (!token.isBlank());
    return entries;
  }

  private static CatalogOverlayEntry overlayEntry(CatalogOverlay overlay) {
    return new CatalogOverlayEntry(
        overlay.getResourceId(),
        overlay.getDisplayName(),
        overlay.hasIntegrationId() ? overlay.getIntegrationId() : null,
        overlay.hasCatalogId() ? overlay.getCatalogId() : null,
        overlay.hasCreatedAt() ? instant(overlay.getCreatedAt()) : null,
        overlay.hasUpdatedAt() ? instant(overlay.getUpdatedAt()) : null);
  }

  private static Instant instant(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }
}
