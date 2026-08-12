/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.repo.model;

public record CatalogOverlayKey(String accountId, String overlayId, String sha256)
    implements ResourceKey {
  public CatalogOverlayKey(String accountId, String overlayId) {
    this(accountId, overlayId, "");
  }
}
