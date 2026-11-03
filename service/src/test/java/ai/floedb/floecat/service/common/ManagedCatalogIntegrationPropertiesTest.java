/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package ai.floedb.floecat.service.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ManagedCatalogIntegrationPropertiesTest {
  @Test
  void reservesOverlayAndIntegrationProvenanceProperties() {
    assertTrue(ManagedCatalogIntegrationProperties.isReserved("floecat.catalog-overlay.id"));
    assertTrue(
        ManagedCatalogIntegrationProperties.isReserved("floecat.catalog-integration.external-id"));
    assertTrue(ManagedCatalogIntegrationProperties.isReserved("floecat.catalog-overlay"));
  }

  @Test
  void permitsOrdinaryUserPropertiesAndSimilarNames() {
    assertFalse(ManagedCatalogIntegrationProperties.isReserved("owner"));
    assertFalse(ManagedCatalogIntegrationProperties.isReserved("floecat.catalog.id"));
    assertFalse(ManagedCatalogIntegrationProperties.isReserved("floecat.catalog-overlayed.id"));
  }
}
