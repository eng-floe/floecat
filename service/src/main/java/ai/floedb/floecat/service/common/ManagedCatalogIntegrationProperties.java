/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package ai.floedb.floecat.service.common;

import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.util.Map;

/** Property names reserved for Catalog Integration reconciliation provenance. */
public final class ManagedCatalogIntegrationProperties {
  private static final String OVERLAY_PREFIX = "floecat.catalog-overlay";
  private static final String INTEGRATION_PREFIX = "floecat.catalog-integration";

  private ManagedCatalogIntegrationProperties() {}

  public static void validateUserProperties(
      Map<String, String> values, String corr, String fieldName) {
    if (values == null || values.isEmpty()) return;
    for (String key : values.keySet()) {
      if (!isReserved(key)) continue;
      throw GrpcErrors.invalidArgument(
          corr, null, Map.of("field", fieldName, "key", String.valueOf(key)));
    }
  }

  public static boolean containsReservedProperty(Map<String, String> values) {
    return values != null
        && values.keySet().stream().anyMatch(ManagedCatalogIntegrationProperties::isReserved);
  }

  public static boolean isReserved(String key) {
    return hasPrefix(key, OVERLAY_PREFIX) || hasPrefix(key, INTEGRATION_PREFIX);
  }

  private static boolean hasPrefix(String key, String prefix) {
    return key != null && (key.equals(prefix) || key.startsWith(prefix + "."));
  }
}
