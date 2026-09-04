/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.catalog.unity;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.client.unity.UnityCatalogException;
import java.util.function.Supplier;

final class UnityCatalogErrors {
  private UnityCatalogErrors() {}

  static <T> T call(String operation, Supplier<T> action) {
    try {
      return action.get();
    } catch (CatalogAccessException failure) {
      throw failure;
    } catch (UnityCatalogException failure) {
      throw translate(operation, failure);
    } catch (IllegalArgumentException failure) {
      throw new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION,
          "Unity Catalog " + operation + " configuration is invalid",
          failure);
    }
  }

  static void run(String operation, Runnable action) {
    call(
        operation,
        () -> {
          action.run();
          return null;
        });
  }

  private static CatalogAccessException translate(String operation, UnityCatalogException failure) {
    CatalogAccessException authenticationFailure = catalogAccessCause(failure);
    if (authenticationFailure != null) {
      return authenticationFailure;
    }
    CatalogAccessException.Code code =
        switch (failure.failure()) {
          case UNAUTHENTICATED -> CatalogAccessException.Code.UNAUTHENTICATED;
          case PERMISSION_DENIED -> CatalogAccessException.Code.PERMISSION_DENIED;
          case NOT_FOUND -> CatalogAccessException.Code.NOT_FOUND;
          case INTERRUPTED -> CatalogAccessException.Code.TIMEOUT;
          case INVALID_REQUEST -> CatalogAccessException.Code.INVALID_CONFIGURATION;
          case RATE_LIMITED, SERVER_ERROR, TRANSPORT, TRANSIENT, OTHER ->
              CatalogAccessException.Code.UNAVAILABLE;
          case INVALID_RESPONSE -> CatalogAccessException.Code.INTERNAL;
        };
    return new CatalogAccessException(code, "Unity Catalog " + operation + " failed", failure);
  }

  private static CatalogAccessException catalogAccessCause(Throwable failure) {
    // A visited set, not a self-reference check: the latter misses a two-element cycle, which
    // initCause can build and which would spin this walk forever inside error translation. Same
    // traversal as SourceCatalogCredentialVendor.findCatalogAccessFailure, same guard.
    java.util.Set<Throwable> seen = new java.util.HashSet<>();
    for (Throwable cause = failure.getCause();
        cause != null && seen.add(cause);
        cause = cause.getCause()) {
      if (cause instanceof CatalogAccessException catalogAccessException) {
        return catalogAccessException;
      }
    }
    return null;
  }
}
