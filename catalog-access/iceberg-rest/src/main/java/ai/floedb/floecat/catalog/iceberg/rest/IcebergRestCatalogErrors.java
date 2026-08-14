/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.catalog.iceberg.rest;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;

final class IcebergRestCatalogErrors {
  private IcebergRestCatalogErrors() {}

  static <T> T call(String operation, Supplier<T> action) {
    try {
      return action.get();
    } catch (RuntimeException failure) {
      throw translate(operation, failure);
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

  static RuntimeException translate(String operation, RuntimeException failure) {
    if (failure instanceof CatalogAccessException) {
      return failure;
    }
    String safeMessage = "Upstream catalog " + operation + " failed";
    if (failure instanceof NotAuthorizedException) {
      return new CatalogAccessException(
          CatalogAccessException.Code.UNAUTHENTICATED, safeMessage, failure);
    }
    if (failure instanceof ForbiddenException) {
      return new CatalogAccessException(
          CatalogAccessException.Code.PERMISSION_DENIED, safeMessage, failure);
    }
    if (failure instanceof NotFoundException) {
      return new CatalogAccessException(
          CatalogAccessException.Code.NOT_FOUND, safeMessage, failure);
    }
    if (failure instanceof BadRequestException) {
      return new CatalogAccessException(
          CatalogAccessException.Code.INVALID_CONFIGURATION, safeMessage, failure);
    }
    if (hasCause(failure, SocketTimeoutException.class)
        || hasCause(failure, TimeoutException.class)) {
      return new CatalogAccessException(CatalogAccessException.Code.TIMEOUT, safeMessage, failure);
    }
    if (failure instanceof ServiceUnavailableException
        || failure instanceof ServiceFailureException
        || failure instanceof RESTException
        || failure instanceof RuntimeIOException) {
      return new CatalogAccessException(
          CatalogAccessException.Code.UNAVAILABLE, safeMessage, failure);
    }
    return failure;
  }

  private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
    for (Throwable cause = failure;
        cause != null && cause.getCause() != cause;
        cause = cause.getCause()) {
      if (type.isInstance(cause)) {
        return true;
      }
    }
    return false;
  }
}
