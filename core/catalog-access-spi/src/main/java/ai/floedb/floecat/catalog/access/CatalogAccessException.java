/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.catalog.access;

import java.util.Objects;

/** A credential-safe provider failure that can be mapped onto a public service status. */
public final class CatalogAccessException extends RuntimeException {
  public enum Code {
    INVALID_CONFIGURATION,
    UNAUTHENTICATED,
    PERMISSION_DENIED,
    NOT_FOUND,
    UNAVAILABLE,
    TIMEOUT,
    UNSUPPORTED,
    INTERNAL
  }

  private final Code code;

  public CatalogAccessException(Code code, String safeMessage) {
    super(safeMessage);
    this.code = Objects.requireNonNull(code, "code");
  }

  public CatalogAccessException(Code code, String safeMessage, Throwable cause) {
    super(safeMessage, cause);
    this.code = Objects.requireNonNull(code, "code");
  }

  public Code code() {
    return code;
  }
}
