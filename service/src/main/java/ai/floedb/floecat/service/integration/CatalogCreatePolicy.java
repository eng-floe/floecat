/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.integration;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.FIELD;

import ai.floedb.floecat.common.rpc.CreateMode;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.util.Map;

final class CatalogCreatePolicy {
  private CatalogCreatePolicy() {}

  static Selection validate(CreateMode mode, String idempotencyKey, String correlationId) {
    String key = idempotencyKey == null ? "" : idempotencyKey.trim();
    if (mode == CreateMode.UNRECOGNIZED) {
      throw GrpcErrors.invalidArgument(correlationId, FIELD, Map.of("field", "create_mode"));
    }
    if (!key.isEmpty() && mode != CreateMode.CM_ERROR_IF_EXISTS) {
      throw GrpcErrors.invalidArgument(correlationId, FIELD, Map.of("field", "idempotency"));
    }
    return new Selection(mode, key);
  }

  record Selection(CreateMode mode, String idempotencyKey) {}
}
