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
package ai.floedb.floecat.gateway.iceberg.rest.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.catalog.rpc.ResolveRelationResult;
import ai.floedb.floecat.catalog.rpc.ResolveRelationsResponse;
import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.engine.catalog.RelationResults;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import org.junit.jupiter.api.Test;

/**
 * A per-relation resolution failure arrives in band, so without a mapper it reaches the client as a
 * 500 and loses the status the server actually reported.
 */
class RelationResolutionExceptionMapperTest {

  private final RelationResolutionExceptionMapper mapper = new RelationResolutionExceptionMapper();

  @Test
  void permissionDeniedBecomesForbiddenRatherThanServerError() {
    var response = failedWith(ErrorCode.MC_PERMISSION_DENIED, "access denied");

    var thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            RelationResults.RelationResolutionException.class,
            () -> RelationResults.requireResolved(response));

    try (var mapped = mapper.toResponse(thrown)) {
      assertEquals(403, mapped.getStatus());
      var body = (IcebergErrorResponse) mapped.getEntity();
      assertEquals("ForbiddenException", body.error().type());
      assertEquals("access denied", body.error().message());
    }
  }

  @Test
  void invalidArgumentBecomesBadRequest() {
    var response = failedWith(ErrorCode.MC_INVALID_ARGUMENT, "bad name");

    var thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            RelationResults.RelationResolutionException.class,
            () -> RelationResults.requireResolved(response));

    try (var mapped = mapper.toResponse(thrown)) {
      assertEquals(400, mapped.getStatus());
      assertEquals(
          "ValidationException", ((IcebergErrorResponse) mapped.getEntity()).error().type());
    }
  }

  @Test
  void anUnmappedCodeStaysAServerError() {
    var response = failedWith(ErrorCode.MC_INTERNAL, "boom");

    var thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            RelationResults.RelationResolutionException.class,
            () -> RelationResults.requireResolved(response));

    try (var mapped = mapper.toResponse(thrown)) {
      assertEquals(500, mapped.getStatus());
    }
  }

  private static ResolveRelationsResponse failedWith(ErrorCode code, String message) {
    return ResolveRelationsResponse.newBuilder()
        .addResults(
            ResolveRelationResult.newBuilder()
                .setError(Error.newBuilder().setCode(code).setMessage(message)))
        .build();
  }
}
