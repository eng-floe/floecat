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

package ai.floedb.floecat.service.error.impl;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.DebugInfo;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GrpcErrorsTest {
  @Test
  void invalidArgumentAddsBadRequestAndMessage() {
    StatusRuntimeException ex =
        GrpcErrors.invalidArgument(
            "corr-id",
            GeneratedErrorMessages.MessageKey.CATALOG_MISSING,
            Map.of("field", "catalog_id"));
    Status statusProto = StatusProto.fromThrowable(ex);
    assertNotNull(statusProto, "expected rpc status in exception");
    assertEquals(
        io.grpc.Status.Code.INVALID_ARGUMENT.value(),
        statusProto.getCode(),
        "canonical code should be INVALID_ARGUMENT");
    assertFalse(statusProto.getMessage().isBlank(), "status message should not be blank");

    BadRequest badRequest = detailOfType(statusProto, BadRequest.class);
    assertNotNull(badRequest, "BadRequest detail should be present for validation failures");
    assertEquals("catalog_id", badRequest.getFieldViolations(0).getField());
    assertEquals(statusProto.getMessage(), badRequest.getFieldViolations(0).getDescription());

    FloecatStatus floecatStatus = FloecatStatus.fromThrowable(ex);
    assertNotNull(floecatStatus, "FloecatStatus helper should decode the error detail");
    assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, floecatStatus.canonicalCode());
    assertEquals(ErrorCode.MC_INVALID_ARGUMENT, floecatStatus.errorCode());
    assertEquals("catalog_id", floecatStatus.params().get("field"));
    assertEquals("catalog.missing", floecatStatus.messageKey());
    assertEquals("corr-id", floecatStatus.correlationId());
  }

  @Test
  void floecatStatusExtractsDebugInfo() {
    Error error =
        Error.newBuilder()
            .setCode(ErrorCode.MC_INTERNAL)
            .setCorrelationId("corr")
            .setMessageKey("internal.problem")
            .setMessage("Internal problem")
            .putParams("field", "value")
            .build();
    DebugInfo debugInfo =
        DebugInfo.newBuilder()
            .setDetail("io.example.Inner: reason")
            .addStackEntries("frame1")
            .build();
    Status statusProto =
        Status.newBuilder()
            .setCode(io.grpc.Status.Code.INTERNAL.value())
            .setMessage("Internal error. correlation_id=corr")
            .addDetails(Any.pack(error))
            .addDetails(Any.pack(debugInfo))
            .build();
    StatusRuntimeException ex = StatusProto.toStatusRuntimeException(statusProto);

    FloecatStatus parsed = FloecatStatus.fromThrowable(ex);
    assertNotNull(parsed);
    assertEquals(io.grpc.Status.Code.INTERNAL, parsed.canonicalCode());
    assertEquals(ErrorCode.MC_INTERNAL, parsed.errorCode());
    assertEquals("corr", parsed.correlationId());
    assertTrue(parsed.hasDebugInfo());
    assertEquals("frame1", parsed.debugInfo().getStackEntries(0));
  }

  @Test
  void requestValidationThrowsInvalidArgumentForBlankValue() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> RequestValidation.requireNonBlank("  ", "header", "cid", "header.required"));
    Status statusProto = StatusProto.fromThrowable(ex);
    BadRequest badRequest = detailOfType(statusProto, BadRequest.class);
    assertNotNull(badRequest);
    assertEquals("header", badRequest.getFieldViolations(0).getField());
  }

  private static <T extends com.google.protobuf.Message> T detailOfType(
      Status statusProto, Class<T> clazz) {
    for (Any detail : statusProto.getDetailsList()) {
      if (detail.is(clazz)) {
        try {
          return detail.unpack(clazz);
        } catch (Exception e) {
          fail("detail unpack failed", e);
        }
      }
    }
    return null;
  }
}
