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
import java.util.Locale;
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

  @Test
  void unimplementedUsesGeneratedMessageKeyAndCanonicalCode() {
    StatusRuntimeException ex =
        GrpcErrors.unimplemented(
            "corr-id",
            GeneratedErrorMessages.MessageKey.STATS_ENGINE_NOT_IMPLEMENTED,
            Map.of("operation", "list_target_stats", "reason", "no engine"));

    Status statusProto = StatusProto.fromThrowable(ex);
    assertNotNull(statusProto);
    assertEquals(io.grpc.Status.Code.UNIMPLEMENTED.value(), statusProto.getCode());

    FloecatStatus floecatStatus = FloecatStatus.fromThrowable(ex);
    assertNotNull(floecatStatus);
    assertEquals(io.grpc.Status.Code.UNIMPLEMENTED, floecatStatus.canonicalCode());
    assertEquals(ErrorCode.MC_UNAVAILABLE, floecatStatus.errorCode());
    assertEquals("stats.engine.not.implemented", floecatStatus.messageKey());
    assertEquals("list_target_stats", floecatStatus.params().get("operation"));
  }

  @Test
  void clampDetailKeepsHeadAndTailAndElidesMiddle() {
    // Shape mirrors an AWS SDK validation message: the actionable constraint trails the
    // embedded request/value dump, so the truncation must preserve both ends.
    String head = "1 validation error detected: Value '";
    String tail =
        "' at 'transactItems' failed to satisfy constraint: "
            + "Member must have length less than or equal to 100";
    String oversized = head + "A".repeat(40_000) + tail;

    String clamped = GrpcErrors.clampDetail(oversized);

    assertTrue(clamped.length() < 700, "clamped detail should be bounded, was " + clamped.length());
    assertTrue(clamped.startsWith("1 validation error detected"), clamped);
    assertTrue(
        clamped.endsWith("Member must have length less than or equal to 100"),
        "tail constraint must survive: " + clamped);
    assertTrue(clamped.contains("elided"), clamped);
  }

  @Test
  void clampDetailLeavesShortMessagesIntact() {
    assertEquals("boom", GrpcErrors.clampDetail("boom"));
    assertEquals("", GrpcErrors.clampDetail(null));
    // Whitespace runs (multi-line SDK dumps) are collapsed so they stay header-safe.
    assertEquals("a b c", GrpcErrors.clampDetail("a\n\t b   c"));
  }

  @Test
  void internalErrorBoundsRootMessageAndRenderedMessageForOversizedCause() {
    String dump = "X".repeat(40_000);
    StatusRuntimeException ex =
        GrpcErrors.internal("corr-id", null, null, new RuntimeException(dump));

    Status statusProto = StatusProto.fromThrowable(ex);
    Error error = detailOfType(statusProto, Error.class);
    assertNotNull(error, "Error detail should be present");

    String rootMessage = error.getParamsMap().get("root_message");
    assertNotNull(rootMessage, "root_message param should be set");
    assertTrue(
        rootMessage.length() < 700, "root_message must be clamped, was " + rootMessage.length());

    // The MC_INTERNAL catalog template interpolates {root_message}; LocalizeErrorsInterceptor
    // re-renders it into grpc-message + grpc-status-details-bin. With the source clamped, the
    // rendered message stays well under the HTTP/2 header-list-size limit.
    String rendered = new MessageCatalog(Locale.ENGLISH).render(error);
    assertTrue(rendered.contains("root="), "template should still render the cause: " + rendered);
    assertTrue(
        rendered.length() < 2048,
        "rendered message must stay header-safe, was " + rendered.length());

    // The whole serialized status (what becomes grpc-status-details-bin) must be bounded too.
    assertTrue(
        statusProto.toByteArray().length < 4096,
        "serialized status trailer must be bounded, was " + statusProto.toByteArray().length);
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
