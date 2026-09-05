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

package ai.floedb.floecat.storage.errors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.ErrorCode;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Test;

class SourceCatalogVendingGrpcStatusTest {
  @Test
  void aRefusalIsRecognizedUnderEveryStatusCodeItCanCarry() {
    var precondition = SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused("upstream gone");
    var unauthenticated =
        SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(
            Status.Code.UNAUTHENTICATED, ErrorCode.MC_UNAUTHENTICATED, "catalog rejected us", null);
    var forbidden =
        SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(
            Status.Code.PERMISSION_DENIED, ErrorCode.MC_PERMISSION_DENIED, "no grant", null);

    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(precondition));
    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(unauthenticated));
    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(forbidden));

    // The code still says what kind of refusal it was; only the reason says where it came from.
    assertEquals(Status.Code.UNAUTHENTICATED, unauthenticated.getStatus().getCode());
    assertEquals(Status.Code.PERMISSION_DENIED, forbidden.getStatus().getCode());
  }

  @Test
  void aServiceLevelAuthFailureIsNotAVendingRefusal() {
    var serviceAuthFailure =
        Status.UNAUTHENTICATED.withDescription("caller token expired").asRuntimeException();

    assertFalse(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(serviceAuthFailure));
  }

  @Test
  void theOriginatingFailureStaysAttachedForServerSideLogs() {
    var upstream = new IllegalStateException("401 from https://catalog.example/v1/oauth/tokens");

    var refusal =
        SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused(
            Status.Code.UNAUTHENTICATED,
            ErrorCode.MC_UNAUTHENTICATED,
            "catalog rejected us",
            upstream);

    assertSame(upstream, refusal.getStatus().getCause());
    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(refusal));
    // The wire-visible description carries only the safe detail, never the upstream's own text.
    assertEquals("catalog rejected us", refusal.getStatus().getDescription());
  }

  @Test
  void theRetryableConditionIsIdentifiableAndNotARefusal() {
    var unavailable =
        SourceCatalogVendingGrpcStatus.sourceCatalogVendUnavailable("credential expired", null);

    assertEquals(Status.Code.UNAVAILABLE, unavailable.getStatus().getCode());
    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendUnavailable(unavailable));
    // Retryable: the reconciler classifies only the refusal reasons terminally.
    assertFalse(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(unavailable));
    // Carries a floecat error detail, so BaseServiceImpl.toStatus passes it through untouched
    // instead of rebuilding it with a synthesized message.
    assertTrue(StatusProto.fromThrowable(unavailable).getDetailsCount() > 0);
  }

  @Test
  void aReasonFromANodeThatPredatesTheDomainParamStillMatches() {
    // What an older service node puts on the wire. LocalizeErrorsInterceptor rebuilds every status
    // with clearDetails().addDetails(the localized Error), so the ErrorInfo never survives the RPC
    // and this shape is all a reconciler sees. Refusing it would, for the length of a rolling
    // deploy, stop the delegation fall-back being absorbed for existing connectors and turn
    // terminal vending failures into retries until the attempt budget was spent.
    for (String reason :
        new String[] {
          SourceCatalogVendingGrpcStatus.SOURCE_CATALOG_VEND_REFUSED_REASON,
          SourceCatalogVendingGrpcStatus.VENDED_CREDENTIALS_NOT_REFRESHABLE_REASON
        }) {
      var legacy =
          legacyStatus(Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, reason);

      assertTrue(
          SourceCatalogVendingGrpcStatus.SOURCE_CATALOG_VEND_REFUSED_REASON.equals(reason)
              ? SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(legacy)
              : SourceCatalogVendingGrpcStatus.isVendedCredentialsNotRefreshable(legacy),
          reason);
    }

    var legacyNoAuthority =
        legacyStatus(
            Status.Code.INVALID_ARGUMENT,
            ErrorCode.MC_INVALID_ARGUMENT,
            SourceCatalogVendingGrpcStatus.NO_MATCHING_STORAGE_AUTHORITY_REASON);

    assertTrue(SourceCatalogVendingGrpcStatus.isNoMatchingStorageAuthority(legacyNoAuthority));
  }

  @Test
  void aReasonNewerThanTheDomainParamHasNoLegacyShape() {
    // SOURCE_CATALOG_VEND_UNAVAILABLE arrives with this change, so a domain-less detail claiming it
    // is not an older node that predates the param -- there is no such node. Accepting it would
    // reopen the gap the param closes, and it would tell a reason added later that its historical
    // code was MC_PRECONDITION_FAILED whether or not it was ever raised under one.
    var impostor =
        legacyStatus(
            Status.Code.UNAVAILABLE,
            ErrorCode.MC_PRECONDITION_FAILED,
            SourceCatalogVendingGrpcStatus.SOURCE_CATALOG_VEND_UNAVAILABLE_REASON);

    assertFalse(SourceCatalogVendingGrpcStatus.isSourceCatalogVendUnavailable(impostor));

    // The three that do predate it still match, so the rolling-deploy compatibility is intact.
    assertTrue(
        SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(
            legacyStatus(
                Status.Code.FAILED_PRECONDITION,
                ErrorCode.MC_PRECONDITION_FAILED,
                SourceCatalogVendingGrpcStatus.SOURCE_CATALOG_VEND_REFUSED_REASON)));
  }

  @Test
  void aLegacyDetailUnderTheWrongErrorCodeIsNotAccepted() {
    // The ErrorCode is all a domain-less detail carries to narrow with, so it has to carry weight.
    var mismatched =
        legacyStatus(
            Status.Code.FAILED_PRECONDITION,
            ErrorCode.MC_INTERNAL,
            SourceCatalogVendingGrpcStatus.SOURCE_CATALOG_VEND_REFUSED_REASON);

    assertFalse(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(mismatched));
  }

  /** A status shaped the way a node emitted it before the domain param existed. */
  private static io.grpc.StatusRuntimeException legacyStatus(
      Status.Code code, ErrorCode errorCode, String reason) {
    return StatusProto.toStatusRuntimeException(
        com.google.rpc.Status.newBuilder()
            .setCode(code.value())
            .setMessage("legacy")
            .addDetails(
                com.google.protobuf.Any.pack(
                    ai.floedb.floecat.common.rpc.Error.newBuilder()
                        .setCode(errorCode)
                        .setMessage("legacy")
                        .putParams("reason", reason)
                        .build()))
            .build());
  }

  @Test
  void aDetailClaimingAnotherDomainCannotImpersonateAVendingReason() {
    // What the domain param can and cannot do. It rejects a detail that names a different domain,
    // which is the case it was added for. It cannot reject a detail that names no domain at all:
    // that shape is byte-identical to what a node predating the param emits, and refusing it would
    // break a rolling deploy -- see aReasonFromANodeThatPredatesTheDomainParamStillMatches. Those
    // fall back to the historical ErrorCode, which is all such a detail carries.
    var foreign =
        com.google.rpc.Status.newBuilder()
            .setCode(Status.Code.FAILED_PRECONDITION.value())
            .setMessage("unrelated subsystem failure")
            .addDetails(
                com.google.protobuf.Any.pack(
                    ai.floedb.floecat.common.rpc.Error.newBuilder()
                        .setCode(ErrorCode.MC_PRECONDITION_FAILED)
                        .setMessage("unrelated subsystem failure")
                        .putParams(
                            "reason",
                            SourceCatalogVendingGrpcStatus.SOURCE_CATALOG_VEND_REFUSED_REASON)
                        .putParams("domain", "ai.floedb.floecat.someone.else")
                        .build()))
            .build();

    var impostor = StatusProto.toStatusRuntimeException(foreign);

    assertFalse(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(impostor));
  }

  @Test
  void otherVendingReasonsStayDistinct() {
    var notRefreshable =
        SourceCatalogVendingGrpcStatus.vendedCredentialsNotRefreshable("missing session token");
    var noAuthority = SourceCatalogVendingGrpcStatus.noMatchingStorageAuthority("none matches");

    assertFalse(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(notRefreshable));
    assertFalse(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(noAuthority));
    assertFalse(SourceCatalogVendingGrpcStatus.isSourceCatalogVendUnavailable(notRefreshable));
    assertTrue(SourceCatalogVendingGrpcStatus.isVendedCredentialsNotRefreshable(notRefreshable));
    assertTrue(SourceCatalogVendingGrpcStatus.isNoMatchingStorageAuthority(noAuthority));
  }
}
