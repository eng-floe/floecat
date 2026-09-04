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

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.HashSet;
import java.util.Map;

/**
 * Structured discriminators for the source-catalog vending path, so callers can act on a specific
 * condition instead of a status code.
 *
 * <p>These conditions otherwise travel as bare status codes, which are ambiguous. {@code
 * INVALID_ARGUMENT} is also what {@code vendStorageCredentials} returns for account_id,
 * execution_binding and location_prefix validation failures, so a delegating connector matching on
 * the code alone turned real configuration errors into a silent fallback. {@code
 * FAILED_PRECONDITION} is shared with lease-precondition failures, which have their own retry
 * semantics, so it cannot simply be classified terminal.
 *
 * <p>Same shape as {@code ReconcileLeaseGrpcStatus} in the reconciler: an ErrorInfo detail carrying
 * a stable domain and reason, plus a matching predicate. Lives here, beside the other storage
 * errors, because the domain it declares is {@code ai.floedb.floecat.storage} and the conditions
 * are raised by the storage service -- the reconciler is a consumer, not the owner.
 */
public final class SourceCatalogVendingGrpcStatus {
  public static final String ERROR_DOMAIN = "ai.floedb.floecat.storage";

  /** No configured storage authority covers the requested location. */
  public static final String NO_MATCHING_STORAGE_AUTHORITY_REASON = "NO_MATCHING_STORAGE_AUTHORITY";

  /**
   * The source catalog vended credentials that cannot be renewed -- an incomplete session tuple or
   * no usable expiry. Terminal: a catalog that omits these will keep omitting them.
   */
  public static final String VENDED_CREDENTIALS_NOT_REFRESHABLE_REASON =
      "VENDED_CREDENTIALS_NOT_REFRESHABLE";

  /**
   * The source catalog will not vend, for a reason a retry cannot change: it rejected floecat's
   * credentials, the upstream table is gone, it vended credentials that do not cover the table, or
   * -- on the Catalog Integration path -- it cannot vend at all. Terminal.
   *
   * <p>That last group is this reason only for an Integration, which has no storage authority to
   * fall back to and refuses instead. The same condition on a legacy Connector is answered by
   * returning no credentials, so the caller uses the authority configured for it and nothing is
   * raised. A condition the next attempt could clear is not this reason either; those stay
   * retryable.
   *
   * <p>Carried under whichever status code fits the refusal, so a consumer reads the reason to know
   * it came from a source catalog and the code to know what kind of refusal it was.
   */
  public static final String SOURCE_CATALOG_VEND_REFUSED_REASON = "SOURCE_CATALOG_VEND_REFUSED";

  /**
   * The source catalog could not vend right now, and a later attempt could succeed: it vended a
   * credential that had already expired, or its stored credentials were momentarily unresolvable.
   * Retryable, and distinct from the storage service itself being unreachable, which is what a bare
   * {@code UNAVAILABLE} would be indistinguishable from.
   */
  public static final String SOURCE_CATALOG_VEND_UNAVAILABLE_REASON =
      "SOURCE_CATALOG_VEND_UNAVAILABLE";

  private static final String REASON_PARAM = "reason";

  /**
   * Names this class as the author of a reason on the floecat {@code Error} detail, which -- unlike
   * {@code ErrorInfo} -- has no domain field of its own. Without it the reason string alone
   * decides, and a {@code params["reason"]} written by any other subsystem would read as one of
   * these conditions. That matters most for the refusal, which is classified terminal.
   */
  private static final String DOMAIN_PARAM = "domain";

  /**
   * Where the diagnostic travels, because {@code Status.message} does not survive the trip.
   *
   * <p>{@code LocalizeErrorsInterceptor} rewrites every outgoing status with {@code
   * MessageCatalog.render(error)}, which builds the text from the error code and message key and
   * never reads {@code Error.message}. Without a keyed template these conditions reached a caller
   * as "Precondition failed." -- the reason param still classified them, but nothing named the
   * cause, which is the whole point of refusing instead of relabelling. Each factory below names a
   * template whose body is this parameter, so the rendered message is the diagnostic.
   */
  private static final String DETAIL_PARAM = "detail";

  /**
   * Message-key suffixes, one per code-and-reason pair.
   *
   * <p>Distinct even where the reason repeats: the generator normalizes a suffix into an enum
   * constant and fails the build when two collide, so {@code SOURCE_CATALOG_VEND_REFUSED} cannot
   * reuse one suffix across the three codes it is raised under.
   */
  private static final String NO_AUTHORITY_KEY = "source.catalog.no.matching.storage.authority";

  private static final String NOT_REFRESHABLE_KEY =
      "source.catalog.vended.credentials.not.refreshable";

  private static final String VEND_REFUSED_KEY = "source.catalog.vend.refused";

  private static final String VEND_REFUSED_UNAUTHENTICATED_KEY =
      "source.catalog.vend.refused.unauthenticated";

  private static final String VEND_REFUSED_FORBIDDEN_KEY = "source.catalog.vend.refused.forbidden";

  private static final String VEND_UNAVAILABLE_KEY = "source.catalog.vend.unavailable";

  private SourceCatalogVendingGrpcStatus() {}

  public static StatusRuntimeException noMatchingStorageAuthority(String description) {
    return withReason(
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        NO_MATCHING_STORAGE_AUTHORITY_REASON,
        description);
  }

  public static boolean isNoMatchingStorageAuthority(Throwable error) {
    return hasReason(error, NO_MATCHING_STORAGE_AUTHORITY_REASON, Status.Code.INVALID_ARGUMENT);
  }

  public static StatusRuntimeException vendedCredentialsNotRefreshable(String description) {
    return withReason(
        Status.Code.FAILED_PRECONDITION,
        ErrorCode.MC_PRECONDITION_FAILED,
        VENDED_CREDENTIALS_NOT_REFRESHABLE_REASON,
        description);
  }

  public static boolean isVendedCredentialsNotRefreshable(Throwable error) {
    return hasReason(
        error, VENDED_CREDENTIALS_NOT_REFRESHABLE_REASON, Status.Code.FAILED_PRECONDITION);
  }

  public static StatusRuntimeException sourceCatalogVendRefused(String description) {
    return withReason(
        Status.Code.FAILED_PRECONDITION,
        ErrorCode.MC_PRECONDITION_FAILED,
        SOURCE_CATALOG_VEND_REFUSED_REASON,
        description);
  }

  /**
   * The same refusal, carrying the status code the upstream catalog's answer deserves.
   *
   * <p>An authentication or authorization refusal from a source catalog is still a source-catalog
   * vending refusal, and it has to say so: the alternative is a bare {@code UNAUTHENTICATED} that
   * no consumer can tell apart from floecat's own service-level auth failing. The reason is what
   * identifies the condition; the code only conveys what kind of failure it was.
   */
  public static StatusRuntimeException sourceCatalogVendRefused(
      Status.Code code, ErrorCode errorCode, String description, Throwable cause) {
    StatusRuntimeException refusal =
        withReason(code, errorCode, SOURCE_CATALOG_VEND_REFUSED_REASON, description);
    if (cause == null) {
      return refusal;
    }
    // Keep the originating exception attached: the detail carries only a message safe to return to
    // a caller, and server-side logs still need the stack that produced it.
    return new StatusRuntimeException(refusal.getStatus().withCause(cause), refusal.getTrailers());
  }

  /**
   * The retryable counterpart of {@link #sourceCatalogVendRefused(String)}.
   *
   * <p>{@code UNAVAILABLE} because the caller should come back, and structured because the code
   * alone does not distinguish "the source catalog's vending is temporarily unusable" from
   * "floecat's storage service is unreachable" -- and because {@code BaseServiceImpl.toStatus}
   * passes a status through untouched only when it carries a floecat error detail, so a bare status
   * is rebuilt with a synthesized message and loses this description.
   */
  public static StatusRuntimeException sourceCatalogVendUnavailable(
      String description, Throwable cause) {
    StatusRuntimeException unavailable =
        withReason(
            Status.Code.UNAVAILABLE,
            ErrorCode.MC_UNAVAILABLE,
            SOURCE_CATALOG_VEND_UNAVAILABLE_REASON,
            description);
    if (cause == null) {
      return unavailable;
    }
    return new StatusRuntimeException(
        unavailable.getStatus().withCause(cause), unavailable.getTrailers());
  }

  public static boolean isSourceCatalogVendUnavailable(Throwable error) {
    return hasReason(error, SOURCE_CATALOG_VEND_UNAVAILABLE_REASON, Status.Code.UNAVAILABLE);
  }

  /**
   * Whether {@code error} is a source-catalog vending refusal.
   *
   * <p>Matched on the domain-scoped reason alone, across every status code a refusal can carry. The
   * reason is unambiguous on its own -- it names this domain and this condition -- whereas the code
   * is not, which is the whole point of raising a reason rather than a bare status.
   */
  public static boolean isSourceCatalogVendRefused(Throwable error) {
    return hasReason(error, SOURCE_CATALOG_VEND_REFUSED_REASON, null);
  }

  private static StatusRuntimeException withReason(
      Status.Code code, ErrorCode errorCode, String reason, String description) {
    return withReason(code, errorCode, messageKey(errorCode, reason), reason, description);
  }

  /** The suffix for a code-and-reason pair; see the key constants for why it is not just reason. */
  private static String messageKey(ErrorCode errorCode, String reason) {
    if (NO_MATCHING_STORAGE_AUTHORITY_REASON.equals(reason)) {
      return NO_AUTHORITY_KEY;
    }
    if (VENDED_CREDENTIALS_NOT_REFRESHABLE_REASON.equals(reason)) {
      return NOT_REFRESHABLE_KEY;
    }
    if (SOURCE_CATALOG_VEND_UNAVAILABLE_REASON.equals(reason)) {
      return VEND_UNAVAILABLE_KEY;
    }
    return switch (errorCode) {
      case MC_UNAUTHENTICATED -> VEND_REFUSED_UNAUTHENTICATED_KEY;
      case MC_PERMISSION_DENIED -> VEND_REFUSED_FORBIDDEN_KEY;
      default -> VEND_REFUSED_KEY;
    };
  }

  private static StatusRuntimeException withReason(
      Status.Code code, ErrorCode errorCode, String messageKey, String reason, String description) {
    String message = description == null ? "" : description;
    com.google.rpc.Status status =
        com.google.rpc.Status.newBuilder()
            .setCode(code.value())
            .setMessage(message)
            .addDetails(
                Any.pack(
                    Error.newBuilder()
                        .setCode(errorCode)
                        .setMessage(message)
                        .setMessageKey(messageKey)
                        .putParams(REASON_PARAM, reason)
                        .putParams(DOMAIN_PARAM, ERROR_DOMAIN)
                        .putParams(DETAIL_PARAM, message)
                        .build()))
            .addDetails(
                Any.pack(ErrorInfo.newBuilder().setDomain(ERROR_DOMAIN).setReason(reason).build()))
            .build();
    return StatusProto.toStatusRuntimeException(status);
  }

  /** {@code expectedCode} narrows the match; {@code null} accepts the reason under any code. */
  private static boolean hasReason(Throwable error, String reason, Status.Code expectedCode) {
    Throwable current = error;
    var seen = new HashSet<Throwable>();
    while (current != null && seen.add(current)) {
      if (current instanceof StatusRuntimeException statusError
          && (expectedCode == null || statusError.getStatus().getCode() == expectedCode)
          && hasReasonDetail(statusError, reason)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  /**
   * The ErrorCode a reason was raised with before the domain param existed, or empty when no node
   * could have emitted it that way.
   *
   * <p>Three can arrive domain-less, and which three is a function of the stack's merge order
   * rather than of {@code main} alone: {@code NO_MATCHING_STORAGE_AUTHORITY} and {@code
   * VENDED_CREDENTIALS_NOT_REFRESHABLE} are in {@code main}, and {@code
   * SOURCE_CATALOG_VEND_REFUSED} is in the branch this one is stacked on, which ships first. {@code
   * SOURCE_CATALOG_VEND_UNAVAILABLE} arrives with this change, so a domain-less detail claiming it
   * is not an older node -- it is something else, and matching it would reopen exactly the gap the
   * domain param closes.
   */
  private static java.util.Optional<ErrorCode> legacyErrorCode(String reason) {
    if (NO_MATCHING_STORAGE_AUTHORITY_REASON.equals(reason)) {
      return java.util.Optional.of(ErrorCode.MC_INVALID_ARGUMENT);
    }
    if (VENDED_CREDENTIALS_NOT_REFRESHABLE_REASON.equals(reason)
        || SOURCE_CATALOG_VEND_REFUSED_REASON.equals(reason)) {
      return java.util.Optional.of(ErrorCode.MC_PRECONDITION_FAILED);
    }
    // Everything else is newer than the domain param, so no node can have emitted it without one
    // and there is no legacy shape to accept. Empty rather than a default: a reason added later --
    // under MC_NOT_FOUND, say -- would otherwise be told its historical code was
    // MC_PRECONDITION_FAILED and quietly match the wrong details, or fail to match its own.
    return java.util.Optional.empty();
  }

  private static boolean hasReasonDetail(StatusRuntimeException error, String reason) {
    com.google.rpc.Status status = StatusProto.fromThrowable(error);
    if (status == null) {
      return false;
    }
    for (Any detail : status.getDetailsList()) {
      try {
        if (detail.is(ErrorInfo.class)) {
          ErrorInfo errorInfo = detail.unpack(ErrorInfo.class);
          if (reason.equals(errorInfo.getReason()) && ERROR_DOMAIN.equals(errorInfo.getDomain())) {
            return true;
          }
        } else if (detail.is(Error.class)) {
          // Not a backstop, whatever it looks like. LocalizeErrorsInterceptor is a
          // @GlobalInterceptor
          // that rebuilds every outgoing status with clearDetails().addDetails(the localized
          // Error),
          // so the ErrorInfo above never survives an RPC -- this branch is the only one a remote
          // consumer, the reconciler included, ever matches on.
          Error floecatError = detail.unpack(Error.class);
          Map<String, String> params = floecatError.getParamsMap();
          if (!reason.equals(params.get(REASON_PARAM))) {
            continue;
          }
          if (ERROR_DOMAIN.equals(params.get(DOMAIN_PARAM))) {
            return true;
          }
          // Emitted before the domain param existed. A node still running the older build sends the
          // reason with no domain, and refusing it here would make a new reconciler read an old
          // service's answers as "not this condition" for the length of a rolling deploy: the
          // delegation fall-back would stop being absorbed for existing connectors, and terminal
          // vending failures would retry until the attempt budget was spent. Narrowed to the
          // ErrorCode that reason was historically raised with, which is as much as a legacy detail
          // carries. Removable once no node emits the older shape.
          if (!params.containsKey(DOMAIN_PARAM)
              && legacyErrorCode(reason).filter(floecatError.getCode()::equals).isPresent()) {
            return true;
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException ignored) {
        // A detail we cannot decode simply is not this condition.
      }
    }
    return false;
  }
}
