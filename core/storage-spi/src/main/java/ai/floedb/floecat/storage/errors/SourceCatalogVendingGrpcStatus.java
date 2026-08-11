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

/**
 * Structured discriminators for the source-catalog vending path, so callers can act on a specific
 * condition instead of a status code.
 *
 * <p>Both conditions here previously travelled as bare status codes, and both were ambiguous.
 * {@code INVALID_ARGUMENT} is also what {@code vendStorageCredentials} returns for account_id,
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

  private static final String REASON_PARAM = "reason";

  private SourceCatalogVendingGrpcStatus() {}

  public static StatusRuntimeException noMatchingStorageAuthority(String description) {
    return withReason(
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        NO_MATCHING_STORAGE_AUTHORITY_REASON,
        description);
  }

  public static boolean isNoMatchingStorageAuthority(Throwable error) {
    return hasReason(error, Status.Code.INVALID_ARGUMENT, NO_MATCHING_STORAGE_AUTHORITY_REASON);
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
        error, Status.Code.FAILED_PRECONDITION, VENDED_CREDENTIALS_NOT_REFRESHABLE_REASON);
  }

  private static StatusRuntimeException withReason(
      Status.Code code, ErrorCode errorCode, String reason, String description) {
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
                        .putParams(REASON_PARAM, reason)
                        .build()))
            .addDetails(
                Any.pack(ErrorInfo.newBuilder().setDomain(ERROR_DOMAIN).setReason(reason).build()))
            .build();
    return StatusProto.toStatusRuntimeException(status);
  }

  private static boolean hasReason(Throwable error, Status.Code expectedCode, String reason) {
    Throwable current = error;
    var seen = new HashSet<Throwable>();
    while (current != null && seen.add(current)) {
      if (current instanceof StatusRuntimeException statusError
          && statusError.getStatus().getCode() == expectedCode
          && hasReasonDetail(statusError, reason)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
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
          Error floecatError = detail.unpack(Error.class);
          if (reason.equals(floecatError.getParamsMap().get(REASON_PARAM))) {
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
