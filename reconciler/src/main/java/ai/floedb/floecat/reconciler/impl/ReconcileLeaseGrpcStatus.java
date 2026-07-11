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

package ai.floedb.floecat.reconciler.impl;

import com.google.protobuf.Any;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.HashSet;

public final class ReconcileLeaseGrpcStatus {
  public static final String ERROR_DOMAIN = "ai.floedb.floecat.reconciler";
  public static final String LEASE_PRECONDITION_FAILED_REASON =
      "RECONCILE_LEASE_PRECONDITION_FAILED";

  private ReconcileLeaseGrpcStatus() {}

  public static StatusRuntimeException leasePreconditionFailed(String description) {
    com.google.rpc.Status status =
        com.google.rpc.Status.newBuilder()
            .setCode(Status.Code.FAILED_PRECONDITION.value())
            .setMessage(description == null ? "" : description)
            .addDetails(
                Any.pack(
                    ErrorInfo.newBuilder()
                        .setDomain(ERROR_DOMAIN)
                        .setReason(LEASE_PRECONDITION_FAILED_REASON)
                        .build()))
            .build();
    return StatusProto.toStatusRuntimeException(status);
  }

  public static boolean isLeasePreconditionFailure(Throwable error) {
    Throwable current = error;
    var seen = new HashSet<Throwable>();
    while (current != null && seen.add(current)) {
      if (current instanceof StatusRuntimeException statusError
          && statusError.getStatus().getCode() == Status.Code.FAILED_PRECONDITION
          && hasLeasePreconditionFailureDetail(statusError)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static boolean hasLeasePreconditionFailureDetail(StatusRuntimeException error) {
    com.google.rpc.Status status = StatusProto.fromThrowable(error);
    if (status == null) {
      return false;
    }
    for (Any detail : status.getDetailsList()) {
      if (!detail.is(ErrorInfo.class)) {
        continue;
      }
      try {
        ErrorInfo errorInfo = detail.unpack(ErrorInfo.class);
        if (LEASE_PRECONDITION_FAILED_REASON.equals(errorInfo.getReason())
            && ERROR_DOMAIN.equals(errorInfo.getDomain())) {
          return true;
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException ignored) {
        return false;
      }
    }
    return false;
  }
}
