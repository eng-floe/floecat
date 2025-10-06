package ai.floedb.metacat.service.error.impl;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import ai.floedb.metacat.common.rpc.Error;

public final class GrpcErrors {
  public static StatusRuntimeException notFound(String msg, String correlationId) {
    Error err = Error.newBuilder()
        .setCode("NOT_FOUND").setMessage(msg)
        .putDetails("hint", "check id/tenant")
        .setCorrelationId(correlationId == null ? "" : correlationId)
        .build();

    com.google.rpc.Status st = com.google.rpc.Status.newBuilder()
        .setCode(Code.NOT_FOUND.getNumber())
        .setMessage(msg)
        .addDetails(Any.pack(err))
        .build();

    return StatusProto.toStatusRuntimeException(st);
  }
}