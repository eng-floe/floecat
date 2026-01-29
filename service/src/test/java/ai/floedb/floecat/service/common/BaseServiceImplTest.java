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

package ai.floedb.floecat.service.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Test;

class BaseServiceImplTest {
  private static final String CORRELATION_ID = "corr-test";

  @Test
  void toStatusPreservesOriginalCanonicalCode() {
    TestServiceImpl service = new TestServiceImpl();
    StatusRuntimeException original = io.grpc.Status.NOT_FOUND.asRuntimeException();
    StatusRuntimeException repacked = service.repack(original, CORRELATION_ID);

    Status statusProto = StatusProto.fromThrowable(repacked);
    assertEquals(io.grpc.Status.Code.NOT_FOUND.value(), statusProto.getCode());
    Error err =
        statusProto.getDetailsList().stream()
            .filter(any -> any.is(Error.class))
            .findFirst()
            .map(
                any -> {
                  try {
                    return any.unpack(Error.class);
                  } catch (InvalidProtocolBufferException e) {
                    throw new AssertionError("failed to unpack Error detail", e);
                  }
                })
            .orElseThrow();
    assertEquals(CORRELATION_ID, err.getCorrelationId());
    assertEquals(ErrorCode.MC_NOT_FOUND, err.getCode());
  }

  private static final class TestServiceImpl extends BaseServiceImpl {
    StatusRuntimeException repack(StatusRuntimeException ex, String corrId) {
      return toStatus(ex, corrId);
    }
  }
}
