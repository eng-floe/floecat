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

package ai.floedb.floecat.client.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.reconciler.rpc.ClearReconcileQueueRequest;
import ai.floedb.floecat.reconciler.rpc.ClearReconcileQueueResponse;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ReconcilerCliSupportTest {

  @Test
  void queueClearCallsRpcAndPrintsCounts() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ReconcilerCliSupport.handle(
          List.of("queue", "clear", "--force"), new PrintStream(buf), h.reconcileControlStub);

      assertEquals(1, h.service.clearCalls.get());
      assertTrue(h.service.lastRequest.getForce());
      assertTrue(buf.toString().contains("jobs_cleared=3"));
      assertTrue(buf.toString().contains("pointers_deleted=12"));
      assertTrue(buf.toString().contains("job_blob_prefixes_deleted=2"));
    }
  }

  @Test
  void queueClearPrintsUsageWithoutForce() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      ReconcilerCliSupport.handle(
          List.of("queue", "clear"), new PrintStream(buf), h.reconcileControlStub);

      assertEquals(0, h.service.clearCalls.get());
      assertTrue(buf.toString().contains("usage: reconciler queue clear --force"));
    }
  }

  private static final class Harness implements AutoCloseable {
    final String serverName = InProcessServerBuilder.generateName();
    final TestReconcileControlService service = new TestReconcileControlService();
    final Server server =
        InProcessServerBuilder.forName(serverName).directExecutor().addService(service).build();
    final ManagedChannel channel =
        InProcessChannelBuilder.forName(serverName).directExecutor().build();
    final ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControlStub =
        ReconcileControlGrpc.newBlockingStub(channel);

    Harness() throws Exception {
      server.start();
    }

    @Override
    public void close() {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class TestReconcileControlService
      extends ReconcileControlGrpc.ReconcileControlImplBase {
    final AtomicInteger clearCalls = new AtomicInteger();
    volatile ClearReconcileQueueRequest lastRequest =
        ClearReconcileQueueRequest.getDefaultInstance();

    @Override
    public void clearReconcileQueue(
        ClearReconcileQueueRequest request,
        StreamObserver<ClearReconcileQueueResponse> responseObserver) {
      clearCalls.incrementAndGet();
      lastRequest = request;
      responseObserver.onNext(
          ClearReconcileQueueResponse.newBuilder()
              .setJobsCleared(3)
              .setPointersDeleted(12)
              .setJobBlobPrefixesDeleted(2)
              .build());
      responseObserver.onCompleted();
    }
  }
}
