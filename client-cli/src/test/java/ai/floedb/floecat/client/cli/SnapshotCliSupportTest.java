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

import ai.floedb.floecat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.DeleteSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.common.rpc.ResourceId;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SnapshotCliSupportTest {

  private static ResourceId tableId() {
    return ResourceId.newBuilder().setId("table-uuid-1").build();
  }

  // --- snapshots (list) ---

  @Test
  void snapshotsListPrintsHeader() throws Exception {
    try (Harness h = new Harness()) {
      h.snapshotService.snapshotsToReturn.add(Snapshot.newBuilder().setSnapshotId(42L).build());

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      SnapshotCliSupport.handle(
          "snapshots",
          List.of("catalog.ns.tbl"),
          new PrintStream(buf),
          h.snapshotsStub,
          ignored -> tableId());

      String out = buf.toString();
      assertTrue(out.contains("SNAPSHOT_ID"), "expected header row");
      assertTrue(out.contains("42"), "expected snapshot id");
    }
  }

  @Test
  void snapshotsListPrintsUsageWhenNoArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      SnapshotCliSupport.handle(
          "snapshots", List.of(), new PrintStream(buf), h.snapshotsStub, ignored -> tableId());
      assertTrue(buf.toString().contains("usage:"), "expected usage message");
    }
  }

  // --- snapshot get ---

  @Test
  void snapshotGetCallsServiceWithCorrectArgs() throws Exception {
    try (Harness h = new Harness()) {
      h.snapshotService.snapshotToReturn =
          Snapshot.newBuilder().setSnapshotId(99L).setTableId(tableId()).build();

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      SnapshotCliSupport.handle(
          "snapshot",
          List.of("get", "catalog.ns.tbl", "99"),
          new PrintStream(buf),
          h.snapshotsStub,
          ignored -> tableId());

      assertEquals(1, h.snapshotService.getSnapshotCalls.get());
      assertEquals(99L, h.snapshotService.lastGetRequest.getSnapshot().getSnapshotId());
    }
  }

  @Test
  void snapshotGetPrintsUsageWhenMissingArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      SnapshotCliSupport.handle(
          "snapshot",
          List.of("get", "catalog.ns.tbl"),
          new PrintStream(buf),
          h.snapshotsStub,
          ignored -> tableId());
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- snapshot delete ---

  @Test
  void snapshotDeleteCallsServiceAndPrintsOk() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      SnapshotCliSupport.handle(
          "snapshot",
          List.of("delete", "catalog.ns.tbl", "7"),
          new PrintStream(buf),
          h.snapshotsStub,
          ignored -> tableId());

      assertEquals(1, h.snapshotService.deleteSnapshotCalls.get());
      assertEquals(7L, h.snapshotService.lastDeleteRequest.getSnapshotId());
      assertTrue(buf.toString().contains("ok"));
    }
  }

  @Test
  void snapshotDeleteSetsEtagPrecondition() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      SnapshotCliSupport.handle(
          "snapshot",
          List.of("delete", "catalog.ns.tbl", "7", "--etag", "abc123"),
          new PrintStream(buf),
          h.snapshotsStub,
          ignored -> tableId());

      assertEquals(
          "abc123", h.snapshotService.lastDeleteRequest.getPrecondition().getExpectedEtag());
    }
  }

  @Test
  void snapshotDeleteHandlesPreconditionFailedGracefully() throws Exception {
    try (Harness h = new Harness()) {
      h.snapshotService.failDeleteWithPrecondition = true;

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      SnapshotCliSupport.handle(
          "snapshot",
          List.of("delete", "catalog.ns.tbl", "7"),
          new PrintStream(buf),
          h.snapshotsStub,
          ignored -> tableId());

      assertTrue(buf.toString().contains("Precondition failed"));
    }
  }

  @Test
  void snapshotDeletePrintsUsageWhenMissingArgs() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      SnapshotCliSupport.handle(
          "snapshot",
          List.of("delete", "catalog.ns.tbl"),
          new PrintStream(buf),
          h.snapshotsStub,
          ignored -> tableId());
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- snapshot unknown subcommand ---

  @Test
  void snapshotUnknownSubcommandPrintsError() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      SnapshotCliSupport.handle(
          "snapshot",
          List.of("frobnicate"),
          new PrintStream(buf),
          h.snapshotsStub,
          ignored -> tableId());
      assertTrue(buf.toString().contains("unknown subcommand"));
    }
  }

  @Test
  void snapshotEmptyArgsPrintsUsage() throws Exception {
    try (Harness h = new Harness()) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      SnapshotCliSupport.handle(
          "snapshot", List.of(), new PrintStream(buf), h.snapshotsStub, ignored -> tableId());
      assertTrue(buf.toString().contains("usage:"));
    }
  }

  // --- test infrastructure ---

  private static final class Harness implements AutoCloseable {
    final Server server;
    final ManagedChannel channel;
    final CapturingSnapshotService snapshotService;
    final SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsStub;

    Harness() throws Exception {
      String serverName = InProcessServerBuilder.generateName();
      this.snapshotService = new CapturingSnapshotService();
      this.server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(snapshotService)
              .build()
              .start();
      this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
      this.snapshotsStub = SnapshotServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class CapturingSnapshotService
      extends SnapshotServiceGrpc.SnapshotServiceImplBase {

    final AtomicInteger getSnapshotCalls = new AtomicInteger();
    final AtomicInteger deleteSnapshotCalls = new AtomicInteger();
    final List<Snapshot> snapshotsToReturn = new ArrayList<>();
    Snapshot snapshotToReturn = Snapshot.getDefaultInstance();
    GetSnapshotRequest lastGetRequest;
    DeleteSnapshotRequest lastDeleteRequest;
    boolean failDeleteWithPrecondition = false;

    @Override
    public void listSnapshots(
        ListSnapshotsRequest request, StreamObserver<ListSnapshotsResponse> responseObserver) {
      responseObserver.onNext(
          ListSnapshotsResponse.newBuilder().addAllSnapshots(snapshotsToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void getSnapshot(
        GetSnapshotRequest request, StreamObserver<GetSnapshotResponse> responseObserver) {
      getSnapshotCalls.incrementAndGet();
      lastGetRequest = request;
      responseObserver.onNext(
          GetSnapshotResponse.newBuilder().setSnapshot(snapshotToReturn).build());
      responseObserver.onCompleted();
    }

    @Override
    public void deleteSnapshot(
        DeleteSnapshotRequest request, StreamObserver<DeleteSnapshotResponse> responseObserver) {
      deleteSnapshotCalls.incrementAndGet();
      lastDeleteRequest = request;
      if (failDeleteWithPrecondition) {
        responseObserver.onError(new StatusRuntimeException(Status.FAILED_PRECONDITION));
        return;
      }
      responseObserver.onNext(DeleteSnapshotResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
