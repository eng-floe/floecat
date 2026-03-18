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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.AppendTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.AppendTableConstraintsResponse;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.GetTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.GetTableConstraintsResponse;
import ai.floedb.floecat.catalog.rpc.MergeTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.MergeTableConstraintsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableConstraintsServiceGrpc;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import com.google.protobuf.MessageOrBuilder;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ConstraintsCliSupportTest {

  @Test
  void resolveSnapshotIdParsesSnapshotFlag() {
    long snapshotId =
        ConstraintsCliSupport.resolveSnapshotId(
            List.of("demo.sales.users", "--snapshot", "42"),
            tableId(),
            null,
            ConstraintsCliSupportTest::parseStringFlag);

    assertEquals(42L, snapshotId);
  }

  @Test
  void resolveSnapshotIdRejectsInvalidSnapshotFlag() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ConstraintsCliSupport.resolveSnapshotId(
                    List.of("demo.sales.users", "--snapshot", "abc"),
                    tableId(),
                    null,
                    ConstraintsCliSupportTest::parseStringFlag));

    assertEquals("snapshot_id must be a valid integer: abc", error.getMessage());
  }

  @Test
  void handleUpdateUsesMergeRequestAndDoesNotResolveCurrentSnapshotWhenExplicit() throws Exception {
    Path payload = writeConstraintsPayload(List.of("pk_users"));
    try (Harness harness = new Harness(99L)) {
      ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(outBytes);

      ConstraintsCliSupport.handle(
          List.of("update", "demo.sales.users", "--snapshot", "42", "--file", payload.toString()),
          out,
          harness.constraintsStub,
          harness.snapshotsStub,
          ignored -> tableId(),
          ConstraintsCliSupportTest::parseStringFlag,
          ConstraintsCliSupportTest::parseIntFlag,
          ConstraintsCliSupportTest::hasFlag,
          ConstraintsCliSupportTest::ignoreJson);

      assertEquals(1, harness.constraintsService.mergeRequests.size());
      assertEquals(0, harness.constraintsService.appendRequests.size());
      assertEquals(0, harness.snapshotService.getSnapshotCalls.get());
      assertEquals(42L, harness.constraintsService.mergeRequests.get(0).getSnapshotId());
      assertTrue(outBytes.toString().contains("snapshot_id=42"));
    } finally {
      Files.deleteIfExists(payload);
    }
  }

  @Test
  void handleAddUsesAppendRequestAndResolvesCurrentSnapshotWhenSnapshotOmitted() throws Exception {
    Path payload = writeConstraintsPayload(List.of("pk_users"));
    try (Harness harness = new Harness(1234L)) {
      ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(outBytes);

      ConstraintsCliSupport.handle(
          List.of("add", "demo.sales.users", "--file", payload.toString()),
          out,
          harness.constraintsStub,
          harness.snapshotsStub,
          ignored -> tableId(),
          ConstraintsCliSupportTest::parseStringFlag,
          ConstraintsCliSupportTest::parseIntFlag,
          ConstraintsCliSupportTest::hasFlag,
          ConstraintsCliSupportTest::ignoreJson);

      assertEquals(0, harness.constraintsService.mergeRequests.size());
      assertEquals(1, harness.constraintsService.appendRequests.size());
      assertEquals(0, harness.constraintsService.getConstraintsCalls.get());
      assertEquals(1, harness.snapshotService.getSnapshotCalls.get());
      assertEquals(1234L, harness.constraintsService.appendRequests.get(0).getSnapshotId());
      assertTrue(outBytes.toString().contains("snapshot_id=1234"));
    } finally {
      Files.deleteIfExists(payload);
    }
  }

  @Test
  void handleUpdateForwardsDuplicateConstraintNamesInPayload() throws Exception {
    Path payload = writeConstraintsPayload(List.of("dup_name", "dup_name"));
    try (Harness harness = new Harness(55L)) {
      ConstraintsCliSupport.handle(
          List.of("update", "demo.sales.users", "--snapshot", "55", "--file", payload.toString()),
          new PrintStream(new ByteArrayOutputStream()),
          harness.constraintsStub,
          harness.snapshotsStub,
          ignored -> tableId(),
          ConstraintsCliSupportTest::parseStringFlag,
          ConstraintsCliSupportTest::parseIntFlag,
          ConstraintsCliSupportTest::hasFlag,
          ConstraintsCliSupportTest::ignoreJson);

      MergeTableConstraintsRequest request = harness.constraintsService.mergeRequests.get(0);
      assertEquals(2, request.getConstraints().getConstraintsCount());
      assertEquals("dup_name", request.getConstraints().getConstraints(0).getName());
      assertEquals("dup_name", request.getConstraints().getConstraints(1).getName());
    } finally {
      Files.deleteIfExists(payload);
    }
  }

  @Test
  void handleRejectsLegacyPositionalSnapshotForAddPk() throws Exception {
    try (Harness harness = new Harness(77L)) {
      ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(outBytes);

      ConstraintsCliSupport.handle(
          List.of("add-pk", "demo.sales.users", "42", "pk_users", "id"),
          out,
          harness.constraintsStub,
          harness.snapshotsStub,
          ignored -> tableId(),
          ConstraintsCliSupportTest::parseStringFlag,
          ConstraintsCliSupportTest::parseIntFlag,
          ConstraintsCliSupportTest::hasFlag,
          ConstraintsCliSupportTest::ignoreJson);

      assertEquals(0, harness.snapshotService.getSnapshotCalls.get());
      assertEquals(0, harness.constraintsService.mergeRequests.size());
      assertEquals(0, harness.constraintsService.appendRequests.size());
      assertTrue(outBytes.toString().contains("usage: constraints add-pk"));
    }
  }

  @Test
  void handleRejectsInvalidSnapshotFlagAtCommandLevel() throws Exception {
    Path payload = writeConstraintsPayload(List.of("pk_users"));
    try (Harness harness = new Harness(88L)) {
      IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  ConstraintsCliSupport.handle(
                      List.of(
                          "update",
                          "demo.sales.users",
                          "--snapshot",
                          "abc",
                          "--file",
                          payload.toString()),
                      new PrintStream(new ByteArrayOutputStream()),
                      harness.constraintsStub,
                      harness.snapshotsStub,
                      ignored -> tableId(),
                      ConstraintsCliSupportTest::parseStringFlag,
                      ConstraintsCliSupportTest::parseIntFlag,
                      ConstraintsCliSupportTest::hasFlag,
                      ConstraintsCliSupportTest::ignoreJson));
      assertEquals("snapshot_id must be a valid integer: abc", error.getMessage());
    } finally {
      Files.deleteIfExists(payload);
    }
  }

  private static ResourceId tableId() {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_TABLE)
        .setId("users")
        .build();
  }

  private static Path writeConstraintsPayload(List<String> names) throws Exception {
    StringBuilder json = new StringBuilder();
    json.append("{\"constraints\":[");
    for (int i = 0; i < names.size(); i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append(
          "{\"name\":\""
              + names.get(i)
              + "\",\"type\":\"CT_PRIMARY_KEY\",\"columns\":[{\"columnName\":\"id\",\"ordinal\":1}]}");
    }
    json.append("]}");
    Path file = Files.createTempFile("constraints-cli-", ".json");
    Files.writeString(file, json.toString());
    return file;
  }

  private static String parseStringFlag(List<String> args, String flag, String defaultValue) {
    for (int i = 0; i < args.size(); i++) {
      if (!flag.equals(args.get(i))) {
        continue;
      }
      if (i + 1 >= args.size()) {
        return defaultValue;
      }
      return args.get(i + 1);
    }
    return defaultValue;
  }

  private static int parseIntFlag(List<String> args, String flag, int defaultValue) {
    String value = parseStringFlag(args, flag, "");
    if (value.isBlank()) {
      return defaultValue;
    }
    return Integer.parseInt(value);
  }

  private static boolean hasFlag(List<String> args, String flag) {
    for (String arg : args) {
      if (flag.equals(arg)) {
        return true;
      }
    }
    return false;
  }

  private static void ignoreJson(MessageOrBuilder ignored) {}

  private static final class Harness implements AutoCloseable {
    private final Server server;
    private final ManagedChannel channel;
    private final CapturingConstraintsService constraintsService;
    private final CapturingSnapshotService snapshotService;
    private final TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsStub;
    private final SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsStub;

    private Harness(long currentSnapshotId) throws Exception {
      String serverName = InProcessServerBuilder.generateName();
      this.constraintsService = new CapturingConstraintsService();
      this.snapshotService = new CapturingSnapshotService(currentSnapshotId);
      this.server =
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(constraintsService)
              .addService(snapshotService)
              .build()
              .start();
      this.channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
      this.constraintsStub = TableConstraintsServiceGrpc.newBlockingStub(channel);
      this.snapshotsStub = SnapshotServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() throws Exception {
      channel.shutdownNow();
      server.shutdownNow();
    }
  }

  private static final class CapturingConstraintsService
      extends TableConstraintsServiceGrpc.TableConstraintsServiceImplBase {
    private final List<MergeTableConstraintsRequest> mergeRequests = new ArrayList<>();
    private final List<AppendTableConstraintsRequest> appendRequests = new ArrayList<>();
    private final AtomicInteger getConstraintsCalls = new AtomicInteger();

    @Override
    public void getTableConstraints(
        GetTableConstraintsRequest request, StreamObserver<GetTableConstraintsResponse> response) {
      getConstraintsCalls.incrementAndGet();
      response.onNext(GetTableConstraintsResponse.getDefaultInstance());
      response.onCompleted();
    }

    @Override
    public void mergeTableConstraints(
        MergeTableConstraintsRequest request,
        StreamObserver<MergeTableConstraintsResponse> response) {
      mergeRequests.add(request);
      response.onNext(
          MergeTableConstraintsResponse.newBuilder()
              .setConstraints(
                  normalizeConstraints(
                      request.getTableId(), request.getSnapshotId(), request.getConstraints()))
              .build());
      response.onCompleted();
    }

    @Override
    public void appendTableConstraints(
        AppendTableConstraintsRequest request,
        StreamObserver<AppendTableConstraintsResponse> response) {
      appendRequests.add(request);
      response.onNext(
          AppendTableConstraintsResponse.newBuilder()
              .setConstraints(
                  normalizeConstraints(
                      request.getTableId(), request.getSnapshotId(), request.getConstraints()))
              .build());
      response.onCompleted();
    }

    private static SnapshotConstraints normalizeConstraints(
        ResourceId tableId, long snapshotId, SnapshotConstraints incoming) {
      return SnapshotConstraints.newBuilder(incoming)
          .setTableId(tableId)
          .setSnapshotId(snapshotId)
          .build();
    }
  }

  private static final class CapturingSnapshotService
      extends SnapshotServiceGrpc.SnapshotServiceImplBase {
    private final long currentSnapshotId;
    private final AtomicInteger getSnapshotCalls = new AtomicInteger();

    private CapturingSnapshotService(long currentSnapshotId) {
      this.currentSnapshotId = currentSnapshotId;
    }

    @Override
    public void getSnapshot(
        GetSnapshotRequest request, StreamObserver<GetSnapshotResponse> response) {
      getSnapshotCalls.incrementAndGet();
      response.onNext(
          GetSnapshotResponse.newBuilder()
              .setSnapshot(
                  Snapshot.newBuilder()
                      .setTableId(request.getTableId())
                      .setSnapshotId(currentSnapshotId)
                      .build())
              .build());
      response.onCompleted();
    }
  }
}
