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

import ai.floedb.floecat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.PrintStream;
import java.time.Instant;
import java.util.List;
import java.util.function.Function;

/** CLI support for {@code snapshots} and {@code snapshot} commands. */
final class SnapshotCliSupport {

  private static final int DEFAULT_PAGE_SIZE = 1000;

  private SnapshotCliSupport() {}

  /**
   * Dispatches {@code snapshots} and {@code snapshot} top-level commands.
   *
   * @param command either {@code "snapshots"} (list) or {@code "snapshot"} (get/delete)
   * @param args tokens after the top-level command word
   * @param out output stream
   * @param snapshotsService gRPC snapshot service stub
   * @param resolveTableId resolves a table FQ name or UUID to a {@link ResourceId}
   */
  static void handle(
      String command,
      List<String> args,
      PrintStream out,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId) {
    switch (command) {
      case "snapshots" -> listSnapshots(args, out, snapshotsService, resolveTableId);
      case "snapshot" -> snapshotCrud(args, out, snapshotsService, resolveTableId);
      default -> out.println("Unknown snapshot command: " + command);
    }
  }

  private static void listSnapshots(
      List<String> args,
      PrintStream out,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId) {
    if (args.isEmpty()) {
      out.println("usage: snapshots <tableFQ>");
      return;
    }
    ResourceId tableId = resolveTableId.apply(args.get(0));
    var snaps =
        CliArgs.collectPages(
            DEFAULT_PAGE_SIZE,
            pr ->
                snapshotsService.listSnapshots(
                    ListSnapshotsRequest.newBuilder().setTableId(tableId).setPage(pr).build()),
            r -> r.getSnapshotsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    printSnapshots(snaps, out);
  }

  private static void snapshotCrud(
      List<String> args,
      PrintStream out,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId) {
    if (args.isEmpty()) {
      out.println("usage: snapshot <get|delete> ...");
      return;
    }
    String sub = args.get(0);
    List<String> tail = CliArgs.tail(args);
    switch (sub) {
      case "get" -> snapshotGet(tail, out, snapshotsService, resolveTableId);
      case "delete" -> snapshotDelete(tail, out, snapshotsService, resolveTableId);
      default -> out.println("unknown subcommand");
    }
  }

  private static void snapshotGet(
      List<String> args,
      PrintStream out,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId) {
    if (args.size() < 2) {
      out.println("usage: snapshot get <id|catalog.ns[.ns...].table> <snapshot_id>");
      return;
    }
    ResourceId tableId = resolveTableId.apply(args.get(0));
    long snapshotId = Long.parseLong(args.get(1));
    var resp =
        snapshotsService.getSnapshot(
            GetSnapshotRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                .build());
    printSnapshotDetail(resp.getSnapshot(), out);
  }

  private static void snapshotDelete(
      List<String> args,
      PrintStream out,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId) {
    if (args.size() < 2) {
      out.println("usage: snapshot delete <id|catalog.ns[.ns...].table> <snapshot_id>");
      return;
    }
    ResourceId tableId = resolveTableId.apply(args.get(0));
    long snapshotId = Long.parseLong(args.get(1));
    var builder = DeleteSnapshotRequest.newBuilder().setTableId(tableId).setSnapshotId(snapshotId);
    Precondition precondition = preconditionFromEtag(args);
    if (precondition != null) {
      builder.setPrecondition(precondition);
    }
    try {
      snapshotsService.deleteSnapshot(builder.build());
      out.println("ok");
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
        out.println(
            "! Precondition failed (etag/version mismatch). Retry with --etag from"
                + " `snapshot get`.");
      } else {
        throw e;
      }
    }
  }

  private static void printSnapshots(List<Snapshot> snaps, PrintStream out) {
    out.printf("%-20s  %-24s  %-20s%n", "SNAPSHOT_ID", "UPSTREAM_CREATED_AT", "PARENT_ID");
    for (var s : snaps) {
      out.printf(
          "%-20d  %-24s  %-20d%n",
          s.getSnapshotId(), ts(s.getUpstreamCreatedAt()), s.getParentSnapshotId());
    }
  }

  private static void printSnapshotDetail(Snapshot snapshot, PrintStream out) {
    try {
      JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields();
      out.println(printer.print(snapshot));
      printDecodedFormatMetadata(snapshot, printer, out);
    } catch (InvalidProtocolBufferException e) {
      out.println(snapshot.toString());
    }
  }

  private static void printDecodedFormatMetadata(
      Snapshot snapshot, JsonFormat.Printer printer, PrintStream out) {
    if (snapshot == null || snapshot.getFormatMetadataCount() == 0) {
      return;
    }
    ByteString icebergRaw = snapshot.getFormatMetadataOrDefault("iceberg", ByteString.EMPTY);
    if (icebergRaw == null || icebergRaw.isEmpty()) {
      return;
    }
    out.println("decoded_format_metadata:");
    try {
      IcebergMetadata metadata = IcebergMetadata.parseFrom(icebergRaw);
      out.println("  iceberg: " + printer.print(metadata));
    } catch (InvalidProtocolBufferException e) {
      out.println("  iceberg: <failed to parse: " + e.getMessage() + ">");
    }
  }

  private static Precondition preconditionFromEtag(List<String> args) {
    String etag = CliArgs.parseStringFlag(args, "--etag", "");
    if (etag == null || etag.isBlank()) {
      return null;
    }
    return Precondition.newBuilder().setExpectedEtag(etag).build();
  }

  private static String ts(Timestamp t) {
    if (t == null || (t.getSeconds() == 0 && t.getNanos() == 0)) {
      return "-";
    }
    return Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toString();
  }
}
