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

import ai.floedb.floecat.catalog.rpc.AddTableConstraintRequest;
import ai.floedb.floecat.catalog.rpc.AppendTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.DeleteTableConstraintRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.GetTableConstraintsResponse;
import ai.floedb.floecat.catalog.rpc.ListTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.ListTableConstraintsResponse;
import ai.floedb.floecat.catalog.rpc.MergeTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.PutTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableConstraintsServiceGrpc;
import ai.floedb.floecat.client.cli.util.Quotes;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

final class ConstraintsCliSupport {
  private enum BundleMutationMode {
    PUT_REPLACE,
    UPDATE_MERGE,
    ADD_APPEND
  }

  @FunctionalInterface
  interface JsonPrinter {
    void print(MessageOrBuilder message);
  }

  private static final String USAGE =
      "usage: constraints <get|list|put|update|add|delete|add-one|delete-one|add-pk|add-unique|add-not-null|add-check|add-fk> ...";

  /** Static utility holder for `constraints` shell subcommands. */
  private ConstraintsCliSupport() {}

  /**
   * Dispatches `constraints` subcommands and delegates to the matching handler.
   *
   * <p>Supported verbs: {@code get}, {@code list}, {@code put}, {@code update}, {@code add}, {@code
   * delete}, {@code add-one}, {@code delete-one}, {@code add-pk}, {@code add-unique}, {@code
   * add-not-null}, {@code add-check}, {@code add-fk}.
   */
  static void handle(
      List<String> args,
      PrintStream out,
      TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId,
      JsonPrinter printJson) {
    if (args.isEmpty()) {
      out.println(USAGE);
      return;
    }

    String sub = args.get(0);
    List<String> tail = args.subList(1, args.size());
    switch (sub) {
      case "get" -> get(tail, out, constraintsService, snapshotsService, resolveTableId, printJson);
      case "list" -> list(tail, out, constraintsService, resolveTableId, printJson);
      case "put" ->
          put(
              BundleMutationMode.PUT_REPLACE,
              tail,
              out,
              constraintsService,
              snapshotsService,
              resolveTableId,
              printJson);
      case "update" ->
          put(
              BundleMutationMode.UPDATE_MERGE,
              tail,
              out,
              constraintsService,
              snapshotsService,
              resolveTableId,
              printJson);
      case "add" ->
          put(
              BundleMutationMode.ADD_APPEND,
              tail,
              out,
              constraintsService,
              snapshotsService,
              resolveTableId,
              printJson);
      case "delete" -> delete(tail, out, constraintsService, snapshotsService, resolveTableId);
      case "add-one" ->
          addOne(tail, out, constraintsService, snapshotsService, resolveTableId, printJson);
      case "delete-one" ->
          deleteOne(tail, out, constraintsService, snapshotsService, resolveTableId, printJson);
      case "add-pk" ->
          addTyped(
              "add-pk",
              ConstraintType.CT_PRIMARY_KEY,
              tail,
              out,
              constraintsService,
              snapshotsService,
              resolveTableId,
              printJson);
      case "add-unique" ->
          addTyped(
              "add-unique",
              ConstraintType.CT_UNIQUE,
              tail,
              out,
              constraintsService,
              snapshotsService,
              resolveTableId,
              printJson);
      case "add-not-null" ->
          addTyped(
              "add-not-null",
              ConstraintType.CT_NOT_NULL,
              tail,
              out,
              constraintsService,
              snapshotsService,
              resolveTableId,
              printJson);
      case "add-check" ->
          addCheck(tail, out, constraintsService, snapshotsService, resolveTableId, printJson);
      case "add-fk" ->
          addFk(tail, out, constraintsService, snapshotsService, resolveTableId, printJson);
      default -> out.println(USAGE);
    }
  }

  /** Fetches one snapshot constraints bundle and prints plain or JSON output. */
  private static void get(
      List<String> args,
      PrintStream out,
      TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId,
      JsonPrinter printJson) {
    if (args.isEmpty()) {
      out.println(
          "usage: constraints get <id|catalog.ns[.ns...].table> [--snapshot <id>] [--json]");
      return;
    }
    if (!hasExpectedPositionals(args, 1)) {
      out.println(
          "usage: constraints get <id|catalog.ns[.ns...].table> [--snapshot <id>] [--json]");
      return;
    }
    ResourceId tableId = resolveTableId.apply(args.get(0));
    long snapshotId = resolveSnapshotId(args, tableId, snapshotsService);
    GetTableConstraintsResponse response =
        constraintsService.getTableConstraints(
            GetTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .build());
    if (CliArgs.hasFlag(args, "--json")) {
      printJson.print(response);
      return;
    }

    SnapshotConstraints constraints = response.getConstraints();
    out.printf(
        "constraints table_id=%s snapshot_id=%d count=%d%n",
        constraints.getTableId().getId(),
        constraints.getSnapshotId(),
        constraints.getConstraintsCount());
    if (response.hasMeta()) {
      out.printf(
          "meta pointer=%s version=%d etag=%s%n",
          response.getMeta().getPointerKey(),
          response.getMeta().getPointerVersion(),
          response.getMeta().getEtag());
    }
  }

  /** Lists snapshot constraints bundles for a table with optional limit and JSON rendering. */
  private static void list(
      List<String> args,
      PrintStream out,
      TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService,
      Function<String, ResourceId> resolveTableId,
      JsonPrinter printJson) {
    if (args.isEmpty()) {
      out.println("usage: constraints list <id|catalog.ns[.ns...].table> [--limit N] [--json]");
      return;
    }
    if (!hasExpectedPositionals(args, 1)) {
      out.println("usage: constraints list <id|catalog.ns[.ns...].table> [--limit N] [--json]");
      return;
    }
    ResourceId tableId = resolveTableId.apply(args.get(0));
    int limit = Math.max(1, CliArgs.parseIntFlag(args, "--limit", 100));
    ListTableConstraintsResponse response =
        constraintsService.listTableConstraints(
            ListTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setPage(PageRequest.newBuilder().setPageSize(limit).build())
                .build());
    if (CliArgs.hasFlag(args, "--json")) {
      printJson.print(response);
      return;
    }

    out.printf(
        "constraints total=%d returned=%d next_token=%s%n",
        response.getPage().getTotalSize(),
        response.getConstraintsCount(),
        response.getPage().getNextPageToken().isBlank()
            ? "<none>"
            : response.getPage().getNextPageToken());
    for (SnapshotConstraints constraints : response.getConstraintsList()) {
      out.printf(
          "  snapshot_id=%d constraint_count=%d%n",
          constraints.getSnapshotId(), constraints.getConstraintsCount());
    }
  }

  /** Writes/updates/adds snapshot constraints bundle from a JSON file payload. */
  private static void put(
      BundleMutationMode mode,
      List<String> args,
      PrintStream out,
      TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId,
      JsonPrinter printJson) {
    if (!hasExpectedPositionals(args, 1)) {
      out.println(usageForBundleMutation(mode));
      return;
    }
    String file = requiredBundleFileArg(mode, args, out);
    if (file == null) {
      return;
    }

    ResourceId tableId = resolveTableId.apply(args.get(0));
    long snapshotId = resolveSnapshotId(args, tableId, snapshotsService);
    SnapshotConstraints constraints = readSnapshotConstraints(Path.of(Quotes.unquote(file)));
    switch (mode) {
      case PUT_REPLACE -> {
        PutTableConstraintsRequest.Builder request =
            PutTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setConstraints(constraints);
        String key = CliArgs.parseStringFlag(args, "--idempotency", "").trim();
        if (!key.isBlank()) {
          request.setIdempotency(IdempotencyKey.newBuilder().setKey(Quotes.unquote(key)).build());
        }
        var response = constraintsService.putTableConstraints(request.build());
        if (!printJsonIfRequested(args, printJson, response)) {
          printBundleMutationOk(out, response.getConstraints());
        }
      }
      case UPDATE_MERGE -> {
        MergeTableConstraintsRequest.Builder request =
            MergeTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setConstraints(constraints);
        Precondition precondition = preconditionFromFlags(args);
        if (precondition != null) {
          request.setPrecondition(precondition);
        }
        var response = constraintsService.mergeTableConstraints(request.build());
        if (!printJsonIfRequested(args, printJson, response)) {
          printBundleMutationOk(out, response.getConstraints());
        }
      }
      case ADD_APPEND -> {
        AppendTableConstraintsRequest.Builder request =
            AppendTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setConstraints(constraints);
        Precondition precondition = preconditionFromFlags(args);
        if (precondition != null) {
          request.setPrecondition(precondition);
        }
        var response = constraintsService.appendTableConstraints(request.build());
        if (!printJsonIfRequested(args, printJson, response)) {
          printBundleMutationOk(out, response.getConstraints());
        }
      }
    }
  }

  /** Deletes one snapshot constraints bundle. */
  private static void delete(
      List<String> args,
      PrintStream out,
      TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId) {
    if (args.isEmpty()) {
      out.println("usage: constraints delete <id|catalog.ns[.ns...].table> [--snapshot <id>]");
      return;
    }
    if (!hasExpectedPositionals(args, 1)) {
      out.println("usage: constraints delete <id|catalog.ns[.ns...].table> [--snapshot <id>]");
      return;
    }
    ResourceId tableId = resolveTableId.apply(args.get(0));
    long snapshotId = resolveSnapshotId(args, tableId, snapshotsService);
    constraintsService.deleteTableConstraints(
        DeleteTableConstraintsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .build());
    out.println("ok");
  }

  /** Adds or replaces one named constraint while preserving other constraints in the bundle. */
  private static void addOne(
      List<String> args,
      PrintStream out,
      TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId,
      JsonPrinter printJson) {
    if (args.isEmpty()) {
      out.println(
          "usage: constraints add-one <id|catalog.ns[.ns...].table> [--snapshot <id>] --file"
              + " <constraint_definition_json>"
              + " [--etag <etag>|--version <n>] [--json]");
      return;
    }
    if (!hasExpectedPositionals(args, 1)) {
      out.println(
          "usage: constraints add-one <id|catalog.ns[.ns...].table> [--snapshot <id>] --file"
              + " <constraint_definition_json>"
              + " [--etag <etag>|--version <n>] [--json]");
      return;
    }
    ResourceId tableId = resolveTableId.apply(args.get(0));
    long snapshotId = resolveSnapshotId(args, tableId, snapshotsService);
    String file = CliArgs.parseStringFlag(args, "--file", "").trim();
    if (file.isBlank()) {
      out.println(
          "usage: constraints add-one <id|catalog.ns[.ns...].table> [--snapshot <id>] --file"
              + " <constraint_definition_json>"
              + " [--etag <etag>|--version <n>] [--json]");
      return;
    }
    ConstraintDefinition constraint = readConstraintDefinition(Path.of(Quotes.unquote(file)));

    AddTableConstraintRequest.Builder request = addRequest(tableId, snapshotId, constraint);
    Precondition precondition = preconditionFromFlags(args);
    if (precondition != null) {
      request.setPrecondition(precondition);
    }

    var response = constraintsService.addTableConstraint(request.build());
    if (!printJsonIfRequested(args, printJson, response)) {
      printBundleMutationOk(out, response.getConstraints());
    }
  }

  /** Adds PK/UNIQUE/NOT NULL constraints from explicit name + columns input. */
  private static void addTyped(
      String command,
      ConstraintType type,
      List<String> args,
      PrintStream out,
      TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId,
      JsonPrinter printJson) {
    String usage =
        "usage: constraints "
            + command
            + " <id|catalog.ns[.ns...].table> <constraint_name>"
            + " <columns_csv>"
            + " [--snapshot <id>]"
            + " [--etag <etag>|--version <n>] [--json]";
    if (args.isEmpty()) {
      out.println(usage);
      return;
    }
    if (!hasExpectedPositionals(args, 3)) {
      out.println(usage);
      return;
    }
    ResourceId tableId = resolveTableId.apply(args.get(0));
    long snapshotId = resolveSnapshotId(args, tableId, snapshotsService);
    String name = Quotes.unquote(args.get(1)).trim();
    if (name.isEmpty()) {
      out.println("constraint_name cannot be blank");
      return;
    }
    List<String> columns = parseColumnNames(args.get(2));
    if (type == ConstraintType.CT_NOT_NULL && columns.size() != 1) {
      out.println("add-not-null requires exactly one column");
      return;
    }
    ConstraintDefinition constraint =
        ConstraintDefinition.newBuilder()
            .setName(name)
            .setType(type)
            .addAllColumns(columnRefs(columns))
            .build();
    addConstraintAndPrint(
        args, out, constraintsService, printJson, addRequest(tableId, snapshotId, constraint));
  }

  /** Adds a CHECK constraint from explicit name + expression input. */
  private static void addCheck(
      List<String> args,
      PrintStream out,
      TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId,
      JsonPrinter printJson) {
    String usage =
        "usage: constraints add-check <id|catalog.ns[.ns...].table>"
            + " <constraint_name> <check_expression>"
            + " [--snapshot <id>]"
            + " [--etag <etag>|--version <n>] [--json]";
    if (args.isEmpty()) {
      out.println(usage);
      return;
    }
    if (!hasExpectedPositionals(args, 3)) {
      out.println(usage);
      return;
    }
    ResourceId tableId = resolveTableId.apply(args.get(0));
    long snapshotId = resolveSnapshotId(args, tableId, snapshotsService);
    String name = Quotes.unquote(args.get(1)).trim();
    String expression = Quotes.unquote(args.get(2)).trim();
    if (name.isEmpty() || expression.isEmpty()) {
      out.println("constraint_name and check_expression cannot be blank");
      return;
    }
    ConstraintDefinition constraint =
        ConstraintDefinition.newBuilder()
            .setName(name)
            .setType(ConstraintType.CT_CHECK)
            .setCheckExpression(expression)
            .build();
    addConstraintAndPrint(
        args, out, constraintsService, printJson, addRequest(tableId, snapshotId, constraint));
  }

  /** Adds an FK constraint from explicit local/referenced table+column args. */
  private static void addFk(
      List<String> args,
      PrintStream out,
      TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId,
      JsonPrinter printJson) {
    String usage =
        "usage: constraints add-fk <id|catalog.ns[.ns...].table>"
            + " <constraint_name> <local_columns_csv> <referenced_table>"
            + " <referenced_columns_csv>"
            + " [--snapshot <id>]"
            + " [--etag <etag>|--version <n>] [--json]";
    if (args.isEmpty()) {
      out.println(usage);
      return;
    }
    if (!hasExpectedPositionals(args, 5)) {
      out.println(usage);
      return;
    }
    ResourceId tableId = resolveTableId.apply(args.get(0));
    long snapshotId = resolveSnapshotId(args, tableId, snapshotsService);
    String name = Quotes.unquote(args.get(1)).trim();
    List<String> localColumns = parseColumnNames(args.get(2));
    String referencedTableRaw = Quotes.unquote(args.get(3)).trim();
    List<String> referencedColumns = parseColumnNames(args.get(4));
    if (name.isEmpty() || referencedTableRaw.isEmpty()) {
      out.println("constraint_name and referenced_table cannot be blank");
      return;
    }
    if (localColumns.size() != referencedColumns.size()) {
      out.println("local_columns_csv and referenced_columns_csv must have the same length");
      return;
    }
    NameRef referencedTable = parseReferencedTableRef(referencedTableRaw);
    ConstraintDefinition constraint =
        ConstraintDefinition.newBuilder()
            .setName(name)
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .addAllColumns(columnRefs(localColumns))
            .setReferencedTable(referencedTable)
            .addAllReferencedColumns(columnRefs(referencedColumns))
            .build();
    addConstraintAndPrint(
        args, out, constraintsService, printJson, addRequest(tableId, snapshotId, constraint));
  }

  private static AddTableConstraintRequest.Builder addRequest(
      ResourceId tableId, long snapshotId, ConstraintDefinition constraint) {
    return AddTableConstraintRequest.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setConstraint(constraint);
  }

  private static NameRef parseReferencedTableRef(String raw) {
    String token = raw == null ? "" : raw.trim();
    if (token.isEmpty()) {
      throw new IllegalArgumentException("referenced_table cannot be blank");
    }
    String[] parts = token.split("\\.");
    if (parts.length < 2) {
      throw new IllegalArgumentException(
          "referenced_table must be fully qualified: catalog.ns[.ns...].table");
    }
    String catalog = parts[0].trim();
    String tableName = parts[parts.length - 1].trim();
    if (catalog.isEmpty() || tableName.isEmpty()) {
      throw new IllegalArgumentException(
          "referenced_table must be fully qualified: catalog.ns[.ns...].table");
    }
    NameRef.Builder b = NameRef.newBuilder().setCatalog(catalog).setName(tableName);
    for (int i = 1; i < parts.length - 1; i++) {
      String segment = parts[i].trim();
      if (segment.isEmpty()) {
        throw new IllegalArgumentException(
            "referenced_table must be fully qualified: catalog.ns[.ns...].table");
      }
      b.addPath(segment);
    }
    return b.build();
  }

  private static void addConstraintAndPrint(
      List<String> args,
      PrintStream out,
      TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService,
      JsonPrinter printJson,
      AddTableConstraintRequest.Builder request) {
    Precondition precondition = preconditionFromFlags(args);
    if (precondition != null) {
      request.setPrecondition(precondition);
    }
    var response = constraintsService.addTableConstraint(request.build());
    if (!printJsonIfRequested(args, printJson, response)) {
      printBundleMutationOk(out, response.getConstraints());
    }
  }

  /** Deletes one named constraint while preserving other constraints in the bundle. */
  private static void deleteOne(
      List<String> args,
      PrintStream out,
      TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      Function<String, ResourceId> resolveTableId,
      JsonPrinter printJson) {
    String usage =
        "usage: constraints delete-one <id|catalog.ns[.ns...].table>"
            + " <constraint_name> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]";
    if (args.isEmpty()) {
      out.println(usage);
      return;
    }
    if (!hasExpectedPositionals(args, 2)) {
      out.println(usage);
      return;
    }
    ResourceId tableId = resolveTableId.apply(args.get(0));
    long snapshotId = resolveSnapshotId(args, tableId, snapshotsService);
    String constraintName = Quotes.unquote(args.get(1)).trim();
    if (constraintName.isEmpty()) {
      out.println("constraint_name cannot be blank");
      return;
    }

    DeleteTableConstraintRequest.Builder request =
        DeleteTableConstraintRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .setConstraintName(constraintName);
    Precondition precondition = preconditionFromFlags(args);
    if (precondition != null) {
      request.setPrecondition(precondition);
    }

    var response = constraintsService.deleteTableConstraint(request.build());
    if (!printJsonIfRequested(args, printJson, response)) {
      printBundleMutationOk(out, response.getConstraints());
    }
  }

  static boolean hasExpectedPositionals(List<String> args, int expectedPositionals) {
    int count = 0;
    for (int i = 0; i < args.size(); i++) {
      String token = args.get(i);
      if (token.startsWith("--")) {
        if (flagConsumesValue(token)) {
          i++;
        }
        continue;
      }
      count++;
    }
    return count == expectedPositionals;
  }

  private static boolean flagConsumesValue(String flag) {
    return "--snapshot".equals(flag)
        || "--file".equals(flag)
        || "--etag".equals(flag)
        || "--version".equals(flag)
        || "--idempotency".equals(flag)
        || "--limit".equals(flag);
  }

  /** Reads a JSON-encoded {@link SnapshotConstraints} payload from disk. */
  private static SnapshotConstraints readSnapshotConstraints(Path filePath) {
    try {
      String json = Files.readString(filePath);
      SnapshotConstraints.Builder builder = SnapshotConstraints.newBuilder();
      JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
      return builder.build();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "failed to read constraints payload from " + filePath + ": " + e.getMessage(), e);
    }
  }

  /** Reads a JSON-encoded {@link ConstraintDefinition} payload from disk. */
  private static ConstraintDefinition readConstraintDefinition(Path filePath) {
    try {
      String json = Files.readString(filePath);
      ConstraintDefinition.Builder builder = ConstraintDefinition.newBuilder();
      JsonFormat.parser().ignoringUnknownFields().merge(json, builder);
      return builder.build();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "failed to read constraint payload from " + filePath + ": " + e.getMessage(), e);
    }
  }

  /** Parses optional `--etag` / `--version` precondition flags for mutation commands. */
  private static Precondition preconditionFromFlags(List<String> args) {
    String etag = Quotes.unquote(CliArgs.parseStringFlag(args, "--etag", "")).trim();
    String versionRaw = Quotes.unquote(CliArgs.parseStringFlag(args, "--version", "")).trim();
    if (etag.isEmpty() && versionRaw.isEmpty()) {
      return null;
    }
    Precondition.Builder precondition = Precondition.newBuilder();
    if (!etag.isEmpty()) {
      precondition.setExpectedEtag(etag);
    }
    if (!versionRaw.isEmpty()) {
      precondition.setExpectedVersion(Long.parseLong(versionRaw));
    }
    return precondition.build();
  }

  static long resolveSnapshotId(
      List<String> args,
      ResourceId tableId,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService) {
    int snapshotIndex = args.indexOf("--snapshot");
    if (snapshotIndex < 0) {
      return resolveCurrentSnapshotId(tableId, snapshotsService);
    }
    if (snapshotIndex + 1 >= args.size() || args.get(snapshotIndex + 1).startsWith("--")) {
      throw new IllegalArgumentException("snapshot_id must be provided after --snapshot");
    }
    String snapshotRaw = Quotes.unquote(args.get(snapshotIndex + 1)).trim();
    if (snapshotRaw.isEmpty()) {
      throw new IllegalArgumentException("snapshot_id must be provided after --snapshot");
    }
    try {
      return Long.parseLong(snapshotRaw);
    } catch (NumberFormatException ignored) {
      throw new IllegalArgumentException("snapshot_id must be a valid integer: " + snapshotRaw);
    }
  }

  /** Resolves the current snapshot id for the given table. */
  private static long resolveCurrentSnapshotId(
      ResourceId tableId, SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService) {
    var response =
        snapshotsService.getSnapshot(
            GetSnapshotRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                .build());
    return response.getSnapshot().getSnapshotId();
  }

  private static List<String> parseColumnNames(String rawColumns) {
    String[] pieces = Quotes.unquote(rawColumns).split(",");
    List<String> out = new ArrayList<>(pieces.length);
    for (String piece : pieces) {
      String name = Quotes.unquote(piece.trim());
      if (name.isEmpty()) {
        throw new IllegalArgumentException("column list contains an empty name");
      }
      out.add(name);
    }
    return out;
  }

  private static List<ConstraintColumnRef> columnRefs(List<String> columns) {
    List<ConstraintColumnRef> refs = new ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      refs.add(
          ConstraintColumnRef.newBuilder().setColumnName(columns.get(i)).setOrdinal(i + 1).build());
    }
    return refs;
  }

  private static String usageForBundleMutation(BundleMutationMode mode) {
    return switch (mode) {
      case PUT_REPLACE ->
          "usage: constraints put <id|catalog.ns[.ns...].table> [--snapshot <id>] --file"
              + " <snapshot_constraints_json> [--idempotency <key>] [--json]";
      case UPDATE_MERGE ->
          "usage: constraints update <id|catalog.ns[.ns...].table> [--snapshot <id>] --file"
              + " <snapshot_constraints_json> [--etag <etag>|--version <n>] [--json]";
      case ADD_APPEND ->
          "usage: constraints add <id|catalog.ns[.ns...].table> [--snapshot <id>] --file"
              + " <snapshot_constraints_json> [--etag <etag>|--version <n>] [--json]";
    };
  }

  private static String requiredBundleFileArg(
      BundleMutationMode mode, List<String> args, PrintStream out) {
    if (args.isEmpty()) {
      out.println(usageForBundleMutation(mode));
      return null;
    }
    String file = CliArgs.parseStringFlag(args, "--file", "");
    if (file.isBlank()) {
      out.println(usageForBundleMutation(mode));
      return null;
    }
    return file;
  }

  private static boolean printJsonIfRequested(
      List<String> args, JsonPrinter printJson, MessageOrBuilder response) {
    if (CliArgs.hasFlag(args, "--json")) {
      printJson.print(response);
      return true;
    }
    return false;
  }

  private static void printBundleMutationOk(PrintStream out, SnapshotConstraints constraints) {
    out.printf(
        "ok table_id=%s snapshot_id=%d constraint_count=%d%n",
        constraints.getTableId().getId(),
        constraints.getSnapshotId(),
        constraints.getConstraintsCount());
  }
}
