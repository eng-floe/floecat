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

package io.floecat.tools.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.SqlAggregate;
import ai.floedb.floecat.query.rpc.SqlCast;
import ai.floedb.floecat.query.rpc.SqlCollation;
import ai.floedb.floecat.query.rpc.SqlFunction;
import ai.floedb.floecat.query.rpc.SqlOperator;
import ai.floedb.floecat.query.rpc.SqlType;
import ai.floedb.floecat.query.rpc.SystemObjectsRegistry;
import com.google.protobuf.TextFormat;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for the standalone builtin catalog validator CLI. */
class SystemObjectsValidatorCliTest {

  /** Happy path coverage ensuring a valid catalog reports success and prints the summary. */
  @Test
  void validatesBinaryCatalog() throws Exception {
    Path catalogPath = writeBinaryCatalog(sampleCatalog());
    var stdout = new ByteArrayOutputStream();
    var stderr = new ByteArrayOutputStream();

    int exit =
        new SystemObjectsValidatorCli()
            .run(
                new String[] {catalogPath.toString()},
                new PrintStream(stdout),
                new PrintStream(stderr));

    assertEquals(0, exit);
    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("ALL CHECKS PASSED."));
    assertTrue(output.contains("Types: 2"));
    assertEquals("", stderr.toString(StandardCharsets.UTF_8));
  }

  /** Ensures JSON mode is deterministic so the validator can be consumed by scripts or CI. */
  @Test
  void jsonModeProducesStats() throws Exception {
    Path catalogPath = writeBinaryCatalog(sampleCatalog());
    var stdout = new ByteArrayOutputStream();

    int exit =
        new SystemObjectsValidatorCli()
            .run(
                new String[] {catalogPath.toString(), "--json"},
                new PrintStream(stdout),
                System.err);

    assertEquals(0, exit);
    String json = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(json.contains("\"valid\": true"));
    assertTrue(json.contains("\"types\": 2"));
    assertTrue(json.contains("\"functions\": 3"));
  }

  /**
   * Guards against silent failures by verifying a malformed catalog surfaces the validator errors.
   */
  @Test
  void invalidCatalogReportsErrors() throws Exception {
    SystemObjectsRegistry broken =
        SystemObjectsRegistry.newBuilder()
            .addFunctions(simpleFunction("pg_catalog.missing"))
            .build();

    Path catalogPath = writeBinaryCatalog(broken);
    var stdout = new ByteArrayOutputStream();
    var stderr = new ByteArrayOutputStream();

    int exit =
        new SystemObjectsValidatorCli()
            .run(
                new String[] {catalogPath.toString()},
                new PrintStream(stdout),
                new PrintStream(stderr));

    assertEquals(1, exit);
    String out = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(out.contains("ERROR"));
  }

  private static Path writeBinaryCatalog(SystemObjectsRegistry catalog) throws IOException {
    Path tempFile = Files.createTempFile("builtin_catalog", ".pb");
    Files.write(tempFile, catalog.toByteArray());
    tempFile.toFile().deleteOnExit();
    return tempFile;
  }

  /** Produces a small valid SystemObjectsRegistry using the SQL-neutral protobuf model. */
  private static SystemObjectsRegistry sampleCatalog() {

    SqlFunction identity =
        SqlFunction.newBuilder()
            .setName(NameRef.newBuilder().addPath("pg_catalog").setName("int4_identity"))
            .addArgumentTypes(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
            .setReturnType(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
            .build();

    SqlFunction sumState =
        SqlFunction.newBuilder()
            .setName(NameRef.newBuilder().addPath("pg_catalog").setName("sum_int4_state"))
            .addArgumentTypes(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
            .addArgumentTypes(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
            .setReturnType(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
            .build();

    SqlFunction sumFinal =
        SqlFunction.newBuilder()
            .setName(NameRef.newBuilder().addPath("pg_catalog").setName("sum_int4_final"))
            .addArgumentTypes(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
            .setReturnType(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
            .build();

    SqlAggregate sumAgg =
        SqlAggregate.newBuilder()
            .setName(NameRef.newBuilder().addPath("pg_catalog").setName("sum"))
            .addArgumentTypes(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
            .setStateType(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
            .setReturnType(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
            .build();

    return SystemObjectsRegistry.newBuilder()
        .addTypes(
            SqlType.newBuilder()
                .setName(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
                .setCategory("N")
                .setIsArray(false)
                .build())
        .addTypes(
            SqlType.newBuilder()
                .setName(NameRef.newBuilder().addPath("pg_catalog").setName("_int4"))
                .setCategory("A")
                .setIsArray(true)
                .setElementType(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
                .build())
        .addFunctions(identity)
        .addFunctions(sumState)
        .addFunctions(sumFinal)
        .addOperators(
            SqlOperator.newBuilder()
                .setName(NameRef.newBuilder().addPath("pg_catalog").setName("plus"))
                .setLeftType(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
                .setRightType(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
                .setReturnType(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
                .build())
        .addCasts(
            SqlCast.newBuilder()
                .setName(NameRef.newBuilder().addPath("pg_catalog").setName("int42int4"))
                .setSourceType(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
                .setTargetType(NameRef.newBuilder().addPath("pg_catalog").setName("int4"))
                .setMethod("assignment")
                .build())
        .addCollations(
            SqlCollation.newBuilder()
                .setName(NameRef.newBuilder().addPath("pg_catalog").setName("default"))
                .setLocale("en_US")
                .build())
        .addAggregates(sumAgg)
        .build();
  }

  private static SqlFunction simpleFunction(String name) {
    int lastDot = name.lastIndexOf('.');
    NameRef.Builder nameRefBuilder = NameRef.newBuilder();
    if (lastDot >= 0) {
      String path = name.substring(0, lastDot);
      String simpleName = name.substring(lastDot + 1);
      for (String p : path.split("\\.")) {
        nameRefBuilder.addPath(p);
      }
      nameRefBuilder.setName(simpleName);
    } else {
      nameRefBuilder.setName(name);
    }
    return SqlFunction.newBuilder()
        .setName(nameRefBuilder)
        .setReturnType(NameRef.newBuilder().addPath("pg_catalog").setName("missing"))
        .build();
  }

  @Test
  void engineModeLoadsExtension() throws Exception {
    var stdout = new ByteArrayOutputStream();
    var stderr = new ByteArrayOutputStream();
    int exit =
        new SystemObjectsValidatorCli()
            .run(
                new String[] {"--engine", "floe-demo"},
                new PrintStream(stdout),
                new PrintStream(stderr));
    System.out.println(stdout.toString(StandardCharsets.UTF_8));
    System.out.println(stderr.toString(StandardCharsets.UTF_8));
    assertEquals(0, exit);
    String out = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(out.contains("ALL CHECKS PASSED."));
    assertEquals("", stderr.toString(StandardCharsets.UTF_8));
  }

  @Test
  void indexDirectoryMergesFragments() throws Exception {
    Path dir = Files.createTempDirectory("catalog-index");
    dir.toFile().deleteOnExit();
    Path fragmentA = dir.resolve("types.pbtxt");
    Path fragmentB = dir.resolve("functions.pbtxt");
    writeTextCatalog(typesFragment(), fragmentA);
    writeTextCatalog(functionFragment(), fragmentB);
    writeIndex(dir, List.of("types.pbtxt", "functions.pbtxt"));

    var stdout = new ByteArrayOutputStream();
    var stderr = new ByteArrayOutputStream();
    int exit =
        new SystemObjectsValidatorCli()
            .run(new String[] {dir.toString()}, new PrintStream(stdout), new PrintStream(stderr));

    assertEquals(0, exit);
    String out = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(out.contains("ALL CHECKS PASSED."));
    assertEquals("", stderr.toString(StandardCharsets.UTF_8));
  }

  @Test
  void indexWithParentPathsFails() throws Exception {
    Path dir = Files.createTempDirectory("catalog-index-parent");
    dir.toFile().deleteOnExit();
    writeIndex(dir, List.of("../escape.pbtxt"));
    Path index = dir.resolve("_index.txt");

    var stdout = new ByteArrayOutputStream();
    var stderr = new ByteArrayOutputStream();
    int exit =
        new SystemObjectsValidatorCli()
            .run(new String[] {index.toString()}, new PrintStream(stdout), new PrintStream(stderr));

    assertEquals(1, exit);
    String err = stderr.toString(StandardCharsets.UTF_8);
    assertTrue(err.contains("Invalid catalog fragment"));
  }

  @Test
  void missingFragmentOnIndexFails() throws Exception {
    Path dir = Files.createTempDirectory("catalog-index-missing");
    dir.toFile().deleteOnExit();
    writeIndex(dir, List.of("missing.pbtxt"));
    Path index = dir.resolve("_index.txt");

    var stdout = new ByteArrayOutputStream();
    var stderr = new ByteArrayOutputStream();
    int exit =
        new SystemObjectsValidatorCli()
            .run(new String[] {index.toString()}, new PrintStream(stdout), new PrintStream(stderr));

    assertEquals(1, exit);
    String err = stderr.toString(StandardCharsets.UTF_8);
    assertTrue(err.contains("Catalog fragment does not exist"));
  }

  @Test
  void blankIndexFails() throws Exception {
    Path dir = Files.createTempDirectory("catalog-index-blank");
    dir.toFile().deleteOnExit();
    writeIndex(dir, List.of("", " # comment "));
    Path index = dir.resolve("_index.txt");

    var stdout = new ByteArrayOutputStream();
    var stderr = new ByteArrayOutputStream();
    int exit =
        new SystemObjectsValidatorCli()
            .run(new String[] {index.toString()}, new PrintStream(stdout), new PrintStream(stderr));

    assertEquals(1, exit);
    String err = stderr.toString(StandardCharsets.UTF_8);
    assertTrue(err.contains("Catalog index contains no fragments"));
  }

  private static SystemObjectsRegistry typesFragment() {
    return SystemObjectsRegistry.newBuilder()
        .addTypes(
            SqlType.newBuilder()
                .setName(NameRef.newBuilder().setName("int4"))
                .setCategory("N")
                .setIsArray(false)
                .build())
        .build();
  }

  private static SystemObjectsRegistry functionFragment() {
    return SystemObjectsRegistry.newBuilder()
        .addFunctions(
            SqlFunction.newBuilder()
                .setName(NameRef.newBuilder().addPath("pg_catalog").setName("fragment_func"))
                .addArgumentTypes(NameRef.newBuilder().setName("int4"))
                .setReturnType(NameRef.newBuilder().setName("int4"))
                .build())
        .build();
  }

  private static void writeTextCatalog(SystemObjectsRegistry catalog, Path path)
      throws IOException {
    Files.createDirectories(path.getParent());
    try (var writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      TextFormat.printer().print(catalog, writer);
    }
  }

  private static void writeIndex(Path directory, List<String> entries) throws IOException {
    Path index = directory.resolve("_index.txt");
    Files.write(index, entries, StandardCharsets.UTF_8);
  }
}
