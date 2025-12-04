package io.floecat.tools.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.metacat.query.rpc.BuiltinRegistry;
import ai.floedb.metacat.query.rpc.SqlAggregate;
import ai.floedb.metacat.query.rpc.SqlCast;
import ai.floedb.metacat.query.rpc.SqlCollation;
import ai.floedb.metacat.query.rpc.SqlFunction;
import ai.floedb.metacat.query.rpc.SqlOperator;
import ai.floedb.metacat.query.rpc.SqlType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for the standalone builtin catalog validator CLI. */
class BuiltinCatalogValidatorCliTest {

  /** Happy path coverage ensuring a valid catalog reports success and prints the summary. */
  @Test
  void validatesBinaryCatalog() throws Exception {
    Path catalogPath = writeBinaryCatalog(sampleCatalog());
    var stdout = new ByteArrayOutputStream();
    var stderr = new ByteArrayOutputStream();

    int exit =
        new BuiltinCatalogValidatorCli()
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
        new BuiltinCatalogValidatorCli()
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
    BuiltinRegistry broken =
        BuiltinRegistry.newBuilder().addFunctions(simpleFunction("pg_catalog.missing")).build();

    Path catalogPath = writeBinaryCatalog(broken);
    var stdout = new ByteArrayOutputStream();
    var stderr = new ByteArrayOutputStream();

    int exit =
        new BuiltinCatalogValidatorCli()
            .run(
                new String[] {catalogPath.toString()},
                new PrintStream(stdout),
                new PrintStream(stderr));

    assertEquals(1, exit);
    String out = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(out.contains("ERROR"));
  }

  private static Path writeBinaryCatalog(BuiltinRegistry catalog) throws IOException {
    Path tempFile = Files.createTempFile("builtin_catalog", ".pb");
    Files.write(tempFile, catalog.toByteArray());
    tempFile.toFile().deleteOnExit();
    return tempFile;
  }

  /** Produces a small valid BuiltinRegistry using the SQL-neutral protobuf model. */
  private static BuiltinRegistry sampleCatalog() {

    SqlFunction identity =
        SqlFunction.newBuilder()
            .setName("pg_catalog.int4_identity")
            .addAllArgumentTypes(List.of("pg_catalog.int4"))
            .setReturnType("pg_catalog.int4")
            .build();

    SqlFunction sumState =
        SqlFunction.newBuilder()
            .setName("pg_catalog.sum_int4_state")
            .addAllArgumentTypes(List.of("pg_catalog.int4", "pg_catalog.int4"))
            .setReturnType("pg_catalog.int4")
            .build();

    SqlFunction sumFinal =
        SqlFunction.newBuilder()
            .setName("pg_catalog.sum_int4_final")
            .addAllArgumentTypes(List.of("pg_catalog.int4"))
            .setReturnType("pg_catalog.int4")
            .build();

    SqlAggregate sumAgg =
        SqlAggregate.newBuilder()
            .setName("pg_catalog.sum")
            .addAllArgumentTypes(List.of("pg_catalog.int4"))
            .setStateType("pg_catalog.int4")
            .setReturnType("pg_catalog.int4")
            .build();

    return BuiltinRegistry.newBuilder()
        .addTypes(
            SqlType.newBuilder()
                .setName("pg_catalog.int4")
                .setCategory("N")
                .setIsArray(false)
                .build())
        .addTypes(
            SqlType.newBuilder()
                .setName("pg_catalog._int4")
                .setCategory("A")
                .setIsArray(true)
                .setElementType("pg_catalog.int4")
                .build())
        .addFunctions(identity)
        .addFunctions(sumState)
        .addFunctions(sumFinal)
        .addOperators(
            SqlOperator.newBuilder()
                .setName("pg_catalog.plus")
                .setLeftType("pg_catalog.int4")
                .setRightType("pg_catalog.int4")
                .setReturnType("pg_catalog.int4")
                .build())
        .addCasts(
            SqlCast.newBuilder()
                .setSourceType("pg_catalog.int4")
                .setTargetType("pg_catalog.int4")
                .setMethod("assignment")
                .build())
        .addCollations(
            SqlCollation.newBuilder().setName("pg_catalog.default").setLocale("en_US").build())
        .addAggregates(sumAgg)
        .build();
  }

  private static SqlFunction simpleFunction(String name) {
    return SqlFunction.newBuilder().setName(name).setReturnType("pg_catalog.missing").build();
  }
}
