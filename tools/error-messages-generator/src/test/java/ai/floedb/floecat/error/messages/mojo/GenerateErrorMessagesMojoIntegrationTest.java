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

package ai.floedb.floecat.error.messages.mojo;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.project.MavenProject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GenerateErrorMessagesMojoIntegrationTest {
  @TempDir Path tempDir;

  @Test
  void generatesRegistryFromRealBundles() throws Exception {
    Path env = tempDir.resolve("success");
    Path resources = env.resolve("resources");
    Path generated = env.resolve("generated");
    Files.createDirectories(resources);
    Files.createDirectories(generated);
    Path proto = writeProto(env.resolve("proto"));

    writeBundle(
        resources,
        "errors_en.properties",
        "MC_NOT_FOUND=Resource not found.",
        "MC_NOT_FOUND.table=Table {id} missing.",
        "MC_INVALID_ARGUMENT.field=Bad value for {field}.");
    writeBundle(
        resources,
        "errors_fr.properties",
        "MC_NOT_FOUND=Ressource introuvable.",
        "MC_NOT_FOUND.table=Table {id} introuvable.",
        "MC_INVALID_ARGUMENT.field=Valeur invalide pour {field}.");

    GenerateErrorMessagesMojo mojo = mojo(resources, generated, proto);
    assertDoesNotThrow(mojo::execute);

    Path output =
        generated.resolve("ai/floedb/floecat/service/error/impl/GeneratedErrorMessages.java");
    assertTrue(Files.exists(output), "Generated file should exist");
    String content = Files.readString(output, StandardCharsets.UTF_8);
    assertTrue(content.contains("TABLE(ErrorCode.MC_NOT_FOUND"), "Table key should be emitted");
    assertTrue(content.contains("FIELD(ErrorCode.MC_INVALID_ARGUMENT"));
  }

  @Test
  void localeMissingKeyFails() throws Exception {
    Path env = tempDir.resolve("missing-key");
    Path resources = env.resolve("resources");
    Path generated = env.resolve("generated");
    Files.createDirectories(resources);
    Files.createDirectories(generated);
    Path proto = writeProto(env.resolve("proto"));

    writeBundle(
        resources,
        "errors_en.properties",
        "MC_NOT_FOUND=Resource not found.",
        "MC_NOT_FOUND.table=Table {id} missing.");
    writeBundle(resources, "errors_fr.properties", "MC_NOT_FOUND=Ressource introuvable.");

    GenerateErrorMessagesMojo mojo = mojo(resources, generated, proto);
    MojoFailureException failure = assertThrows(MojoFailureException.class, mojo::execute);
    assertTrue(
        failure.getMessage().toLowerCase().contains("missing keys"),
        "Expected failure mentioning missing keys but got: " + failure.getMessage());
  }

  @Test
  void placeholderMismatchFails() throws Exception {
    Path env = tempDir.resolve("placeholder");
    Path resources = env.resolve("resources");
    Path generated = env.resolve("generated");
    Files.createDirectories(resources);
    Files.createDirectories(generated);
    Path proto = writeProto(env.resolve("proto"));

    writeBundle(resources, "errors_en.properties", "MC_NOT_FOUND.table=Table {id} missing.");
    writeBundle(resources, "errors_fr.properties", "MC_NOT_FOUND.table=Table manquante.");

    GenerateErrorMessagesMojo mojo = mojo(resources, generated, proto);
    MojoFailureException failure = assertThrows(MojoFailureException.class, mojo::execute);
    assertTrue(
        failure.getMessage().toLowerCase().contains("placeholder mismatch"),
        "Expected placeholder mismatch failure but got: " + failure.getMessage());
  }

  @Test
  void unknownPrefixFails() throws Exception {
    Path env = tempDir.resolve("unknown-prefix");
    Path resources = env.resolve("resources");
    Path generated = env.resolve("generated");
    Files.createDirectories(resources);
    Files.createDirectories(generated);
    Path proto = writeProto(env.resolve("proto"));

    writeBundle(resources, "errors_en.properties", "MC_UNKNOWN.foo=Bad key.");
    writeBundle(resources, "errors_fr.properties", "MC_UNKNOWN.foo=Mauvaise clé.");

    GenerateErrorMessagesMojo mojo = mojo(resources, generated, proto);
    MojoFailureException failure = assertThrows(MojoFailureException.class, mojo::execute);
    assertTrue(
        failure.getMessage().contains("Unknown error code prefix"),
        "Expected prefix validation failure but got: " + failure.getMessage());
  }

  @Test
  void duplicateSuffixFails() throws Exception {
    Path env = tempDir.resolve("duplicate-suffix");
    Path resources = env.resolve("resources");
    Path generated = env.resolve("generated");
    Files.createDirectories(resources);
    Files.createDirectories(generated);
    Path proto = writeProto(env.resolve("proto"));

    writeBundle(
        resources,
        "errors_en.properties",
        "MC_NOT_FOUND.table=Table {id}.",
        "MC_INVALID_ARGUMENT.table=Bad table.");
    writeBundle(
        resources,
        "errors_fr.properties",
        "MC_NOT_FOUND.table=Table {id}.",
        "MC_INVALID_ARGUMENT.table=Mauvaise table.");

    GenerateErrorMessagesMojo mojo = mojo(resources, generated, proto);
    MojoFailureException failure = assertThrows(MojoFailureException.class, mojo::execute);
    assertTrue(
        failure.getMessage().contains("Duplicate suffix"),
        "Expected duplicate suffix failure but got: " + failure.getMessage());
  }

  @Test
  void underscoreSuffixFails() throws Exception {
    Path env = tempDir.resolve("underscore-suffix");
    Path resources = env.resolve("resources");
    Path generated = env.resolve("generated");
    Files.createDirectories(resources);
    Files.createDirectories(generated);
    Path proto = writeProto(env.resolve("proto"));

    writeBundle(resources, "errors_en.properties", "MC_NOT_FOUND.table_name=Bad key.");
    writeBundle(resources, "errors_fr.properties", "MC_NOT_FOUND.table_name=Mauvaise clé.");

    GenerateErrorMessagesMojo mojo = mojo(resources, generated, proto);
    MojoFailureException failure = assertThrows(MojoFailureException.class, mojo::execute);
    assertTrue(
        failure.getMessage().contains("Invalid message suffix"),
        "Expected suffix format failure but got: " + failure.getMessage());
  }

  private GenerateErrorMessagesMojo mojo(Path resources, Path generated, Path proto)
      throws Exception {
    GenerateErrorMessagesMojo mojo = new GenerateErrorMessagesMojo();
    setField(mojo, "project", new MavenProject());
    setField(mojo, "resourcesDirectory", resources.toFile());
    setField(mojo, "generatedSourcesDirectory", generated.toFile());
    setField(mojo, "errorCodeProto", proto.toFile());
    return mojo;
  }

  private static void setField(GenerateErrorMessagesMojo mojo, String name, Object value)
      throws Exception {
    Field field = GenerateErrorMessagesMojo.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(mojo, value);
  }

  private static Path writeProto(Path dir) throws IOException {
    Files.createDirectories(dir);
    Path proto = dir.resolve("common.proto");
    String content =
        """
        syntax = "proto3";

        package ai.floedb.floecat.common;

        enum ErrorCode {
          MC_UNSPECIFIED = 0;
          MC_INVALID_ARGUMENT = 1;
          MC_NOT_FOUND = 2;
          MC_CONFLICT = 3;
        }
        """;
    Files.writeString(proto, content, StandardCharsets.UTF_8);
    return proto;
  }

  private static void writeBundle(Path resources, String name, String... lines) throws IOException {
    Files.writeString(
        resources.resolve(name), String.join("\n", lines) + "\n", StandardCharsets.ISO_8859_1);
  }
}
