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

package ai.floedb.floecat.extensions.example;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.EngineSpecific;
import ai.floedb.floecat.query.rpc.SystemObjectsRegistry;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.utils.EngineContextNormalizer;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogProtoMapper;
import ai.floedb.floecat.systemcatalog.registry.SystemObjectsRegistryMerger;
import ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

/**
 * File-based catalog extension for Floecat.
 *
 * <p>Designed to work out of the box without any Java coding. Point it at a directory of
 * {@code .pbtxt} files describing your engine's catalog and Floecat will serve it.
 *
 * <h2>Configuration</h2>
 *
 * <ul>
 *   <li>{@code floecat.extension.engine-kind} (env: {@code FLOECAT_EXTENSION_ENGINE_KIND}):
 *       the engine identifier this catalog is served under. Defaults to {@code "example"}.
 *   <li>{@code floecat.extension.builtins-dir} (env: {@code FLOECAT_EXTENSION_BUILTINS_DIR}):
 *       path to a directory of {@code *.pbtxt} catalog files, loaded alphabetically. When absent,
 *       the bundled format-reference catalog is loaded from the classpath.
 * </ul>
 *
 * <h2>PBtxt format</h2>
 *
 * Files must be valid {@code SystemObjectsRegistry} proto text format using core fields only.
 * See the bundled {@code builtins/example/} files for field-by-field format documentation.
 *
 * <p>Proto2 extension syntax ({@code [floe.ext.*]} blocks) is not supported.
 * The {@code engine_kind} field inside {@code engine_specific} blocks is ignored; the catalog's
 * configured engine kind always applies.
 *
 * <h2>Error handling</h2>
 *
 * Unreadable or unparseable files are skipped with a warning. A missing configured directory
 * results in an empty catalog rather than a startup failure.
 */
public final class ExampleCatalogExtension implements EngineSystemCatalogExtension {

  private static final Logger LOG = Logger.getLogger(ExampleCatalogExtension.class);

  /** MicroProfile Config key for the engine kind. Env: {@code FLOECAT_EXTENSION_ENGINE_KIND}. */
  static final String CONFIG_ENGINE_KIND = "floecat.extension.engine-kind";

  /**
   * MicroProfile Config key for the filesystem builtins directory. Env: {@code
   * FLOECAT_EXTENSION_BUILTINS_DIR}.
   */
  static final String CONFIG_BUILTINS_DIR = "floecat.extension.builtins-dir";

  /** Default engine kind when nothing is configured. */
  static final String DEFAULT_ENGINE_KIND = "example";

  /** Classpath resource base for bundled example files. */
  private static final String CLASSPATH_RESOURCE_BASE = "/builtins/example/";

  // ---------------------------------------------------------------------------
  // EngineSystemCatalogExtension
  // ---------------------------------------------------------------------------

  @Override
  public String engineKind() {
    return ConfigProvider.getConfig()
        .getOptionalValue(CONFIG_ENGINE_KIND, String.class)
        .filter(s -> !s.isBlank())
        .orElse(DEFAULT_ENGINE_KIND);
  }

  @Override
  public SystemCatalogData loadSystemCatalog() {
    Optional<String> dirConfig =
        ConfigProvider.getConfig()
            .getOptionalValue(CONFIG_BUILTINS_DIR, String.class)
            .filter(s -> !s.isBlank());

    SystemObjectsRegistry registry =
        dirConfig.isPresent()
            ? loadFromFilesystem(Paths.get(dirConfig.get()))
            : loadFromClasspath();

    return SystemCatalogProtoMapper.fromProto(registry, engineKind());
  }

  // ---------------------------------------------------------------------------
  // Classpath loading (bundled /builtins/example/ resources)
  // ---------------------------------------------------------------------------

  private SystemObjectsRegistry loadFromClasspath() {
    String indexPath = CLASSPATH_RESOURCE_BASE + "_index.txt";
    InputStream indexStream = getClass().getResourceAsStream(indexPath);
    if (indexStream == null) {
      LOG.warnf("Classpath index not found: %s — serving empty catalog", indexPath);
      return SystemObjectsRegistry.getDefaultInstance();
    }

    List<String> filenames;
    try (indexStream) {
      String indexText = new String(indexStream.readAllBytes(), StandardCharsets.UTF_8);
      filenames = parseIndex(indexText, indexPath);
    } catch (IOException e) {
      LOG.warnf(e, "Failed to read classpath index %s — serving empty catalog", indexPath);
      return SystemObjectsRegistry.getDefaultInstance();
    }

    SystemObjectsRegistry.Builder accumulator = SystemObjectsRegistry.newBuilder();
    TextFormat.Parser parser = TextFormat.Parser.newBuilder().build();

    for (String filename : filenames) {
      String resourcePath = CLASSPATH_RESOURCE_BASE + filename;
      InputStream in = getClass().getResourceAsStream(resourcePath);
      if (in == null) {
        LOG.warnf("Classpath resource not found: %s — skipping", resourcePath);
        continue;
      }
      try (in) {
        String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        mergeFragment(text, resourcePath, parser, accumulator);
      } catch (IOException e) {
        LOG.warnf(e, "Failed to read classpath resource %s — skipping", resourcePath);
      }
    }

    return accumulator.build();
  }

  // ---------------------------------------------------------------------------
  // Filesystem loading (glob *.pbtxt, alphabetical order)
  // ---------------------------------------------------------------------------

  private SystemObjectsRegistry loadFromFilesystem(Path dir) {
    if (!Files.isDirectory(dir)) {
      LOG.warnf(
          "Configured builtins directory does not exist or is not a directory: %s"
              + " — serving empty catalog",
          dir);
      return SystemObjectsRegistry.getDefaultInstance();
    }

    List<Path> files = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.pbtxt")) {
      stream.forEach(files::add);
    } catch (IOException e) {
      LOG.warnf(e, "Failed to list *.pbtxt files in %s — serving empty catalog", dir);
      return SystemObjectsRegistry.getDefaultInstance();
    }

    Collections.sort(files); // natural path order = alphabetical filename order

    if (files.isEmpty()) {
      LOG.debugf("No *.pbtxt files found in %s — serving empty catalog", dir);
      return SystemObjectsRegistry.getDefaultInstance();
    }

    SystemObjectsRegistry.Builder accumulator = SystemObjectsRegistry.newBuilder();
    TextFormat.Parser parser = TextFormat.Parser.newBuilder().build();

    for (Path file : files) {
      try {
        String text = Files.readString(file, StandardCharsets.UTF_8);
        mergeFragment(text, file.toString(), parser, accumulator);
      } catch (IOException e) {
        LOG.warnf(e, "Failed to read pbtxt file %s — skipping", file);
      }
    }

    return accumulator.build();
  }

  // ---------------------------------------------------------------------------
  // Shared fragment-merge helper
  // ---------------------------------------------------------------------------

  private static void mergeFragment(
      String text,
      String source,
      TextFormat.Parser parser,
      SystemObjectsRegistry.Builder accumulator) {
    SystemObjectsRegistry.Builder tmp = SystemObjectsRegistry.newBuilder();
    try {
      // No proto2 extension registry needed: PBtxt files use only core proto fields.
      parser.merge(new StringReader(text), ExtensionRegistry.getEmptyRegistry(), tmp);
    } catch (Exception e) {
      LOG.warnf(e, "Failed to parse pbtxt fragment %s — skipping", source);
      return;
    }
    // engine_kind inside engine_specific blocks is not supported: the catalog's configured
    // engine kind (FLOECAT_EXTENSION_ENGINE_KIND) always applies. Strip any user-supplied values
    // so they cannot accidentally scope rules to the wrong engine.
    stripEngineKinds(tmp);
    SystemObjectsRegistryMerger.append(accumulator, tmp);
  }

  // ---------------------------------------------------------------------------
  // Strip engine_kind from all engine_specific entries (not a supported field)
  // ---------------------------------------------------------------------------

  private static void stripEngineKinds(SystemObjectsRegistry.Builder builder) {
    for (int i = 0; i < builder.getFunctionsCount(); i++) {
      var fn = builder.getFunctions(i);
      builder.setFunctions(
          i,
          fn.toBuilder()
              .clearEngineSpecific()
              .addAllEngineSpecific(dropEngineKind(fn.getEngineSpecificList()))
              .build());
    }
    for (int i = 0; i < builder.getTypesCount(); i++) {
      var t = builder.getTypes(i);
      builder.setTypes(
          i,
          t.toBuilder()
              .clearEngineSpecific()
              .addAllEngineSpecific(dropEngineKind(t.getEngineSpecificList()))
              .build());
    }
    for (int i = 0; i < builder.getOperatorsCount(); i++) {
      var op = builder.getOperators(i);
      builder.setOperators(
          i,
          op.toBuilder()
              .clearEngineSpecific()
              .addAllEngineSpecific(dropEngineKind(op.getEngineSpecificList()))
              .build());
    }
    for (int i = 0; i < builder.getCastsCount(); i++) {
      var c = builder.getCasts(i);
      builder.setCasts(
          i,
          c.toBuilder()
              .clearEngineSpecific()
              .addAllEngineSpecific(dropEngineKind(c.getEngineSpecificList()))
              .build());
    }
    for (int i = 0; i < builder.getCollationsCount(); i++) {
      var col = builder.getCollations(i);
      builder.setCollations(
          i,
          col.toBuilder()
              .clearEngineSpecific()
              .addAllEngineSpecific(dropEngineKind(col.getEngineSpecificList()))
              .build());
    }
    for (int i = 0; i < builder.getAggregatesCount(); i++) {
      var agg = builder.getAggregates(i);
      builder.setAggregates(
          i,
          agg.toBuilder()
              .clearEngineSpecific()
              .addAllEngineSpecific(dropEngineKind(agg.getEngineSpecificList()))
              .build());
    }
    // Registry-level engine_specific entries
    if (!builder.getEngineSpecificList().isEmpty()) {
      builder
          .clearEngineSpecific()
          .addAllEngineSpecific(dropEngineKind(builder.getEngineSpecificList()));
    }
  }

  private static List<EngineSpecific> dropEngineKind(List<EngineSpecific> rules) {
    return rules.stream()
        .map(r -> r.getEngineKind().isEmpty() ? r : r.toBuilder().clearEngineKind().build())
        .toList();
  }

  // ---------------------------------------------------------------------------
  // _index.txt parsing helper
  // ---------------------------------------------------------------------------

  private static List<String> parseIndex(String indexText, String indexPath) {
    List<String> result = new ArrayList<>();
    indexText
        .lines()
        .map(String::trim)
        .filter(line -> !line.isEmpty())
        .filter(line -> !line.startsWith("#"))
        .forEach(
            line -> {
              if (line.startsWith("/")) {
                LOG.warnf(
                    "Index entry must be a relative path, got '%s' in %s — skipping",
                    line, indexPath);
              } else {
                result.add(line);
              }
            });
    return Collections.unmodifiableList(result);
  }

  // ---------------------------------------------------------------------------
  // SystemObjectScannerProvider — no engine-specific scanners in this extension
  // ---------------------------------------------------------------------------

  @Override
  public List<SystemObjectDef> definitions() {
    return List.of();
  }

  @Override
  public boolean supportsEngine(String engineKind) {
    return EngineContextNormalizer.normalizeEngineKind(engineKind)
        .equals(EngineContextNormalizer.normalizeEngineKind(this.engineKind()));
  }

  @Override
  public boolean supports(NameRef name, String engineKind) {
    // definitions() is empty so this extension never provides scanner-backed objects.
    return false;
  }

  @Override
  public Optional<SystemObjectScanner> provide(
      String scannerId, String engineKind, String engineVersion) {
    return Optional.empty();
  }
}
