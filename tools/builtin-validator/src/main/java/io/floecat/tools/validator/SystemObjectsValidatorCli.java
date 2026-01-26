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

import ai.floedb.floecat.query.rpc.SystemObjectsRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogProtoMapper;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogValidationFormatter;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogValidator;
import ai.floedb.floecat.systemcatalog.registry.SystemObjectsRegistryMerger;
import ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension;
import ai.floedb.floecat.systemcatalog.util.EngineContextNormalizer;
import ai.floedb.floecat.systemcatalog.validation.Severity;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssueFormatter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Standalone command line validator for builtin catalog protobuf files. The CLI intentionally
 * depends only on the shared builtin catalog module so it can run without the rest of the Floecat
 * runtime.
 */
public final class SystemObjectsValidatorCli {

  private static final String CHECK = "\u2714";

  static {
    ensureSystemProperty("com.google.protobuf.useUnsafe", "false");
    ensureSystemProperty("io.netty.noUnsafe", "true");
    configureJbossLogging();
  }

  private final boolean useColor =
      System.console() != null && !"false".equalsIgnoreCase(System.getenv("NO_COLOR"));
  private final String successPrefix = colored(CHECK, AnsiColor.GREEN) + " ";
  private final String errorPrefix = colored("✖ ERROR:", AnsiColor.RED_BOLD) + " ";

  public static void main(String[] args) {
    int exit = new SystemObjectsValidatorCli().run(args, System.out, System.err);
    System.exit(exit);
  }

  private static void ensureSystemProperty(String key, String value) {
    if (System.getProperty(key) == null) {
      System.setProperty(key, value);
    }
  }

  private static void configureJbossLogging() {
    if (System.getProperty("java.util.logging.manager") == null
        && isClassPresent("org.jboss.logmanager.LogManager")) {
      System.setProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager");
    }
  }

  private static boolean isClassPresent(String name) {
    try {
      Class.forName(name, false, SystemObjectsValidatorCli.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException ignored) {
      return false;
    }
  }

  /** Entry point that is easy to invoke from tests. */
  int run(String[] args, PrintStream out, PrintStream err) {
    CliOptions options;
    try {
      options = CliOptions.parse(args);
    } catch (HelpRequested e) {
      printUsage(out);
      return 0;
    } catch (IllegalArgumentException e) {
      err.println("ERROR: " + e.getMessage());
      err.println();
      printUsage(err);
      return 1;
    }

    SystemCatalogData catalog;
    try {
      if (options.engineKind() != null
          && !options.engineKind().isBlank()
          && options.catalogPath() == null) {
        catalog = loadCatalogFromEngine(options.engineKind());
      } else {
        if (options.catalogPath() == null) {
          throw new IOException("Catalog file path is required unless --engine is provided");
        }
        catalog = loadCatalog(options.catalogPath());
      }
    } catch (IOException e) {
      err.println("ERROR: " + e.getMessage());
      return 1;
    }

    var errors = SystemCatalogValidator.validate(catalog);
    var warnings = List.<String>of();
    List<ValidationIssue> engineIssues = List.of();
    if (options.engineKind() != null && !options.engineKind().isBlank()) {
      try {
        engineIssues = validateWithEngine(options.engineKind(), catalog);
      } catch (RuntimeException e) {
        err.println("ERROR: engine validation failed: " + e.getMessage());
        return 1;
      }
    }
    var stats = CatalogStats.from(catalog);
    int engineErrorCount =
        (int)
            engineIssues.stream().filter(i -> i != null && i.severity() == Severity.ERROR).count();
    int engineWarningCount =
        (int)
            engineIssues.stream()
                .filter(i -> i != null && i.severity() == Severity.WARNING)
                .count();
    int engineInfoCount =
        (int) engineIssues.stream().filter(i -> i != null && i.severity() == Severity.INFO).count();

    boolean valid =
        errors.isEmpty()
            && engineErrorCount == 0
            && (warnings.isEmpty() || !options.strict())
            && (engineWarningCount == 0 || !options.strict());
    boolean shouldFail =
        !errors.isEmpty()
            || engineErrorCount > 0
            || (options.strict() && (!warnings.isEmpty() || engineWarningCount > 0));

    String catalogLabel =
        options.catalogPath() != null
            ? options.catalogPath().getFileName().toString()
            : ("<engine:"
                + EngineContextNormalizer.normalizeEngineKind(options.engineKind())
                + ">");

    if (options.json()) {
      emitJson(out, catalogLabel, stats, errors, warnings, engineIssues, valid);
    } else {
      emitHumanReadable(out, catalogLabel, stats, errors, warnings, engineIssues, valid);
    }

    return shouldFail ? 1 : 0;
  }

  /** Emits the friendly console output requested in the design doc. */
  private void emitHumanReadable(
      PrintStream out,
      String catalogLabel,
      CatalogStats stats,
      List<String> errors,
      List<String> warnings,
      List<ValidationIssue> engineIssues,
      boolean valid) {
    if (valid) {
      out.printf("%sLoaded builtin catalog: %s%n", successPrefix, catalogLabel);
      out.printf("%sTypes: %d OK%n", successPrefix, stats.types());
      out.printf("%sFunctions: %d OK%n", successPrefix, stats.functions());
      out.printf("%sOperators: %d OK%n", successPrefix, stats.operators());
      out.printf("%sCasts: %d OK%n", successPrefix, stats.casts());
      out.printf("%sCollations: %d OK%n", successPrefix, stats.collations());
      out.printf("%sAggregates: %d OK%n", successPrefix, stats.aggregates());
      out.println();
      if (engineIssues != null && !engineIssues.isEmpty()) {
        out.printf("%sEngine validation: %d issues%n", successPrefix, engineIssues.size());
      }
      out.println(colored("ALL CHECKS PASSED.", AnsiColor.GREEN_BOLD));
      return;
    }

    errors.stream()
        .map(SystemCatalogValidationFormatter::describeError)
        .forEach(err -> out.println(errorPrefix + colored(err, AnsiColor.RED_BOLD)));
    if (engineIssues != null && !engineIssues.isEmpty()) {
      for (ValidationIssue issue : engineIssues) {
        if (issue == null) {
          continue;
        }
        String rendered = ValidationIssueFormatter.format(issue);
        if (issue.severity() == Severity.ERROR) {
          out.println(errorPrefix + colored(rendered, AnsiColor.RED_BOLD));
        } else if (issue.severity() == Severity.WARNING) {
          out.println(colored("WARN: " + rendered, AnsiColor.YELLOW));
        } else {
          out.println("INFO: " + rendered);
        }
      }
    }
    warnings.stream()
        .map(SystemObjectsValidatorCli::describeWarning)
        .forEach(warning -> out.println(colored("WARN: " + warning, AnsiColor.YELLOW)));
    out.println();
    StringBuilder summary = new StringBuilder();
    summary.append("VALIDATION FAILED (");
    summary.append(errors.size()).append(" core errors");
    if (!warnings.isEmpty()) {
      summary.append(" + ").append(warnings.size()).append(" core warnings");
    }
    EngineIssueCounts counts = tallyEngineIssues(engineIssues);
    if (counts.errorCount > 0) {
      summary.append(" + ").append(counts.errorCount).append(" engine errors");
    }
    if (counts.warningCount > 0) {
      summary.append(" + ").append(counts.warningCount).append(" engine warnings");
    }
    summary.append(")");
    out.println(colored(summary.toString(), AnsiColor.RED_BOLD));
  }

  /** Emits machine readable JSON output for automation. */
  private void emitJson(
      PrintStream out,
      String catalogLabel,
      CatalogStats stats,
      List<String> errors,
      List<String> warnings,
      List<ValidationIssue> engineIssues,
      boolean valid) {
    EngineIssueCounts counts = tallyEngineIssues(engineIssues);
    var builder = new StringBuilder();
    builder.append("{\n");
    builder.append("  \"catalog\": \"").append(escapeJson(catalogLabel)).append("\",\n");
    builder.append("  \"valid\": ").append(valid).append(",\n");
    builder
        .append("  \"errors\": ")
        .append(asJsonArray(errors, SystemCatalogValidationFormatter::describeError))
        .append(",\n");
    builder
        .append("  \"warnings\": ")
        .append(asJsonArray(warnings, SystemObjectsValidatorCli::describeWarning))
        .append(",\n");
    builder
        .append("  \"engine_issues\": ")
        .append(asJsonArrayEngineIssues(engineIssues))
        .append(",\n");
    builder.append("  \"engine_error_count\": ").append(counts.errorCount).append(",\n");
    builder.append("  \"engine_warning_count\": ").append(counts.warningCount).append(",\n");
    builder.append("  \"engine_info_count\": ").append(counts.infoCount).append(",\n");
    int engineErrorCount =
        (int)
            engineIssues.stream().filter(i -> i != null && i.severity() == Severity.ERROR).count();
    int engineWarningCount =
        (int)
            engineIssues.stream()
                .filter(i -> i != null && i.severity() == Severity.WARNING)
                .count();
    int engineInfoCount =
        (int) engineIssues.stream().filter(i -> i != null && i.severity() == Severity.INFO).count();
    builder.append("  \"engine_error_count\": ").append(engineErrorCount).append(",\n");
    builder.append("  \"engine_warning_count\": ").append(engineWarningCount).append(",\n");
    builder.append("  \"engine_info_count\": ").append(engineInfoCount).append(",\n");
    builder.append("  \"stats\": {\n");
    builder.append("    \"types\": ").append(stats.types()).append(",\n");
    builder.append("    \"functions\": ").append(stats.functions()).append(",\n");
    builder.append("    \"operators\": ").append(stats.operators()).append(",\n");
    builder.append("    \"casts\": ").append(stats.casts()).append(",\n");
    builder.append("    \"collations\": ").append(stats.collations()).append(",\n");
    builder.append("    \"aggregates\": ").append(stats.aggregates()).append("\n");
    builder.append("  }\n");
    builder.append("}\n");
    out.print(builder);
  }

  private static String asJsonArray(
      List<String> values, java.util.function.Function<String, String> mapper) {
    var mapped = new ArrayList<String>(values.size());
    for (String value : values) {
      mapped.add("\"" + escapeJson(mapper.apply(value)) + "\"");
    }
    return "[" + String.join(", ", mapped) + "]";
  }

  private static String asJsonArrayEngineIssues(List<ValidationIssue> issues) {
    if (issues == null || issues.isEmpty()) {
      return "[]";
    }
    var mapped = new ArrayList<String>(issues.size());
    for (ValidationIssue issue : issues) {
      if (issue == null) {
        continue;
      }
      mapped.add("\"" + escapeJson(ValidationIssueFormatter.format(issue)) + "\"");
    }
    return "[" + String.join(", ", mapped) + "]";
  }

  private static EngineIssueCounts tallyEngineIssues(List<ValidationIssue> issues) {
    if (issues == null || issues.isEmpty()) {
      return new EngineIssueCounts(0, 0, 0);
    }
    int errors = 0;
    int warnings = 0;
    int infos = 0;
    for (ValidationIssue issue : issues) {
      if (issue == null) {
        continue;
      }
      switch (issue.severity()) {
        case ERROR -> errors++;
        case WARNING -> warnings++;
        case INFO -> infos++;
      }
    }
    return new EngineIssueCounts(errors, warnings, infos);
  }

  private record EngineIssueCounts(int errorCount, int warningCount, int infoCount) {}

  private static String escapeJson(String value) {
    if (value == null) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    for (char c : value.toCharArray()) {
      switch (c) {
        case '\\' -> builder.append("\\\\");
        case '"' -> builder.append("\\\"");
        case '\n' -> builder.append("\\n");
        case '\r' -> builder.append("\\r");
        case '\t' -> builder.append("\\t");
        default -> builder.append(c);
      }
    }
    return builder.toString();
  }

  private static String describeError(String code) {
    return SystemCatalogValidationFormatter.describeError(code);
  }

  private static String describeWarning(String code) {
    return SystemCatalogValidationFormatter.describeWarning(code);
  }

  /** Loads either a catalog directory or a single protobuf file. */
  private SystemCatalogData loadCatalog(Path path) throws IOException {
    Path normalized = path.toAbsolutePath().normalize();
    if (Files.isDirectory(normalized)) {
      return loadCatalogFromIndex(normalized.resolve("_index.txt"));
    }
    if ("_index.txt".equals(normalized.getFileName().toString())) {
      return loadCatalogFromIndex(normalized);
    }

    if (!Files.exists(normalized)) {
      throw new IOException("Catalog file does not exist: " + normalized);
    }

    String engineKind = inferEngineKind(normalized);

    try {
      byte[] bytes = Files.readAllBytes(normalized);
      return SystemCatalogProtoMapper.fromProto(SystemObjectsRegistry.parseFrom(bytes), engineKind);
    } catch (InvalidProtocolBufferException binaryParseFailure) {
      var builder = SystemObjectsRegistry.newBuilder();
      try (var reader = Files.newBufferedReader(normalized, StandardCharsets.UTF_8)) {
        TextFormat.getParser().merge(reader, builder);
        return SystemCatalogProtoMapper.fromProto(builder.build(), engineKind);
      } catch (TextFormat.ParseException textFailure) {
        throw new IOException(
            "Catalog file is not valid protobuf (binary or text): " + normalized, textFailure);
      }
    }
  }

  private SystemCatalogData loadCatalogFromIndex(Path indexPath) throws IOException {
    Path dir = indexPath.getParent();
    if (dir == null) {
      throw new IOException("Catalog index must live inside a directory: " + indexPath);
    }
    if (!Files.exists(indexPath)) {
      throw new IOException("Catalog index does not exist: " + indexPath);
    }

    List<String> entries = readIndexEntries(indexPath);
    if (entries.isEmpty()) {
      throw new IOException("Catalog index contains no fragments: " + indexPath);
    }

    SystemObjectsRegistry.Builder builder = SystemObjectsRegistry.newBuilder();
    TextFormat.Parser parser = TextFormat.Parser.newBuilder().build();

    for (String entry : entries) {
      if (entry.startsWith("/") || entry.startsWith("\\") || entry.contains("..")) {
        throw new IOException("Invalid catalog fragment '" + entry + "' in index " + indexPath);
      }

      Path fragmentPath = dir.resolve(entry).normalize();
      if (!fragmentPath.startsWith(dir)) {
        throw new IOException("Catalog fragment escapes directory: " + fragmentPath);
      }
      if (!Files.exists(fragmentPath)) {
        throw new IOException("Catalog fragment does not exist: " + fragmentPath);
      }

      SystemObjectsRegistry.Builder fragmentBuilder = SystemObjectsRegistry.newBuilder();
      try (var reader = Files.newBufferedReader(fragmentPath, StandardCharsets.UTF_8)) {
        parser.merge(reader, fragmentBuilder);
      } catch (TextFormat.ParseException parseFailure) {
        throw new IOException("Failed to parse catalog fragment: " + fragmentPath, parseFailure);
      }

      SystemObjectsRegistryMerger.append(builder, fragmentBuilder.build());
    }

    return SystemCatalogProtoMapper.fromProto(builder.build(), inferEngineKindFromDirectory(dir));
  }

  private static List<String> readIndexEntries(Path indexPath) throws IOException {
    List<String> entries = new ArrayList<>();
    for (String line : Files.readAllLines(indexPath, StandardCharsets.UTF_8)) {
      String trimmed = line.trim();
      if (trimmed.isEmpty() || trimmed.startsWith("#")) {
        continue;
      }
      entries.add(trimmed);
    }
    return entries;
  }

  private static String inferEngineKindFromDirectory(Path directory) {
    Path name = directory.getFileName();
    return name != null ? name.toString() : directory.toString();
  }

  private static String inferEngineKind(Path path) {
    String name = path.getFileName().toString();
    String base = name;
    if (base.endsWith(".pbtxt")) {
      base = base.substring(0, base.length() - ".pbtxt".length());
    } else if (base.endsWith(".pb")) {
      base = base.substring(0, base.length() - ".pb".length());
    }
    return base;
  }

  /** Prints the expected CLI arguments. */
  private void printUsage(PrintStream out) {
    out.println(
        "Usage:\n"
            + "  java -jar builtin-validator.jar <catalog.pb|pbtxt|dir|_index.txt> [--strict]"
            + " [--json]\n"
            + "  java -jar builtin-validator.jar --engine <engineKind> [--strict] [--json]\n"
            + "  java -jar builtin-validator.jar <catalog...> --engine <engineKind> [--strict]"
            + " [--json]\n"
            + "\n"
            + "Notes:\n"
            + "  --engine loads the EngineSystemCatalogExtension via ServiceLoader.\n"
            + "  The extension implementation must be on the classpath.");
  }

  private static SystemCatalogData loadCatalogFromEngine(String engineKind) throws IOException {
    String normalized = EngineContextNormalizer.normalizeEngineKind(engineKind);
    if (normalized.isBlank()) {
      throw new IOException("Engine kind is blank");
    }
    EngineSystemCatalogExtension ext = findEngineExtension(normalized);
    if (ext == null) {
      throw new IOException("No EngineSystemCatalogExtension found for engine_kind=" + normalized);
    }
    try {
      return ext.loadSystemCatalog();
    } catch (RuntimeException e) {
      throw new IOException("Failed to load catalog from engine extension: " + normalized, e);
    }
  }

  private static List<ValidationIssue> validateWithEngine(
      String engineKind, SystemCatalogData catalog) {
    String normalized = EngineContextNormalizer.normalizeEngineKind(engineKind);
    if (normalized.isBlank()) {
      return List.of();
    }
    EngineSystemCatalogExtension ext = findEngineExtension(normalized);
    if (ext == null) {
      return List.of(
          new ValidationIssue(
              "cli.engine_extension.not_found",
              Severity.ERROR,
              "engine_kind=" + normalized,
              null,
              List.of()));
    }
    return ext.validate(catalog);
  }

  private static EngineSystemCatalogExtension findEngineExtension(String normalizedEngineKind) {
    for (EngineSystemCatalogExtension ext :
        ServiceLoader.load(EngineSystemCatalogExtension.class)) {
      String extKind = EngineContextNormalizer.normalizeEngineKind(ext.engineKind());
      if (normalizedEngineKind.equals(extKind)) {
        return ext;
      }
    }
    return null;
  }

  private String colored(String text, AnsiColor color) {
    if (!useColor || color == AnsiColor.NONE) {
      return text;
    }
    return color.code + text + AnsiColor.RESET.code;
  }

  /** Captures simple cardinality metrics for reporting and JSON output. */
  private record CatalogStats(
      int types, int functions, int operators, int casts, int collations, int aggregates) {
    static CatalogStats from(SystemCatalogData data) {
      return new CatalogStats(
          data.types().size(),
          data.functions().size(),
          data.operators().size(),
          data.casts().size(),
          data.collations().size(),
          data.aggregates().size());
    }
  }

  /** Parsed CLI flags shared between the entry point and tests. */
  private record CliOptions(Path catalogPath, String engineKind, boolean strict, boolean json) {
    static CliOptions parse(String[] args) {
      if (args == null || args.length == 0) {
        throw new IllegalArgumentException("Catalog file path or --engine is required");
      }
      Path path = null;
      String engine = null;
      boolean strict = false;
      boolean json = false;

      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        if (arg == null || arg.isBlank()) {
          continue;
        }
        String value = arg.trim();
        if (value.equals("--strict")) {
          strict = true;
        } else if (value.equals("--json")) {
          json = true;
        } else if (value.equals("--engine")) {
          if (i + 1 >= args.length) {
            throw new IllegalArgumentException("--engine requires a value");
          }
          engine = args[++i];
          if (engine == null) {
            engine = "";
          }
          engine = engine.trim();
        } else if (value.startsWith("--engine=")) {
          engine = value.substring("--engine=".length()).trim();
        } else if (value.equals("--help") || value.equals("-h")) {
          throw new HelpRequested();
        } else if (value.startsWith("--")) {
          throw new IllegalArgumentException("Unknown option: " + value);
        } else if (path == null) {
          path = Path.of(value);
        } else {
          throw new IllegalArgumentException("Unexpected argument: " + value);
        }
      }

      if (path == null && (engine == null || engine.isBlank())) {
        throw new IllegalArgumentException(
            "Catalog file path is required unless --engine is provided");
      }
      return new CliOptions(path, engine, strict, json);
    }
  }

  /** Internal signal used to distinguish --help from regular argument errors. */
  private static final class HelpRequested extends RuntimeException {}

  private enum AnsiColor {
    NONE(""),
    RESET("\u001B[0m"),
    GREEN("\u001B[32m"),
    GREEN_BOLD("\u001B[1;32m"),
    RED("\u001B[31m"),
    RED_BOLD("\u001B[1;31m"),
    YELLOW("\u001B[33m");

    private final String code;

    AnsiColor(String code) {
      this.code = code;
    }
  }
}
