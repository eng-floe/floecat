package io.floecat.tools.validator;

import ai.floedb.floecat.catalog.builtin.BuiltinCatalogData;
import ai.floedb.floecat.catalog.builtin.BuiltinCatalogProtoMapper;
import ai.floedb.floecat.catalog.builtin.BuiltinCatalogValidator;
import ai.floedb.floecat.query.rpc.BuiltinRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Standalone command line validator for builtin catalog protobuf files. The CLI intentionally
 * depends only on the shared builtin catalog module so it can run without the rest of the Floecat
 * runtime.
 */
public final class BuiltinCatalogValidatorCli {

  private static final String CHECK = "\u2714";
  private static final Map<String, String> CONTEXT_LABELS =
      Map.ofEntries(
          Map.entry("function.return", "Function return type"),
          Map.entry("function.arg", "Function argument"),
          Map.entry("operator.left", "Operator left operand"),
          Map.entry("operator.right", "Operator right operand"),
          Map.entry("cast.source", "Cast source type"),
          Map.entry("cast.target", "Cast target type"),
          Map.entry("agg.state", "Aggregate state type"),
          Map.entry("agg.return", "Aggregate return type"),
          Map.entry("agg.arg", "Aggregate argument"),
          Map.entry("agg.stateFn", "Aggregate state function"),
          Map.entry("agg.finalFn", "Aggregate final function"));

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
    int exit = new BuiltinCatalogValidatorCli().run(args, System.out, System.err);
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
      Class.forName(name, false, BuiltinCatalogValidatorCli.class.getClassLoader());
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

    BuiltinCatalogData catalog;
    try {
      catalog = loadCatalog(options.catalogPath());
    } catch (IOException e) {
      err.println("ERROR: " + e.getMessage());
      return 1;
    }

    var errors = BuiltinCatalogValidator.validate(catalog);
    var warnings = List.<String>of();
    var stats = CatalogStats.from(catalog);
    boolean valid = errors.isEmpty() && (warnings.isEmpty() || !options.strict());
    boolean shouldFail = !errors.isEmpty() || (options.strict() && !warnings.isEmpty());

    String catalogLabel = options.catalogPath().getFileName().toString();

    if (options.json()) {
      emitJson(out, catalogLabel, stats, errors, warnings, valid);
    } else {
      emitHumanReadable(out, catalogLabel, stats, errors, warnings, valid);
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
      out.println(colored("ALL CHECKS PASSED.", AnsiColor.GREEN_BOLD));
      return;
    }

    errors.stream()
        .map(BuiltinCatalogValidatorCli::describeError)
        .forEach(err -> out.println(errorPrefix + colored(err, AnsiColor.RED_BOLD)));
    warnings.stream()
        .map(BuiltinCatalogValidatorCli::describeWarning)
        .forEach(warning -> out.println(colored("WARN: " + warning, AnsiColor.YELLOW)));
    out.println();
    out.printf(
        "%s%n",
        colored(
            "VALIDATION FAILED ("
                + errors.size()
                + " errors"
                + (warnings.isEmpty() ? "" : " + " + warnings.size() + " warnings")
                + ")",
            AnsiColor.RED_BOLD));
  }

  /** Emits machine readable JSON output for automation. */
  private void emitJson(
      PrintStream out,
      String catalogLabel,
      CatalogStats stats,
      List<String> errors,
      List<String> warnings,
      boolean valid) {
    var builder = new StringBuilder();
    builder.append("{\n");
    builder.append("  \"catalog\": \"").append(escapeJson(catalogLabel)).append("\",\n");
    builder.append("  \"valid\": ").append(valid).append(",\n");
    builder
        .append("  \"errors\": ")
        .append(asJsonArray(errors, BuiltinCatalogValidatorCli::describeError))
        .append(",\n");
    builder
        .append("  \"warnings\": ")
        .append(asJsonArray(warnings, BuiltinCatalogValidatorCli::describeWarning))
        .append(",\n");
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

  /** Converts validator error codes into human friendly sentences. */
  private static String describeError(String code) {
    Objects.requireNonNull(code, "code");
    if (code.equals("catalog.null")) {
      return "Catalog payload is null";
    }
    if (code.equals("types.empty")) {
      return "Catalog defines no types";
    }
    if (code.equals("functions.empty")) {
      return "Catalog defines no functions";
    }
    if (code.equals("type.name.required")) {
      return "Type name is required";
    }
    if (code.startsWith("type.duplicate:")) {
      return "Duplicate type '" + code.substring(code.indexOf(':') + 1) + "'";
    }
    if (code.equals("function.name.required")) {
      return "Function name is required";
    }
    if (code.startsWith("function.duplicate:")) {
      return "Duplicate function '" + code.substring(code.indexOf(':') + 1) + "'";
    }
    if (code.startsWith("operator.function.missing:")) {
      return "Operator references unknown function '" + code.substring(code.indexOf(':') + 1) + "'";
    }
    if (code.startsWith("collation.duplicate:")) {
      return "Duplicate collation '" + code.substring(code.indexOf(':') + 1) + "'";
    }
    if (code.equals("collation.name.required")) {
      return "Collation name is required";
    }
    if (code.startsWith("cast.duplicate:")) {
      return "Duplicate cast mapping '" + code.substring(code.indexOf(':') + 1) + "'";
    }
    if (code.contains(".type.required")) {
      return contextLabel(code) + " must reference a type";
    }
    if (code.contains(".type.unknown:")) {
      int idx = code.lastIndexOf(':');
      return contextLabel(code) + " references unknown type '" + code.substring(idx + 1) + "'";
    }
    if (code.contains(".function.required")) {
      return contextLabel(code) + " must reference a function";
    }
    if (code.contains(".function.unknown:")) {
      int idx = code.lastIndexOf(':');
      return contextLabel(code) + " references unknown function '" + code.substring(idx + 1) + "'";
    }
    return code;
  }

  /** Maps the validator namespaces to readable labels. */
  private static String contextLabel(String code) {
    String prefix = code;
    int idx = code.indexOf(':');
    if (idx > 0) {
      prefix = code.substring(0, idx);
    }
    return CONTEXT_LABELS.getOrDefault(prefix, prefix);
  }

  /** Placeholder for when warnings are added to the validator. */
  private static String describeWarning(String code) {
    return code;
  }

  /** Loads either a binary or text protobuf from disk and maps it to our data record. */
  private BuiltinCatalogData loadCatalog(Path path) throws IOException {
    if (!Files.exists(path)) {
      throw new IOException("Catalog file does not exist: " + path);
    }

    String engineKind = inferEngineKind(path);

    try {
      byte[] bytes = Files.readAllBytes(path);
      return BuiltinCatalogProtoMapper.fromProto(BuiltinRegistry.parseFrom(bytes), engineKind);
    } catch (InvalidProtocolBufferException binaryParseFailure) {
      var builder = BuiltinRegistry.newBuilder();
      try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
        TextFormat.getParser().merge(reader, builder);
        return BuiltinCatalogProtoMapper.fromProto(builder.build(), engineKind);
      } catch (TextFormat.ParseException textFailure) {
        throw new IOException(
            "Catalog file is not valid protobuf (binary or text): " + path, textFailure);
      }
    }
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
    out.println("Usage: java -jar builtin-validator.jar <catalog.pb|pbtxt> [--strict] [--json]");
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
    static CatalogStats from(BuiltinCatalogData data) {
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
  private record CliOptions(Path catalogPath, boolean strict, boolean json) {
    static CliOptions parse(String[] args) {
      if (args == null || args.length == 0) {
        throw new IllegalArgumentException("Catalog file path is required");
      }
      Path path = null;
      boolean strict = false;
      boolean json = false;

      for (String arg : args) {
        if (arg == null || arg.isBlank()) {
          continue;
        }
        String value = arg.trim();
        if (value.equals("--strict")) {
          strict = true;
        } else if (value.equals("--json")) {
          json = true;
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

      if (path == null) {
        throw new IllegalArgumentException("Catalog file path is required");
      }
      return new CliOptions(path, strict, json);
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
