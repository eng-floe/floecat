package ai.floedb.floecat.telemetry.docgen;

import ai.floedb.floecat.telemetry.MetricDef;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.TelemetryRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class MetricCatalogDocgen {
  static final String SECTION_START = "<!-- METRICS:START -->";
  static final String SECTION_END = "<!-- METRICS:END -->";
  private static final String MARKDOWN_HEADER =
      "# Telemetry Hub Contract\n\n"
          + "Here is a global overview of all metrics defined in the repo right now:";
  private static final String TABLE_HEADER =
      "| Metric | Type | Unit | Required Tags | Allowed Tags |\n"
          + "| --- | --- | --- | --- | --- |";
  static final String DEFAULT_MARKDOWN_TEMPLATE =
      MARKDOWN_HEADER + "\n\n" + SECTION_START + "\n" + TABLE_HEADER + SECTION_END + "\n";
  private static final Path DOC_DIR = determineDocDir();

  static Path docDir() {
    return DOC_DIR;
  }

  private static Path determineDocDir() {
    String override = System.getProperty("telemetry.doc.dir");
    if (override != null && !override.isBlank()) {
      return Path.of(override);
    }
    Path candidate = Path.of("docs", "telemetry");
    if (Files.exists(candidate)) {
      return candidate;
    }
    Path fallback = Path.of("..", "..", "docs", "telemetry");
    if (Files.exists(fallback)) {
      return fallback;
    }
    return candidate;
  }

  public static void main(String[] args) throws IOException {
    Path contractMd = DOC_DIR.resolve("contract.md");
    Path contractJson = DOC_DIR.resolve("contract.json");

    TelemetryRegistry registry = Telemetry.newRegistryWithCore();
    Map<String, MetricDef> catalog = Telemetry.metricCatalog(registry);

    ensureMarkdownFile(contractMd);
    String existingMd = Files.readString(contractMd);
    String updatedMd = injectGeneratedRows(existingMd, buildTableSection(catalog));
    writeIfChanged(contractMd, updatedMd);

    String json = buildJson(catalog);
    writeIfChanged(contractJson, json);
  }

  static String buildTableRows(Map<String, MetricDef> catalog) {
    return catalog.values().stream()
        .sorted(Comparator.comparing(def -> def.id().name()))
        .map(MetricCatalogDocgen::formatTableRow)
        .collect(Collectors.joining("\n"));
  }

  private static String formatTableRow(MetricDef def) {
    return String.format(
        "| %s | %s | %s | %s | %s |",
        escapeMarkdown(def.id().name()),
        escapeMarkdown(def.id().type().toString()),
        escapeMarkdown(def.id().unit()),
        joinTags(def.requiredTags()),
        joinTags(def.allowedTags()));
  }

  private static String joinTags(Set<String> tags) {
    if (tags.isEmpty()) {
      return "";
    }
    return tags.stream().sorted().collect(Collectors.joining(", "));
  }

  static String buildTableSection(Map<String, MetricDef> catalog) {
    String rows = buildTableRows(catalog);
    if (rows.isEmpty()) {
      return TABLE_HEADER + "\n";
    }
    return TABLE_HEADER + "\n" + rows;
  }

  static String injectGeneratedRows(String markdown, String tableSection) {
    int start = markdown.indexOf(SECTION_START);
    int end = markdown.indexOf(SECTION_END);
    if (start == -1 || end == -1 || end < start) {
      throw new IllegalStateException("Markdown contract missing generation markers");
    }
    int contentStart = start + SECTION_START.length();
    String prefix = markdown.substring(0, contentStart);
    String suffix = markdown.substring(end);
    String body = "\n" + tableSection + "\n";
    return prefix + body + suffix;
  }

  static String buildJson(Map<String, MetricDef> catalog) {
    List<MetricDef> ordered =
        catalog.values().stream()
            .sorted(Comparator.comparing(def -> def.id().name()))
            .collect(Collectors.toList());

    StringBuilder sb = new StringBuilder();
    sb.append("[\n");
    for (int i = 0; i < ordered.size(); i++) {
      MetricDef def = ordered.get(i);
      sb.append("  {\n");
      sb.append("    \"name\": \"").append(escapeJson(def.id().name())).append("\",\n");
      sb.append("    \"type\": \"").append(escapeJson(def.id().type().toString())).append("\",\n");
      sb.append("    \"unit\": \"").append(escapeJson(def.id().unit())).append("\",\n");
      sb.append("    \"since\": \"").append(escapeJson(def.id().since())).append("\",\n");
      sb.append("    \"origin\": \"").append(escapeJson(def.id().origin())).append("\",\n");
      sb.append("    \"requiredTags\": ").append(toJsonArray(def.requiredTags())).append(",\n");
      sb.append("    \"allowedTags\": ").append(toJsonArray(def.allowedTags())).append("\n");
      sb.append("  }");
      if (i + 1 < ordered.size()) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append("]\n");
    return sb.toString();
  }

  private static String toJsonArray(Iterable<String> items) {
    String joined =
        StreamSupport.stream(items.spliterator(), false)
            .sorted()
            .map(item -> "\"" + escapeJson(item) + "\"")
            .collect(Collectors.joining(", "));
    return "[" + joined + "]";
  }

  private static String escapeJson(CharSequence value) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '\\' -> sb.append("\\\\");
        case '"' -> sb.append("\\\"");
        case '\b' -> sb.append("\\b");
        case '\f' -> sb.append("\\f");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> {
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    return sb.toString();
  }

  private static String escapeMarkdown(String value) {
    return value.replace("|", "\\|").replace("`", "\\`").replace("\n", "\\n").replace("\r", "\\r");
  }

  private static void ensureMarkdownFile(Path path) {
    if (Files.exists(path)) {
      return;
    }
    try {
      Files.createDirectories(path.getParent());
      Files.writeString(path, DEFAULT_MARKDOWN_TEMPLATE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void writeIfChanged(Path path, String content) {
    try {
      Files.createDirectories(path.getParent());
      if (Files.exists(path) && Files.readString(path).equals(content)) {
        return;
      }
      Files.writeString(path, content);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
