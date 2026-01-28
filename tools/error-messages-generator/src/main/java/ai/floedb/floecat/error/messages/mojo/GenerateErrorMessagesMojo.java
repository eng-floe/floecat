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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/** Validates the error message bundles and emits a strongly-typed registry. */
@Mojo(name = "generate", defaultPhase = LifecyclePhase.GENERATE_SOURCES, threadSafe = true)
public class GenerateErrorMessagesMojo extends AbstractMojo {
  private static final String CANONICAL_BUNDLE_NAME = "errors_en.properties";
  private static final Pattern BUNDLE_GLOB = Pattern.compile("errors_.*\\.properties");
  private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{([^}]+)}");
  private static final Pattern SUFFIX_PATTERN =
      Pattern.compile("^[a-z][a-z0-9]*(?:\\.[a-z][a-z0-9]*)*$");
  private static final String GENERATED_PACKAGE = "ai.floedb.floecat.service.error.impl";
  private static final String GENERATED_CLASS_NAME = "GeneratedErrorMessages";

  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  @Parameter(
      property = "errorMessages.resourcesDirectory",
      defaultValue = "${project.basedir}/src/main/resources")
  private File resourcesDirectory;

  @Parameter(
      property = "errorMessages.generatedSourcesDirectory",
      defaultValue = "${project.build.directory}/generated-sources/error-messages")
  private File generatedSourcesDirectory;

  @Parameter(
      property = "errorMessages.errorCodeProto",
      defaultValue =
          "${project.parent.basedir}/core/proto/src/main/proto/floecat/common/common.proto")
  private File errorCodeProto;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    Path resourcesRoot = resourcesDirectory.toPath();
    if (!Files.isDirectory(resourcesRoot)) {
      throw new MojoFailureException("Resources directory " + resourcesRoot + " does not exist");
    }

    Path canonicalBundle = resourcesRoot.resolve(CANONICAL_BUNDLE_NAME);
    if (!Files.isRegularFile(canonicalBundle)) {
      throw new MojoFailureException(
          "Canonical error bundle " + CANONICAL_BUNDLE_NAME + " is missing from " + resourcesRoot);
    }

    Map<String, String> canonicalEntries = loadBundle(canonicalBundle);
    Map<String, Set<String>> canonicalPlaceholders =
        canonicalEntries.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> extractPlaceholders(entry.getValue()),
                    (existing, replacement) -> existing,
                    TreeMap::new));

    validateBundles(resourcesRoot, canonicalEntries, canonicalPlaceholders, canonicalBundle);

    Set<String> allowedErrorCodes = loadAllowedErrorCodes(errorCodeProto.toPath());
    List<MessageDefinition> definitions =
        buildMessageDefinitions(canonicalEntries, canonicalPlaceholders, allowedErrorCodes);

    List<GeneratedEntry> generatedEntries = resolveConstantNames(definitions);

    writeGeneratedFile(generatedEntries);

    project.addCompileSourceRoot(generatedSourcesDirectory.getAbsolutePath());
  }

  private Map<String, String> loadBundle(Path bundle) throws MojoExecutionException {
    Properties properties = new Properties();
    try (var reader = Files.newBufferedReader(bundle, StandardCharsets.ISO_8859_1)) {
      properties.load(reader);
    } catch (IOException e) {
      throw new MojoExecutionException("Unable to load " + bundle, e);
    }
    Map<String, String> entries = new TreeMap<>();
    for (String key : properties.stringPropertyNames()) {
      entries.put(key, properties.getProperty(key));
    }
    return entries;
  }

  private void validateBundles(
      Path resourcesRoot,
      Map<String, String> canonicalEntries,
      Map<String, Set<String>> canonicalPlaceholders,
      Path canonicalBundle)
      throws MojoExecutionException, MojoFailureException {
    List<Path> bundles;
    try (Stream<Path> stream = Files.list(resourcesRoot)) {
      bundles =
          stream
              .filter(Files::isRegularFile)
              .filter(path -> BUNDLE_GLOB.matcher(path.getFileName().toString()).matches())
              .collect(Collectors.toList());
    } catch (IOException e) {
      throw new MojoExecutionException("Failed to list resources in " + resourcesRoot, e);
    }

    for (Path bundle : bundles) {
      if (bundle.equals(canonicalBundle)) {
        continue;
      }
      Map<String, String> localeEntries = loadBundle(bundle);

      TreeSet<String> missing = new TreeSet<>(canonicalEntries.keySet());
      missing.removeAll(localeEntries.keySet());

      TreeSet<String> extra = new TreeSet<>(localeEntries.keySet());
      extra.removeAll(canonicalEntries.keySet());

      if (!missing.isEmpty() || !extra.isEmpty()) {
        throw new MojoFailureException(describeParityDiff(bundle, missing, extra, canonicalBundle));
      }

      for (Map.Entry<String, Set<String>> canonical : canonicalPlaceholders.entrySet()) {
        Set<String> expected = canonical.getValue();
        Set<String> actual = extractPlaceholders(localeEntries.get(canonical.getKey()));
        if (!expected.equals(actual)) {
          throw new MojoFailureException(
              "Placeholder mismatch for key '"
                  + canonical.getKey()
                  + "' in bundle "
                  + bundle
                  + "; expected "
                  + expected
                  + " but found "
                  + actual);
        }
      }
    }
  }

  private String describeParityDiff(
      Path bundle, Set<String> missing, Set<String> extra, Path canonicalBundle) {
    StringBuilder builder = new StringBuilder();
    builder
        .append("Bundle ")
        .append(bundle.getFileName())
        .append(" differs from ")
        .append(canonicalBundle.getFileName())
        .append(":");
    if (!missing.isEmpty()) {
      builder.append("\n  Missing keys:");
      for (String key : missing) {
        builder.append("\n    - ").append(key);
      }
    }
    if (!extra.isEmpty()) {
      builder.append("\n  Extra keys:");
      for (String key : extra) {
        builder.append("\n    - ").append(key);
      }
    }
    return builder.toString();
  }

  private Set<String> extractPlaceholders(String template) {
    if (template == null) {
      return Collections.emptySet();
    }
    Matcher matcher = PLACEHOLDER_PATTERN.matcher(template);
    Set<String> placeholders = new LinkedHashSet<>();
    while (matcher.find()) {
      placeholders.add(matcher.group(1));
    }
    return Collections.unmodifiableSet(placeholders);
  }

  private List<MessageDefinition> buildMessageDefinitions(
      Map<String, String> canonicalEntries,
      Map<String, Set<String>> canonicalPlaceholders,
      Set<String> allowedErrorCodes)
      throws MojoFailureException {
    List<MessageDefinition> definitions = new ArrayList<>();
    for (Map.Entry<String, String> entry : canonicalEntries.entrySet()) {
      String key = entry.getKey();
      int dot = key.indexOf('.');
      if (dot < 0) {
        // Ignore keys that aren't in the ErrorCode.suffix format.
        continue;
      }

      String prefix = key.substring(0, dot);
      String suffix = key.substring(dot + 1);

      if (suffix.isBlank()) {
        throw new MojoFailureException("Message key '" + key + "' must include a suffix");
      }

      // Enforce canonical suffix style.
      // This prevents subtle duplicates like: query.table_not_pinned vs query.table.not_pinned
      if (suffix.indexOf('_') >= 0) {
        throw new MojoFailureException(
            "Invalid message suffix '"
                + suffix
                + "' for key '"
                + key
                + "'. Use dot-separated segments (e.g. query.table.not.pinned), not underscores.");
      }

      if (!SUFFIX_PATTERN.matcher(suffix).matches()) {
        throw new MojoFailureException(
            "Invalid message suffix '"
                + suffix
                + "' for key '"
                + key
                + "'. Suffixes must be lowercase, dot-separated segments (no leading/trailing dots,"
                + " no consecutive dots, and each segment must start with a letter).");
      }

      if (!allowedErrorCodes.contains(prefix)) {
        throw new MojoFailureException(
            "Unknown error code prefix '"
                + prefix
                + "' (key="
                + key
                + "); expected one of "
                + allowedErrorCodes);
      }

      Set<String> placeholders = canonicalPlaceholders.getOrDefault(key, Collections.emptySet());
      definitions.add(new MessageDefinition(key, suffix, prefix, placeholders));
    }

    ensureSuffixUniqueness(definitions);
    definitions.sort(Comparator.comparing(MessageDefinition::fullKey));
    return definitions;
  }

  private void ensureSuffixUniqueness(List<MessageDefinition> definitions)
      throws MojoFailureException {
    Map<String, MessageDefinition> seen = new LinkedHashMap<>();
    for (MessageDefinition definition : definitions) {
      MessageDefinition existing = seen.putIfAbsent(definition.suffix(), definition);
      if (existing != null) {
        throw new MojoFailureException(
            "Duplicate suffix '"
                + definition.suffix()
                + "' for keys '"
                + existing.fullKey()
                + "' and '"
                + definition.fullKey()
                + "'");
      }
    }
  }

  private List<GeneratedEntry> resolveConstantNames(List<MessageDefinition> definitions)
      throws MojoFailureException {
    Map<String, MessageDefinition> byConstant = new LinkedHashMap<>();

    for (MessageDefinition definition : definitions) {
      String constantName = normalizeConstantName(definition.suffix());

      // Fail fast instead of generating hashed constants.
      // This keeps enum names stable and avoids touching service code.
      MessageDefinition existing = byConstant.putIfAbsent(constantName, definition);
      if (existing != null) {
        throw new MojoFailureException(
            "Two message suffixes normalize to the same enum constant '"
                + constantName
                + "':\n"
                + " - "
                + existing.fullKey()
                + "\n"
                + " - "
                + definition.fullKey()
                + "\n"
                + "Rename one suffix to avoid a naming collision.");
      }
    }

    return byConstant.entrySet().stream()
        .map(entry -> new GeneratedEntry(entry.getKey(), entry.getValue()))
        .sorted(Comparator.comparing(GeneratedEntry::constantName))
        .collect(Collectors.toList());
  }

  static String normalizeConstantName(String suffix) {
    String normalized = suffix.replaceAll("[^A-Za-z0-9]+", "_");
    normalized = normalized.replaceAll("_+", "_");
    normalized = normalized.replaceAll("^_+", "");
    normalized = normalized.replaceAll("_+$", "");
    normalized = normalized.toUpperCase();
    if (normalized.isEmpty()) {
      normalized = "MESSAGE";
    }
    return normalized;
  }

  static String shortHash(String input) {
    CRC32 crc = new CRC32();
    crc.update(input.getBytes(StandardCharsets.UTF_8));
    return String.format("%06X", crc.getValue() & 0xFFFFFF);
  }

  private void writeGeneratedFile(List<GeneratedEntry> entries) throws MojoExecutionException {
    Path sourceRoot =
        generatedSourcesDirectory.toPath().resolve(GENERATED_PACKAGE.replace('.', '/'));
    try {
      Files.createDirectories(sourceRoot);
    } catch (IOException e) {
      throw new MojoExecutionException("Unable to create " + sourceRoot, e);
    }

    Path target = sourceRoot.resolve(GENERATED_CLASS_NAME + ".java");
    try (BufferedWriter writer = Files.newBufferedWriter(target, StandardCharsets.UTF_8)) {
      writer.write("/*\n");
      writer.write(" * Copyright 2026 Yellowbrick Data, Inc.\n");
      writer.write(" *\n");
      writer.write(" * Licensed under the Apache License, Version 2.0 (the \"License\");\n");
      writer.write(" * you may not use this file except in compliance with the License.\n");
      writer.write(" * You may obtain a copy of the License at\n");
      writer.write(" *\n");
      writer.write(" *     http://www.apache.org/licenses/LICENSE-2.0\n");
      writer.write(" *\n");
      writer.write(" * Unless required by applicable law or agreed to in writing, software\n");
      writer.write(" * distributed under the License is distributed on an \"AS IS\" BASIS,\n");
      writer.write(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n");
      writer.write(" * See the License for the specific language governing permissions and\n");
      writer.write(" * limitations under the License.\n");
      writer.write(" */\n\n");

      writer.write("package " + GENERATED_PACKAGE + ";\n\n");
      writer.write("import ai.floedb.floecat.common.rpc.ErrorCode;\n");
      writer.write("import java.util.Arrays;\n");
      writer.write("import java.util.Collections;\n");
      writer.write("import java.util.Map;\n");
      writer.write("import java.util.Objects;\n");
      writer.write("import java.util.Set;\n");
      writer.write("import java.util.function.Function;\n");
      writer.write("import java.util.stream.Collectors;\n");
      writer.write("import javax.annotation.processing.Generated;\n\n");

      writer.write(
          "@Generated(\"ai.floedb.floecat.error.messages.mojo.GenerateErrorMessagesMojo\")\n");
      writer.write("public final class " + GENERATED_CLASS_NAME + " {\n");
      writer.write("  private " + GENERATED_CLASS_NAME + "() {}\n\n");

      writer.write("  @SuppressWarnings(\"unused\")\n");
      writer.write("  public enum MessageKey {\n");

      if (entries.isEmpty()) {
        writer.write("    ;\n");
      } else {
        for (int i = 0; i < entries.size(); i++) {
          GeneratedEntry entry = entries.get(i);
          writer.write("    ");
          writer.write(entry.constantName());
          writer.write("(ErrorCode.");
          writer.write(entry.definition().errorCodeName());
          writer.write(", \"");
          writer.write(entry.definition().suffix());
          writer.write("\", ");
          writer.write(renderPlaceholderLiteral(entry.definition().placeholders()));
          writer.write(")");
          writer.write(i == entries.size() - 1 ? ";" : ",");
          writer.write("\n");
        }
      }

      writer.write("\n");
      writer.write("    private final ErrorCode errorCode;\n");
      writer.write("    private final String suffix;\n");
      writer.write("    private final String fullKey;\n");
      writer.write("    private final Set<String> placeholders;\n\n");

      writer.write(
          "    MessageKey(ErrorCode errorCode, String suffix, Set<String> placeholders) {\n");
      writer.write("      this.errorCode = errorCode;\n");
      writer.write("      this.suffix = suffix;\n");
      writer.write("      this.fullKey = errorCode.name() + '.' + suffix;\n");
      writer.write("      this.placeholders = Collections.unmodifiableSet(placeholders);\n");
      writer.write("    }\n\n");

      writer.write("    public ErrorCode errorCode() {\n");
      writer.write("      return errorCode;\n");
      writer.write("    }\n\n");

      writer.write("    public String suffix() {\n");
      writer.write("      return suffix;\n");
      writer.write("    }\n\n");

      writer.write("    public String bundleKey() {\n");
      writer.write("      return fullKey;\n");
      writer.write("    }\n\n");

      writer.write("    public String fullKey() {\n");
      writer.write("      return fullKey;\n");
      writer.write("    }\n\n");

      writer.write("    public Set<String> placeholders() {\n");
      writer.write("      return placeholders;\n");
      writer.write("    }\n\n");

      writer.write("  }\n\n");

      writer.write(
          "  private static final Map<String, MessageKey> KEY_BY_SUFFIX ="
              + " Arrays.stream(MessageKey.values())\n");
      writer.write(
          "      .collect(Collectors.toUnmodifiableMap(MessageKey::suffix,"
              + " Function.identity()));\n\n");

      writer.write(
          "  private static final Map<String, MessageKey> KEY_BY_FULL_KEY ="
              + " Arrays.stream(MessageKey.values())\n");
      writer.write(
          "      .collect(Collectors.toUnmodifiableMap(MessageKey::fullKey,"
              + " Function.identity()));\n\n");

      writer.write("  public static MessageKey bySuffix(String suffix) {\n");
      writer.write("    Objects.requireNonNull(suffix, \"suffix\");\n");
      writer.write("    MessageKey key = KEY_BY_SUFFIX.get(suffix);\n");
      writer.write(
          "    return Objects.requireNonNull(key, \"Unknown error message suffix: \" + suffix);\n");
      writer.write("  }\n\n");

      writer.write("  public static MessageKey findBySuffix(String suffix) {\n");
      writer.write("    if (suffix == null || suffix.isBlank()) {\n");
      writer.write("      return null;\n");
      writer.write("    }\n");
      writer.write("    return KEY_BY_SUFFIX.get(suffix);\n");
      writer.write("  }\n\n");

      writer.write("  public static MessageKey byFullKey(String fullKey) {\n");
      writer.write("    Objects.requireNonNull(fullKey, \"fullKey\");\n");
      writer.write("    MessageKey key = KEY_BY_FULL_KEY.get(fullKey);\n");
      writer.write(
          "    return Objects.requireNonNull(key, \"Unknown error message key: \" + fullKey);\n");
      writer.write("  }\n\n");

      writer.write("  public static MessageKey findByFullKey(String fullKey) {\n");
      writer.write("    if (fullKey == null || fullKey.isBlank()) {\n");
      writer.write("      return null;\n");
      writer.write("    }\n");
      writer.write("    return KEY_BY_FULL_KEY.get(fullKey);\n");
      writer.write("  }\n");

      writer.write("}\n");
    } catch (IOException e) {
      throw new MojoExecutionException("Unable to write " + target, e);
    }
  }

  static String renderPlaceholderLiteral(Set<String> placeholders) {
    if (placeholders.isEmpty()) {
      return "Set.of()";
    }
    String joined =
        placeholders.stream()
            .sorted()
            .map(GenerateErrorMessagesMojo::quote)
            .collect(Collectors.joining(", "));
    return "Set.of(" + joined + ")";
  }

  static String quote(String value) {
    String escaped = value.replace("\\\\", "\\\\\\\\").replace("\"", "\\\\\"");
    return "\"" + escaped + "\"";
  }

  private Set<String> loadAllowedErrorCodes(Path proto)
      throws MojoExecutionException, MojoFailureException {
    if (!Files.isRegularFile(proto)) {
      throw new MojoFailureException("Error code proto file missing: " + proto);
    }
    Pattern memberPattern = Pattern.compile("\\s*([A-Z][A-Z0-9_]+)\\s*=.*");
    boolean inEnum = false;
    Set<String> codes = new TreeSet<>();
    try {
      for (String raw : Files.readAllLines(proto, StandardCharsets.UTF_8)) {
        String line = raw.replaceAll("//.*", "").trim();
        if (line.isEmpty()) {
          continue;
        }
        if (!inEnum) {
          if (line.startsWith("enum ErrorCode")) {
            inEnum = true;
          }
          continue;
        }
        if (line.startsWith("}")) {
          break;
        }
        Matcher matcher = memberPattern.matcher(line);
        if (matcher.matches()) {
          codes.add(matcher.group(1));
        }
      }
    } catch (IOException e) {
      throw new MojoExecutionException("Unable to read " + proto, e);
    }
    if (!inEnum || codes.isEmpty()) {
      throw new MojoFailureException("Failed to load ErrorCode entries from " + proto);
    }
    return Set.copyOf(codes);
  }

  private static final class MessageDefinition {
    private final String fullKey;
    private final String suffix;
    private final String errorCodeName;
    private final Set<String> placeholders;

    private MessageDefinition(
        String fullKey, String suffix, String errorCodeName, Set<String> placeholders) {
      this.fullKey = fullKey;
      this.suffix = suffix;
      this.errorCodeName = errorCodeName;
      this.placeholders = Set.copyOf(placeholders);
    }

    String fullKey() {
      return fullKey;
    }

    String suffix() {
      return suffix;
    }

    String errorCodeName() {
      return errorCodeName;
    }

    Set<String> placeholders() {
      return placeholders;
    }
  }

  private static final class GeneratedEntry {
    private final String constantName;
    private final MessageDefinition definition;

    private GeneratedEntry(String constantName, MessageDefinition definition) {
      this.constantName = constantName;
      this.definition = definition;
    }

    String constantName() {
      return constantName;
    }

    MessageDefinition definition() {
      return definition;
    }
  }
}
