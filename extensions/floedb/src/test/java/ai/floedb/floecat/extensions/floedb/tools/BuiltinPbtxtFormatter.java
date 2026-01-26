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
package ai.floedb.floecat.extensions.floedb.tools;

import ai.floedb.floecat.extensions.floedb.proto.EngineFloeExtensions;
import ai.floedb.floecat.query.rpc.SystemObjectsRegistry;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Formats PBtxt fragments listed in src/main/resources/builtins/<engine>/_index.txt.
 *
 * <p>Goals: - stable formatting via protobuf TextFormat printer - preserve leading comment
 * header/preamble - add a blank line between top-level blocks for readability
 *
 * <p>Usage: --engine=floedb (default floedb) --apply rewrite files in-place --check fail if any
 * file would change
 */
public final class BuiltinPbtxtFormatter {

  public static void main(String[] args) throws Exception {
    Args a = Args.parse(args);

    String moduleBase = System.getProperty("module.basedir", ".");
    Path baseDir = Path.of(moduleBase, "src", "main", "resources", "builtins", a.engineKind);

    Path indexPath = baseDir.resolve("_index.txt");
    if (!Files.exists(indexPath)) {
      throw new IllegalStateException("Index not found: " + indexPath.toAbsolutePath());
    }

    List<String> fragments = readIndex(indexPath);
    if (fragments.isEmpty()) {
      throw new IllegalStateException("No fragments listed in: " + indexPath.toAbsolutePath());
    }

    List<String> changed = new ArrayList<>();
    for (String frag : fragments) {
      Path file = baseDir.resolve(frag);
      if (!Files.exists(file)) {
        throw new IllegalStateException("Fragment missing: " + file.toAbsolutePath());
      }

      String original = Files.readString(file, StandardCharsets.UTF_8);
      String formatted = formatFragment(original);

      // Ensure final newline
      if (!formatted.endsWith("\n")) formatted = formatted + "\n";

      if (!normalizedEquals(original, formatted)) {
        changed.add(frag);
        if (a.apply) {
          Files.writeString(file, formatted, StandardCharsets.UTF_8);
        }
      }
    }

    if (a.check && !changed.isEmpty()) {
      System.err.println("PBtxt formatting differs for " + changed.size() + " file(s):");
      for (String c : changed) {
        System.err.println("  - " + c);
      }
      System.err.println();
      System.err.println("Fix with:");
      System.err.println(
          "  mvn -pl extensions/floedb exec:java@format-pbtxt -Dexec.args=\"--engine="
              + a.engineKind
              + " --apply\"");
      System.exit(2);
    }

    if (a.apply) {
      System.out.println(
          "Formatted " + fragments.size() + " fragment(s). Changed: " + changed.size());
    } else if (a.check) {
      System.out.println("Format check OK for " + fragments.size() + " fragment(s).");
    } else {
      System.out.println("No mode selected. Use --check or --apply.");
      System.exit(2);
    }
  }

  private static boolean normalizedEquals(String a, String b) {
    // Normalize CRLF vs LF and trailing whitespace for robust comparisons.
    String na = a.replace("\r\n", "\n").replaceAll("[ \t]+\n", "\n");
    String nb = b.replace("\r\n", "\n").replaceAll("[ \t]+\n", "\n");
    return na.equals(nb);
  }

  private static List<String> readIndex(Path indexPath) throws IOException {
    List<String> out = new ArrayList<>();
    for (String raw : Files.readAllLines(indexPath, StandardCharsets.UTF_8)) {
      String line = raw.trim();
      if (line.isEmpty() || line.startsWith("#")) continue;
      if (line.startsWith("/")) {
        throw new IllegalStateException("Index entries must be relative paths: " + line);
      }
      out.add(line);
    }
    return out;
  }

  private static String formatFragment(String rawText) throws Exception {
    // Preserve leading preamble (copyright header / comments).
    Split s = splitPreamble(rawText);

    // Parse body as SystemObjectsRegistry fields.
    SystemObjectsRegistry.Builder b = SystemObjectsRegistry.newBuilder();
    TextFormat.Parser parser = TextFormat.Parser.newBuilder().build();

    ExtensionRegistry er = ExtensionRegistry.newInstance();
    EngineFloeExtensions.registerAllExtensions(er);

    parser.merge(new StringReader(s.body), er, b);

    // Print in canonical protobuf TextFormat.
    TextFormat.Printer printer =
        TextFormat.printer().escapingNonAscii(false); // keep readable text when possible
    String printed = printer.printToString(b.build());

    // Add readability: blank line between top-level blocks.
    // Heuristic: whenever a top-level '}' is followed by a new field at column 0, insert \n.
    printed = insertBlankLinesBetweenTopLevelBlocks(printed);

    // Stitch back together
    StringBuilder out = new StringBuilder();
    if (!s.preamble.isEmpty()) {
      out.append(s.preamble);
      if (!s.preamble.endsWith("\n")) out.append("\n");
      if (!s.preamble.endsWith("\n\n")) out.append("\n"); // keep one blank line after header
    }
    out.append(printed.stripTrailing());

    return out.toString() + "\n";
  }

  private static String insertBlankLinesBetweenTopLevelBlocks(String printed) {
    // This is intentionally simple and stable.
    // Example:
    //   functions { ... }\nfunctions { ... }
    // becomes:
    //   functions { ... }\n\nfunctions { ... }
    return printed.replaceAll("\\}\n(?=[a-zA-Z_][a-zA-Z0-9_]*\\s*\\{)", "}\n\n");
  }

  private record Split(String preamble, String body) {}

  private static Split splitPreamble(String raw) {
    // We keep initial comment-only lines and blank lines until we hit something that
    // looks like a pbtxt field start:  identifier {
    String[] lines = raw.replace("\r\n", "\n").split("\n", -1);

    int i = 0;
    for (; i < lines.length; i++) {
      String l = lines[i];
      String t = l.trim();
      if (t.isEmpty()) continue;
      if (t.startsWith("#")) continue;
      if (t.startsWith("//")) continue;

      // First non-comment, non-blank line => treat as pbtxt body.
      break;
    }

    String preamble = String.join("\n", java.util.Arrays.copyOfRange(lines, 0, i)).stripTrailing();
    String body = String.join("\n", java.util.Arrays.copyOfRange(lines, i, lines.length)).strip();

    // If file is only comments/blank, keep it unchanged (rare, but safe)
    if (body.isEmpty()) {
      return new Split(preamble, "");
    }
    return new Split(preamble, body);
  }

  private static final class Args {
    final String engineKind;
    final boolean apply;
    final boolean check;

    private Args(String engineKind, boolean apply, boolean check) {
      this.engineKind = engineKind;
      this.apply = apply;
      this.check = check;
    }

    static Args parse(String[] args) {
      String engine = "floedb";
      boolean apply = false;
      boolean check = false;

      for (String a : args) {
        if (a.startsWith("--engine=")) engine = a.substring("--engine=".length());
        else if (a.equals("--apply")) apply = true;
        else if (a.equals("--check")) check = true;
      }

      if (apply == check) {
        throw new IllegalArgumentException("Specify exactly one of --apply or --check (not both).");
      }

      return new Args(engine, apply, check);
    }
  }
}
