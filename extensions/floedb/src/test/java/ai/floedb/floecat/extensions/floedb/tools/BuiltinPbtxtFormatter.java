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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Formats PBtxt fragments listed in src/main/resources/builtins/&lt;engine&gt;/_index.txt.
 *
 * <p>Features:
 *
 * <ul>
 *   <li>Canonical protobuf formatting (per block)
 *   <li>Stable ordering of top-level blocks
 *   <li>Leading comments preserved and attached to their block
 *   <li>Blank line separation between blocks
 * </ul>
 *
 * <p>Comment handling notes:
 *
 * <ul>
 *   <li>Line comments: {@code # ...} and {@code // ...}
 *   <li>Block comments: {@code /* ... *\/} (can span lines)
 *   <li>Comments are ignored for brace matching and entry splitting
 *   <li>String literals are respected; comment markers inside strings are not treated as comments
 * </ul>
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

      if (!normalizedEquals(original, formatted)) {
        changed.add(frag);
        if (a.apply) {
          Files.writeString(file, formatted, StandardCharsets.UTF_8);
        }
      }
    }

    if (a.check && !changed.isEmpty()) {
      System.err.println("PBtxt formatting differs for " + changed.size() + " file(s):");
      for (String c : changed) System.err.println("  - " + c);
      System.err.println();
      System.err.println(
          "Fix with:\n  mvn -pl extensions/floedb exec:java@format-pbtxt "
              + "-Dexec.args=\"--engine="
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
      throw new IllegalArgumentException("Specify exactly one of --apply or --check.");
    }
  }

  /* ============================ Core formatting ============================ */

  private static String formatFragment(String raw) throws Exception {
    Split split = splitPreamble(raw);

    // If file is only comments/blank, keep stable output with a trailing newline.
    if (split.body.isEmpty()) {
      String pre = split.preamble.stripTrailing();
      return pre.isEmpty() ? "\n" : pre + "\n";
    }

    List<Entry> entries = splitTopLevelEntries(split.body);

    List<Entry> formatted = new ArrayList<>(entries.size());
    for (Entry e : entries) {
      formatted.add(e.withPrinted(formatOneBlock(e.blockText)));
    }

    formatted.sort(
        Comparator.comparing(Entry::fieldName)
            .thenComparing(BuiltinPbtxtFormatter::sortKey)
            .thenComparing(Entry::printedBlock));

    StringBuilder out = new StringBuilder();
    if (!split.preamble.isEmpty()) {
      out.append(split.preamble.stripTrailing()).append("\n\n");
    }

    for (int i = 0; i < formatted.size(); i++) {
      Entry e = formatted.get(i);
      if (!e.leading.isEmpty()) {
        out.append(e.leading.stripTrailing()).append("\n");
      }
      out.append(e.printedBlock.stripTrailing()).append("\n");
      if (i + 1 < formatted.size()) out.append("\n");
    }

    return out.toString().stripTrailing() + "\n";
  }

  private static String formatOneBlock(String blockText) throws Exception {
    SystemObjectsRegistry.Builder b = SystemObjectsRegistry.newBuilder();

    TextFormat.Parser parser = TextFormat.Parser.newBuilder().build();
    ExtensionRegistry er = ExtensionRegistry.newInstance();
    EngineFloeExtensions.registerAllExtensions(er);

    parser.merge(new StringReader(blockText), er, b);

    // Canonical protobuf formatting.
    return TextFormat.printer().escapingNonAscii(false).printToString(b.build()).stripTrailing();
  }

  /* ============================ Ordering logic ============================ */

  private static String sortKey(Entry e) {
    String f = e.fieldName;
    String s = e.printedBlock;

    if (f.equals("functions")) {
      String name = extractNameRef(s, "name");
      String ret = extractNameRef(s, "return_type");
      List<String> args = extractRepeatedNameRefs(s, "argument_types");
      return "fn:" + name + "(" + String.join(",", args) + ")->" + ret;
    }

    if (f.equals("system_tables") || f.equals("system_views")) {
      return f + ":" + extractNameRef(s, "name");
    }

    if (f.equals("system_namespaces")) {
      return "ns:" + extractNameRef(s, "name");
    }

    String name = extractNameRef(s, "name");
    return name.isEmpty() ? s : f + ":" + name;
  }

  /* ============================ Parsing helpers ============================ */

  /**
   * Splits the pbtxt "body" into top-level {@code field_name { ... }} entries and associates any
   * immediately preceding comment/blank lines ("leading") with that entry.
   *
   * <p>Robustness: detects and throws on malformed input (missing braces, unterminated strings,
   * unterminated comments) to avoid hangs or partial output.
   */
  private static List<Entry> splitTopLevelEntries(String body) {
    String src = body.replace("\r\n", "\n");
    int n = src.length();
    int i = 0;

    List<Entry> out = new ArrayList<>();
    StringBuilder leading = new StringBuilder();

    while (i < n) {
      int lineEnd = src.indexOf('\n', i);
      if (lineEnd < 0) lineEnd = n;
      String line = src.substring(i, lineEnd);
      String t = line.trim();

      // Collect leading blank/comment lines (only line-style comments here).
      if (t.isEmpty() || t.startsWith("#") || t.startsWith("//")) {
        leading.append(line).append("\n");
        i = lineEnd + 1;
        continue;
      }

      // Find identifier start (skip whitespace).
      int p = i;
      while (p < n && Character.isWhitespace(src.charAt(p))) p++;

      int idStart = p;

      // Field name must start with [A-Za-z_], then [A-Za-z0-9_]*.
      if (p >= n || !isIdentStart(src.charAt(p))) {
        throw parseErrorAt(src, idStart, "Expected a top-level field identifier");
      }
      p++;
      while (p < n && isIdentPart(src.charAt(p))) p++;

      String fieldName = src.substring(idStart, p);

      // Skip whitespace and require '{'.
      while (p < n && Character.isWhitespace(src.charAt(p))) p++;
      if (p >= n || src.charAt(p) != '{') {
        throw parseErrorAt(src, p, "Expected '{' after top-level field '" + fieldName + "'");
      }

      // Scan forward to match the closing brace for this top-level block.
      int depth = 0;
      boolean inString = false;
      boolean escaped = false;
      int j = p;

      // Track "only whitespace since newline" to limit line-comment detection.
      boolean onlyWsSinceNewline = true;

      boolean foundClose = false;

      while (j < n) {
        char c = src.charAt(j);

        if (inString) {
          if (escaped) {
            escaped = false;
            j++;
            continue;
          }
          if (c == '\\') {
            escaped = true;
            j++;
            continue;
          }
          if (c == '"') {
            inString = false;
          }
          j++;
          continue;
        }

        // Newline resets "only whitespace since newline".
        if (c == '\n') {
          onlyWsSinceNewline = true;
          j++;
          continue;
        }

        // Update whitespace tracker.
        if (!Character.isWhitespace(c)) {
          // We'll re-enable it when we hit a newline.
          onlyWsSinceNewline = false;
        }

        // Block comment: /* ... */
        if (c == '/' && j + 1 < n && src.charAt(j + 1) == '*') {
          j = skipBlockComment(src, j);
          // skipBlockComment returns index after the comment; it may land on '\n'
          continue;
        }

        // Line comments: only if they start at the beginning of a line (or after whitespace).
        if (onlyWsSinceNewline) {
          if (c == '#') {
            j = skipToLineEnd(src, j);
            continue;
          }
          if (c == '/' && j + 1 < n && src.charAt(j + 1) == '/') {
            j = skipToLineEnd(src, j);
            continue;
          }
        }

        if (c == '"') {
          inString = true;
          escaped = false;
          j++;
          continue;
        }

        if (c == '{') {
          depth++;
        } else if (c == '}') {
          depth--;
          if (depth == 0) {
            // Include the closing brace and an optional trailing newline.
            j++;
            if (j < n && src.charAt(j) == '\n') j++;
            String block = src.substring(idStart, j).stripTrailing();
            out.add(new Entry(fieldName, leading.toString().stripTrailing(), block, null));
            leading.setLength(0);
            i = j;
            foundClose = true;
            break;
          }
        }
        j++;
      }

      if (!foundClose) {
        throw parseErrorAt(
            src, idStart, "Unterminated top-level block for field '" + fieldName + "'");
      }
    }

    return out;
  }

  private static boolean isIdentStart(char c) {
    return c == '_' || Character.isLetter(c);
  }

  private static boolean isIdentPart(char c) {
    return c == '_' || Character.isLetterOrDigit(c);
  }

  private static String extractNameRef(String block, String field) {
    int i = indexOfWholeWordField(block, field);
    if (i < 0) return "";
    int brace = block.indexOf('{', i);
    if (brace < 0) return "";
    int end = findClosingBrace(block, brace);
    if (end < 0) return "";
    String chunk = block.substring(i, end);
    String path = extractQuotedKey(chunk, "path");
    String name = extractQuotedKey(chunk, "name");
    return path.isEmpty() ? name : path + "." + name;
  }

  private static List<String> extractRepeatedNameRefs(String block, String field) {
    List<String> out = new ArrayList<>();
    int i = 0;
    while (i < block.length()) {
      int at = indexOfWholeWordField(block, field, i);
      if (at < 0) break;
      int brace = block.indexOf('{', at);
      if (brace < 0) break;
      int end = findClosingBrace(block, brace);
      if (end < 0) break;
      String chunk = block.substring(at, end);
      String path = extractQuotedKey(chunk, "path");
      String name = extractQuotedKey(chunk, "name");
      out.add(path.isEmpty() ? name : path + "." + name);
      i = end;
    }
    return out;
  }

  /**
   * Finds the end of a brace-delimited object starting at {@code openIndex} (where text[openIndex]
   * == '{'). Ignores comments and respects strings.
   */
  private static int findClosingBrace(String text, int openIndex) {
    int depth = 0;
    boolean inString = false;
    boolean escaped = false;
    int n = text.length();

    boolean onlyWsSinceNewline = true;

    for (int i = openIndex; i < n; i++) {
      char c = text.charAt(i);

      if (inString) {
        if (escaped) {
          escaped = false;
          continue;
        }
        if (c == '\\') {
          escaped = true;
          continue;
        }
        if (c == '"') {
          inString = false;
        }
        continue;
      }

      if (c == '\n') {
        onlyWsSinceNewline = true;
        continue;
      }
      if (!Character.isWhitespace(c)) {
        onlyWsSinceNewline = false;
      }

      // Block comment: /* ... */
      if (c == '/' && i + 1 < n && text.charAt(i + 1) == '*') {
        i = skipBlockComment(text, i) - 1;
        continue;
      }

      // Line comments: only if they start at the beginning of a line (or after whitespace).
      if (onlyWsSinceNewline) {
        if (c == '#') {
          i = skipToLineEnd(text, i) - 1;
          continue;
        }
        if (c == '/' && i + 1 < n && text.charAt(i + 1) == '/') {
          i = skipToLineEnd(text, i) - 1;
          continue;
        }
      }

      if (c == '"') {
        inString = true;
        escaped = false;
        continue;
      }

      if (c == '{') {
        depth++;
      } else if (c == '}') {
        depth--;
        if (depth == 0) {
          return i + 1;
        }
      }
    }
    return -1;
  }

  private static int skipToLineEnd(String text, int start) {
    int idx = text.indexOf('\n', start);
    return idx < 0 ? text.length() : idx + 1;
  }

  /** Skips a block comment starting at {@code /*} and returns the index just after {@code //*}. */
  private static int skipBlockComment(String text, int start) {
    int n = text.length();
    // start points at '/', start+1 == '*'
    int i = start + 2;
    while (i + 1 < n) {
      if (text.charAt(i) == '*' && text.charAt(i + 1) == '/') {
        return i + 2;
      }
      i++;
    }
    throw parseErrorAt(text, start, "Unterminated block comment");
  }

  /**
   * Returns the index of a field token as a whole word (not part of a larger identifier).
   *
   * <p>This is intentionally lightweight; it reduces accidental matches (e.g. "name" inside a
   * longer token).
   */
  private static int indexOfWholeWordField(String s, String field) {
    return indexOfWholeWordField(s, field, 0);
  }

  private static int indexOfWholeWordField(String s, String field, int fromIndex) {
    int i = fromIndex;
    while (true) {
      int at = s.indexOf(field, i);
      if (at < 0) return -1;

      boolean okLeft = at == 0 || !isIdentPart(s.charAt(at - 1));
      int right = at + field.length();
      boolean okRight = right >= s.length() || !isIdentPart(s.charAt(right));

      if (okLeft && okRight) return at;
      i = at + 1;
    }
  }

  private static String extractQuotedKey(String s, String key) {
    // Prefer "key:" occurrences (avoids false hits on substrings).
    int i = s.indexOf(key + ":");
    if (i < 0) return "";
    int q1 = s.indexOf('"', i);
    int q2 = q1 >= 0 ? s.indexOf('"', q1 + 1) : -1;
    return q2 > q1 ? s.substring(q1 + 1, q2) : "";
  }

  private static IllegalStateException parseErrorAt(String src, int offset, String msg) {
    int[] lc = lineCol(src, offset);
    String excerpt = excerpt(src, offset, 120);
    return new IllegalStateException(
        msg + " at line " + lc[0] + ", col " + lc[1] + ". Near: " + excerpt);
  }

  private static int[] lineCol(String s, int offset) {
    int n = s.length();
    int o = Math.max(0, Math.min(offset, n));
    int line = 1;
    int col = 1;
    for (int i = 0; i < o; i++) {
      char c = s.charAt(i);
      if (c == '\n') {
        line++;
        col = 1;
      } else {
        col++;
      }
    }
    return new int[] {line, col};
  }

  private static String excerpt(String s, int offset, int max) {
    int n = s.length();
    int o = Math.max(0, Math.min(offset, n));
    int start = Math.max(0, o - max / 2);
    int end = Math.min(n, start + max);
    String chunk = s.substring(start, end).replace("\n", "\\n");
    if (start > 0) chunk = "…" + chunk;
    if (end < n) chunk = chunk + "…";
    return chunk;
  }

  /* ============================ Utilities ============================ */

  private static boolean normalizedEquals(String a, String b) {
    // Normalize CRLF vs LF and trailing whitespace for robust comparisons.
    return a.replace("\r\n", "\n")
        .replaceAll("[ \t]+\n", "\n")
        .equals(b.replace("\r\n", "\n").replaceAll("[ \t]+\n", "\n"));
  }

  private static List<String> readIndex(Path p) throws IOException {
    List<String> out = new ArrayList<>();
    for (String l : Files.readAllLines(p, StandardCharsets.UTF_8)) {
      l = l.trim();
      if (!l.isEmpty() && !l.startsWith("#")) {
        if (l.startsWith("/")) {
          throw new IllegalStateException("Index entries must be relative paths: " + l);
        }
        out.add(l);
      }
    }
    return out;
  }

  private record Entry(String fieldName, String leading, String blockText, String printedBlock) {
    Entry withPrinted(String p) {
      return new Entry(fieldName, leading, blockText, p);
    }
  }

  private record Split(String preamble, String body) {}

  private static Split splitPreamble(String raw) {
    String[] lines = raw.replace("\r\n", "\n").split("\n", -1);
    int i = 0;
    while (i < lines.length) {
      String t = lines[i].trim();
      if (t.isEmpty() || t.startsWith("#") || t.startsWith("//")) i++;
      else break;
    }
    String pre = String.join("\n", Arrays.copyOfRange(lines, 0, i)).stripTrailing();
    String body = String.join("\n", Arrays.copyOfRange(lines, i, lines.length)).strip();
    return new Split(pre, body);
  }

  /* ============================ Args ============================ */

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
        throw new IllegalArgumentException("Specify exactly one of --apply or --check.");
      }
      return new Args(engine, apply, check);
    }
  }
}
