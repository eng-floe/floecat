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
package ai.floedb.floecat.client.cli.util;

import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.reconciler.rpc.CaptureColumnPolicy;
import ai.floedb.floecat.reconciler.rpc.CaptureMode;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.CapturePolicy;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Shared CLI helper utilities used by command support classes. */
public final class CliUtils {
  private CliUtils() {}

  // ---------------------------------------------------------------------------
  // Argument / flag parsing
  // ---------------------------------------------------------------------------

  public static Map<String, String> parseKeyValueList(List<String> args, String flag) {
    Map<String, String> out = new LinkedHashMap<>();
    for (int i = 0; i < args.size(); i++) {
      if (flag.equals(args.get(i)) && i + 1 < args.size()) {
        int j = i + 1;
        while (j < args.size() && !args.get(j).startsWith("--")) {
          String kv = args.get(j);
          int eq = kv.indexOf('=');
          if (eq > 0) {
            out.put(kv.substring(0, eq), kv.substring(eq + 1));
          }
          j++;
        }
      }
    }
    return out;
  }

  public static String nvl(String s, String d) {
    return s == null ? d : s;
  }

  // ---------------------------------------------------------------------------
  // Timestamp helpers
  // ---------------------------------------------------------------------------

  /**
   * Parses a timestamp from an epoch-seconds integer, an epoch-millis integer (≥ 1e12 in absolute
   * value), or an ISO-8601 instant string (e.g. {@code 2025-01-15T10:00:00Z}).
   */
  public static Timestamp parseTimestampFlexible(String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("timestamp value is blank");
    }
    String trimmed = value.trim();
    try {
      long numeric = Long.parseLong(trimmed);
      if (Math.abs(numeric) >= 1_000_000_000_000L) {
        long seconds = Math.floorDiv(numeric, 1000L);
        long millisPart = Math.floorMod(numeric, 1000L);
        int nanos = (int) (millisPart * 1_000_000L);
        return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
      }
      return Timestamp.newBuilder().setSeconds(numeric).build();
    } catch (NumberFormatException ignored) {
      try {
        Instant instant = Instant.parse(trimmed);
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "invalid timestamp '"
                + value
                + "' (expected epoch seconds/millis or ISO-8601 instant)");
      }
    }
  }

  /** Formats a protobuf Timestamp as an ISO-8601 string, or {@code "-"} if null / zero. */
  public static String ts(Timestamp t) {
    if (t == null || (t.getSeconds() == 0 && t.getNanos() == 0)) {
      return "-";
    }
    return Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toString();
  }

  // ---------------------------------------------------------------------------
  // ResourceId helpers
  // ---------------------------------------------------------------------------

  /** Returns the id string of a ResourceId, or {@code "<no-id>"} if null / blank. */
  public static String rid(ResourceId id) {
    String s = (id == null) ? null : id.getId();
    return (s == null || s.isBlank()) ? "<no-id>" : s;
  }

  /** Returns {@code true} if {@code s} is a standard UUID string (case-insensitive). */
  public static boolean looksLikeUuid(String s) {
    if (s == null) return false;
    return s.trim()
        .matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
  }

  // ---------------------------------------------------------------------------
  // Snapshot helpers
  // ---------------------------------------------------------------------------

  /**
   * Parses a snapshot selector token. Returns the SS_CURRENT special snapshot when the token is
   * {@code null}, blank, or the literal {@code "current"}; otherwise parses it as a snapshot-id
   * long.
   */
  public static SnapshotRef snapshotFromTokenOrCurrent(String tokenOrNull) {
    if (tokenOrNull == null || tokenOrNull.isBlank() || tokenOrNull.equalsIgnoreCase("current")) {
      return SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build();
    }
    try {
      return SnapshotRef.newBuilder().setSnapshotId(Long.parseLong(tokenOrNull.trim())).build();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("invalid snapshot selector '" + tokenOrNull + "'");
    }
  }

  // ---------------------------------------------------------------------------
  // CSV / list helpers
  // ---------------------------------------------------------------------------

  /**
   * Splits a CSV string using {@link CsvListParserUtil}, unquotes each token, and drops blanks.
   * Wraps parse errors as {@link IllegalArgumentException}.
   */
  public static List<String> csvList(String s) {
    try {
      return CsvListParserUtil.items(s).stream()
          .map(Quotes::unquote)
          .filter(t -> t != null && !t.isBlank())
          .toList();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("invalid CSV list: " + s, e);
    }
  }

  // ---------------------------------------------------------------------------
  // Formatting helpers
  // ---------------------------------------------------------------------------

  /**
   * Truncates {@code s} to at most {@code n} characters, appending a Unicode ellipsis (…) if
   * trimmed. Returns {@code "-"} for {@code null} input.
   */
  public static String trunc(String s, int n) {
    if (s == null) return "-";
    return s.length() <= n ? s : (s.substring(0, n - 1) + "\u2026");
  }

  /**
   * Converts an NDV (number of distinct values) proto to a human-readable string. Returns {@code
   * "-"} when no value is present.
   */
  public static String ndvToString(Ndv n) {
    if (n.hasExact()) {
      return Long.toString(n.getExact());
    }
    if (n.hasApprox()) {
      NdvApprox approx = n.getApprox();
      return approx.getEstimate() == Math.rint(approx.getEstimate())
          ? Long.toString(Math.round(approx.getEstimate()))
          : String.format(Locale.ROOT, "%.3f", approx.getEstimate());
    }
    return "-";
  }

  // ---------------------------------------------------------------------------
  // Capture / reconcile helpers
  // ---------------------------------------------------------------------------

  /**
   * Parses a capture mode string (e.g. {@code "metadata-only"}, {@code "capture-only"}) into the
   * corresponding {@link CaptureMode} enum value. Defaults to {@code CM_METADATA_AND_CAPTURE} when
   * blank or null.
   */
  public static CaptureMode parseCaptureMode(String s) {
    if (s == null || s.isBlank()) return CaptureMode.CM_METADATA_AND_CAPTURE;
    return switch (s.trim().toUpperCase(Locale.ROOT).replace('-', '_')) {
      case "METADATA_ONLY", "CM_METADATA_ONLY" -> CaptureMode.CM_METADATA_ONLY;
      case "METADATA_AND_CAPTURE", "CM_METADATA_AND_CAPTURE" -> CaptureMode.CM_METADATA_AND_CAPTURE;
      case "CAPTURE_ONLY", "CM_CAPTURE_ONLY" -> CaptureMode.CM_CAPTURE_ONLY;
      default -> throw new IllegalArgumentException("invalid capture mode: " + s);
    };
  }

  /** Parses a comma-separated capture output list. */
  public static Set<CaptureOutput> parseCaptureOutputs(String s) {
    if (s == null || s.isBlank()) {
      return Set.of();
    }
    java.util.LinkedHashSet<CaptureOutput> outputs = new java.util.LinkedHashSet<>();
    for (String token : csvList(s)) {
      switch (token.trim().toUpperCase(Locale.ROOT).replace('-', '_')) {
        case "STATS" -> {
          outputs.add(CaptureOutput.CO_TABLE_STATS);
          outputs.add(CaptureOutput.CO_FILE_STATS);
          outputs.add(CaptureOutput.CO_COLUMN_STATS);
        }
        case "TABLE_STATS" -> outputs.add(CaptureOutput.CO_TABLE_STATS);
        case "FILE_STATS" -> outputs.add(CaptureOutput.CO_FILE_STATS);
        case "COLUMN_STATS" -> outputs.add(CaptureOutput.CO_COLUMN_STATS);
        case "INDEX", "INDEXES", "PARQUET_PAGE_INDEX", "PAGE_INDEX", "PAGE_INDEXES" ->
            outputs.add(CaptureOutput.CO_PARQUET_PAGE_INDEX);
        default -> throw new IllegalArgumentException("invalid capture output: " + token);
      }
    }
    return Set.copyOf(outputs);
  }

  /** Builds a capture policy from explicit outputs. */
  public static CapturePolicy buildCapturePolicy(
      CaptureMode mode, Set<CaptureOutput> requestedOutputs, List<String> columns) {
    if (mode == CaptureMode.CM_METADATA_ONLY) {
      return null;
    }
    java.util.LinkedHashSet<CaptureOutput> outputs =
        new java.util.LinkedHashSet<>(requestedOutputs);
    if (outputs.isEmpty()) {
      throw new IllegalArgumentException("--capture is required for capture modes");
    }
    CapturePolicy.Builder policy = CapturePolicy.newBuilder().addAllOutputs(outputs);
    for (String column : columns == null ? List.<String>of() : columns) {
      if (column == null || column.isBlank()) {
        continue;
      }
      policy.addColumns(
          CaptureColumnPolicy.newBuilder()
              .setSelector(column)
              .setCaptureStats(outputs.contains(CaptureOutput.CO_COLUMN_STATS))
              .setCaptureIndex(outputs.contains(CaptureOutput.CO_PARQUET_PAGE_INDEX))
              .build());
    }
    return policy.build();
  }

  /**
   * Parses a comma-separated list of unsigned long snapshot IDs. Returns an empty list when blank
   * or null.
   */
  public static List<Long> parseSnapshotIds(String s) {
    if (s == null || s.isBlank()) return List.of();
    var result = new ArrayList<Long>();
    for (String token : csvList(s)) {
      result.add(Long.parseUnsignedLong(token));
    }
    return result;
  }

  // ---------------------------------------------------------------------------
  // JSON helpers
  // ---------------------------------------------------------------------------

  /** Prints a protobuf message as pretty JSON to {@code out}, including default-valued fields. */
  public static void printJson(MessageOrBuilder message, PrintStream out) {
    try {
      out.println(JsonFormat.printer().includingDefaultValueFields().print(message));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("failed to render protobuf as json", e);
    }
  }
}
