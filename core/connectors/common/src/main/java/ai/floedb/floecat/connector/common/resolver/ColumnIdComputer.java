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

package ai.floedb.floecat.connector.common.resolver;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.query.rpc.SchemaColumn;

/**
 * Deterministic column_id computation.
 *
 * <p>This is intentionally self-contained and does NOT rely on other Floecat helper classes.
 *
 * <p>column_id must be: - stable across restarts - stable across connector implementations - stable
 * across query planner calls
 *
 * <p>It intentionally does NOT depend on snapshot_id. snapshot_id belongs to the stats key, while
 * column_id is the stable identifier for a column.
 *
 * <p>Algorithms: - CID_FIELD_ID: stable format-level ID (Iceberg-style), derived from field_id
 * only. - CID_PATH_ORDINAL: stable column path (physical_path or name) + ordinal
 * (Delta/generic-style). - CID_UNSPECIFIED: treated as CID_PATH_ORDINAL.
 */
public final class ColumnIdComputer {

  private ColumnIdComputer() {}

  /**
   * Compute a stable column_id for a SchemaColumn.
   *
   * <p>Algorithms: - CID_FIELD_ID: stable format-level ID (Iceberg-style), derived from field_id
   * only. - CID_PATH_ORDINAL: stable column path (physical_path or name) + ordinal
   * (Delta/generic-style). - CID_UNSPECIFIED: treated as CID_PATH_ORDINAL.
   */
  public static long compute(ColumnIdAlgorithm algo, SchemaColumn col) {
    if (col == null) {
      return 0L;
    }
    return compute(algo, col.getName(), col.getPhysicalPath(), col.getOrdinal(), col.getFieldId());
  }

  /**
   * Compute a stable column_id from raw connector/schema fields.
   *
   * <p>Inputs are intentionally minimal so connectors don't need to depend on SchemaColumn.
   */
  public static long compute(
      ColumnIdAlgorithm algo, String name, String physicalPath, int ordinal, int fieldId) {

    // Treat UNSPECIFIED as the default (path + ordinal).
    if (algo == null || algo == ColumnIdAlgorithm.CID_UNSPECIFIED) {
      algo = ColumnIdAlgorithm.CID_PATH_ORDINAL;
    }

    return switch (algo) {
      case CID_FIELD_ID -> computeFieldId(fieldId);
      case CID_PATH_ORDINAL -> computePathOrdinal(name, physicalPath, ordinal);
      case CID_UNKNOWN, UNRECOGNIZED -> 0L;
      default -> 0L;
    };
  }

  /** Convenience: compute() + return a copy with column_id set. */
  public static SchemaColumn withComputedId(ColumnIdAlgorithm algo, SchemaColumn col) {
    if (col == null) {
      return SchemaColumn.getDefaultInstance();
    }
    long id = compute(algo, col);
    if (id == 0L) {
      return col;
    }
    return col.toBuilder().setId(id).build();
  }

  /**
   * Deterministic canonicalization for nested column paths.
   *
   * <p>Goals: - match common vendor conventions - normalize Iceberg "element" paths to [] form -
   * avoid needing schema-based resolution
   *
   * <p>Rules: - trim - collapse repeated dots - normalize ".element." => "[]." - normalize trailing
   * ".element" => "[]"
   */
  public static String canonicalizePath(String raw) {
    String s = raw == null ? "" : raw.trim();
    if (s.isEmpty()) {
      return s;
    }

    // Collapse accidental double dots early so element normalization is stable.
    while (s.contains("..")) {
      s = s.replace("..", ".");
    }

    // Normalize Iceberg list-of-struct convention.
    // Common representations:
    //  - "a.element.b"  -> "a[].b"
    //  - "a.element"    -> "a[]"
    s = s.replace(".element.", "[].");
    if (s.endsWith(".element")) {
      s = s.substring(0, s.length() - ".element".length()) + "[]";
    }

    // Some producers may render list paths as "a.[].b"; normalize to "a[].b".
    // Be defensive and handle both the ".[].[" form and the ".[]" prefix.
    s = s.replace(".[].", "[].");
    s = s.replace(".[]", "[]");

    // Strip edge dots (".a" or "a.")
    while (s.startsWith(".")) {
      s = s.substring(1);
    }
    while (s.endsWith(".")) {
      s = s.substring(0, s.length() - 1);
    }

    return s;
  }

  /**
   * FIELD_ID algorithm: compute id from the stable format-level field id only.
   *
   * <p>Does NOT depend on path/name/ordinal to remain stable across renames and reordering.
   */
  private static long computeFieldId(int fieldId) {
    if (fieldId <= 0) {
      return 0L;
    }

    // Fast path: Iceberg-style stable field ids are already integers.
    // No hashing/tagging for this policy.
    return (long) fieldId;
  }

  /** PATH_ORDINAL algorithm: compute id from canonical path (physical_path or name) + ordinal. */
  private static long computePathOrdinal(String name, String physicalPath, int ordinal) {
    if (ordinal <= 0) {
      return 0L;
    }

    // Prefer physical_path when present; fall back to name.
    String path = (physicalPath == null || physicalPath.isBlank()) ? name : physicalPath;
    if (path == null || path.isBlank()) {
      return 0L;
    }

    final String canonicalPath = canonicalizePath(path);
    return computePathOrdinalId(canonicalPath, ordinal);
  }

  // --- Fast 64-bit hashing utilities (no allocations, no dependencies) ---

  // FNV-1a 64-bit parameters
  private static final long FNV64_OFFSET_BASIS = 0xcbf29ce484222325L;
  private static final long FNV64_PRIME = 0x100000001b3L;

  // High-byte tags to keep algorithm namespaces disjoint.
  private static final long ALGO_TAG_PATH_ORDINAL = 0x02L;

  /**
   * PATH_ORDINAL id = tag + hash64(canonicalPath (UTF-8) + '\0' + ordinal(be32)).
   *
   * <p>This avoids constructing payload strings and avoids allocating UTF-8 byte arrays.
   */
  private static long computePathOrdinalId(String canonicalPath, int ordinal) {
    long h = FNV64_OFFSET_BASIS;

    // Domain separation so different algorithms cannot collide even with same inputs.
    h = fnv1aUpdateByte(h, (byte) ALGO_TAG_PATH_ORDINAL);

    // Hash canonicalPath as UTF-8 bytes, streaming.
    h = fnv1aUpdateUtf8(h, canonicalPath);

    // Separator.
    h = fnv1aUpdateByte(h, (byte) 0);

    // Ordinal as big-endian 32-bit.
    h = fnv1aUpdateIntBE(h, ordinal);

    // Final mix to improve avalanche.
    h = fmix64(h);

    // Tag in high byte as well (keeps namespaces disjoint even if fmix64 changes later).
    long id = (ALGO_TAG_PATH_ORDINAL << 56) ^ (h & 0x00FF_FFFF_FFFF_FFFFL);

    // Keep 0 as a sentinel for "missing/uncomputed".
    return (id == 0L) ? 1L : id;
  }

  private static long fnv1aUpdateByte(long h, byte b) {
    return (h ^ (b & 0xFFL)) * FNV64_PRIME;
  }

  private static long fnv1aUpdateIntBE(long h, int v) {
    h = fnv1aUpdateByte(h, (byte) ((v >>> 24) & 0xFF));
    h = fnv1aUpdateByte(h, (byte) ((v >>> 16) & 0xFF));
    h = fnv1aUpdateByte(h, (byte) ((v >>> 8) & 0xFF));
    h = fnv1aUpdateByte(h, (byte) (v & 0xFF));
    return h;
  }

  /** Update FNV-1a with the UTF-8 encoding of the given string, without allocating. */
  private static long fnv1aUpdateUtf8(long h, String s) {
    if (s == null || s.isEmpty()) {
      return h;
    }

    final int len = s.length();
    for (int i = 0; i < len; i++) {
      char c = s.charAt(i);

      if (c < 0x80) {
        // 1-byte UTF-8: U+0000 to U+007F
        h = fnv1aUpdateByte(h, (byte) c);
        continue;
      }

      if (c < 0x800) {
        // 2-byte UTF-8: U+0080 to U+07FF
        h = fnv1aUpdateByte(h, (byte) (0xC0 | (c >>> 6)));
        h = fnv1aUpdateByte(h, (byte) (0x80 | (c & 0x3F)));
        continue;
      }

      // Check for surrogate pairs BEFORE 3-byte handling (surrogates encode code points > U+FFFF)
      if (Character.isHighSurrogate(c) && (i + 1) < len) {
        char d = s.charAt(i + 1);
        if (Character.isLowSurrogate(d)) {
          // 4-byte UTF-8: U+10000 to U+10FFFF (surrogate pair in Java UTF-16)
          int codePoint = Character.toCodePoint(c, d);
          h = fnv1aUpdateByte(h, (byte) (0xF0 | (codePoint >>> 18)));
          h = fnv1aUpdateByte(h, (byte) (0x80 | ((codePoint >>> 12) & 0x3F)));
          h = fnv1aUpdateByte(h, (byte) (0x80 | ((codePoint >>> 6) & 0x3F)));
          h = fnv1aUpdateByte(h, (byte) (0x80 | (codePoint & 0x3F)));
          i++; // consumed low surrogate
          continue;
        }
        // High surrogate without low surrogate is malformed; treat as 3-byte
      }

      // 3-byte UTF-8: U+0800 to U+FFFF (non-surrogates in this range, surrogates were above)
      h = fnv1aUpdateByte(h, (byte) (0xE0 | (c >>> 12)));
      h = fnv1aUpdateByte(h, (byte) (0x80 | ((c >>> 6) & 0x3F)));
      h = fnv1aUpdateByte(h, (byte) (0x80 | (c & 0x3F)));
    }

    return h;
  }

  /** MurmurHash3 fmix64 finalizer (public domain style), good avalanche for 64-bit state. */
  private static long fmix64(long k) {
    k ^= (k >>> 33);
    k *= 0xff51afd7ed558ccdL;
    k ^= (k >>> 33);
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= (k >>> 33);
    return k;
  }
}
