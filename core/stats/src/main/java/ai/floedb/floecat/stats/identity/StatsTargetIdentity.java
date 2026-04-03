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

package ai.floedb.floecat.stats.identity;

import ai.floedb.floecat.catalog.rpc.ColumnStatsTarget;
import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.engine.util.EngineIdentityNormalizer;
import ai.floedb.floecat.types.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Stable identity helpers for table, file, column, and engine-scoped expression stats targets.
 *
 * <p>Stability contract:
 *
 * <p>- Table identity is constant.
 *
 * <p>- Column identity is a stable function of {@code column_id}.
 *
 * <p>- File identity is a stable function of normalized {@code file_path}.
 *
 * <p>- Expression identity is a stable function of normalized {@code engine_kind} and opaque {@code
 * engine_expression_key} bytes only.
 *
 * <p>- {@code engine_version} is normalized for metadata hygiene but intentionally excluded from
 * identity so upgrades do not invalidate previously captured statistics.
 *
 * <p>- Hash format and separators are internal and deterministic; callers should treat the hash as
 * opaque.
 */
public final class StatsTargetIdentity {

  private static final byte SEP = 0x1f;

  private StatsTargetIdentity() {}

  /**
   * Normalizes and validates an engine expression target.
   *
   * <p>{@code engine_version} is intentionally not used for identity so stats survive engine
   * upgrades.
   */
  public static EngineExpressionStatsTarget normalizeExpressionTarget(
      EngineExpressionStatsTarget in) {
    Objects.requireNonNull(in, "in");
    String kind = normalizeEngineKind(in.getEngineKind());
    if (kind.isEmpty()) {
      throw new IllegalArgumentException("engine_kind must not be blank");
    }
    if (in.getEngineExpressionKey().isEmpty()) {
      throw new IllegalArgumentException("engine_expression_key must not be empty");
    }
    EngineExpressionStatsTarget.Builder b = in.toBuilder().setEngineKind(kind);
    String version = in.getEngineVersion().trim();
    if (version.isEmpty()) {
      b.clearEngineVersion();
    } else {
      b.setEngineVersion(version);
    }
    return b.build();
  }

  /** Canonical identity hash for any stats target. */
  public static String identityHashHex(StatsTarget target) {
    Objects.requireNonNull(target, "target");
    return switch (target.getTargetCase()) {
      case TABLE -> Hashing.sha256Hex(new byte[] {'T'});
      case FILE -> fileIdentityHashHex(target.getFile());
      case COLUMN -> columnIdentityHashHex(target.getColumn());
      case EXPRESSION -> expressionIdentityHashHex(target.getExpression());
      case TARGET_NOT_SET -> throw new IllegalArgumentException("StatsTarget target is not set");
    };
  }

  /** Canonical identity hash for a file target. */
  public static String fileIdentityHashHex(FileStatsTarget fileTarget) {
    Objects.requireNonNull(fileTarget, "fileTarget");
    String filePath = fileTarget.getFilePath().trim();
    if (filePath.isEmpty()) {
      throw new IllegalArgumentException("file_path must not be blank");
    }
    byte[] path = filePath.getBytes(StandardCharsets.UTF_8);
    byte[] payload = new byte[2 + path.length];
    int p = 0;
    payload[p++] = 'F';
    payload[p++] = SEP;
    System.arraycopy(path, 0, payload, p, path.length);
    return Hashing.sha256Hex(payload);
  }

  /** Canonical identity hash for a column target. */
  public static String columnIdentityHashHex(ColumnStatsTarget columnTarget) {
    Objects.requireNonNull(columnTarget, "columnTarget");
    if (columnTarget.getColumnId() <= 0L) {
      throw new IllegalArgumentException("column_id must be positive");
    }
    byte[] id = Long.toString(columnTarget.getColumnId()).getBytes(StandardCharsets.UTF_8);
    byte[] payload = new byte[2 + id.length];
    payload[0] = 'C';
    payload[1] = SEP;
    System.arraycopy(id, 0, payload, 2, id.length);
    return Hashing.sha256Hex(payload);
  }

  /**
   * Canonical identity hash for an engine expression target.
   *
   * <p>Includes normalized kind + opaque expression key. Excludes {@code engine_version}.
   */
  public static String expressionIdentityHashHex(EngineExpressionStatsTarget expressionTarget) {
    EngineExpressionStatsTarget normalized = normalizeExpressionTarget(expressionTarget);
    byte[] kind = normalized.getEngineKind().getBytes(StandardCharsets.UTF_8);
    byte[] expr = normalized.getEngineExpressionKey().toByteArray();
    byte[] payload = new byte[3 + kind.length + expr.length];
    int p = 0;
    payload[p++] = 'E';
    payload[p++] = SEP;
    System.arraycopy(kind, 0, payload, p, kind.length);
    p += kind.length;
    payload[p++] = SEP;
    System.arraycopy(expr, 0, payload, p, expr.length);
    return Hashing.sha256Hex(payload);
  }

  public static StatsTarget tableTarget() {
    return StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build();
  }

  public static StatsTarget columnTarget(long columnId) {
    return StatsTarget.newBuilder()
        .setColumn(ColumnStatsTarget.newBuilder().setColumnId(columnId).build())
        .build();
  }

  public static StatsTarget fileTarget(String filePath) {
    String normalized = filePath(filePath);
    return StatsTarget.newBuilder()
        .setFile(FileStatsTarget.newBuilder().setFilePath(normalized).build())
        .build();
  }

  /** Canonical file-path normalization for file stats targets. */
  public static String filePath(String filePath) {
    String normalized = Objects.requireNonNull(filePath, "filePath").trim();
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException("filePath must not be blank");
    }
    return normalized;
  }

  public static StatsTarget expressionTarget(EngineExpressionStatsTarget target) {
    return StatsTarget.newBuilder().setExpression(normalizeExpressionTarget(target)).build();
  }

  private static String normalizeEngineKind(String engineKind) {
    return EngineIdentityNormalizer.normalizeEngineKind(engineKind);
  }
}
