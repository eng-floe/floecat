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

package ai.floedb.floecat.extensions.floedb.utils;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import java.util.Optional;

public final class ScannerUtils {

  private ScannerUtils() {}

  /**
   * Decode an engine-specific payload using a typed PayloadDescriptor. Any exception or missing
   * payload results in Optional.empty().
   */
  public static <T> Optional<T> payload(
      SystemObjectScanContext ctx, ResourceId id, PayloadDescriptor<T> desc) {
    if (ctx == null) {
      return Optional.empty();
    }
    return payload(ctx.overlay(), id, desc, ctx.engineContext());
  }

  /** Resolve a PostgreSQL OID using a typed payload descriptor. */
  public static <T> int oid(
      SystemObjectScanContext ctx,
      ResourceId id,
      PayloadDescriptor<T> desc,
      java.util.function.ToIntFunction<T> extractor) {
    if (ctx == null) {
      return fallbackOid(id);
    }
    return oid(ctx.overlay(), id, desc, extractor, ctx.engineContext());
  }

  /** Resolve an int[] field using a typed payload descriptor. */
  public static <T> int[] array(
      SystemObjectScanContext ctx,
      ResourceId id,
      PayloadDescriptor<T> desc,
      java.util.function.Function<T, int[]> extractor) {
    if (ctx == null) {
      return new int[0];
    }
    return array(ctx.overlay(), id, desc, extractor, ctx.engineContext());
  }

  /** Decode an engine-specific payload using a typed PayloadDescriptor via overlay. */
  public static <T> Optional<T> payload(
      CatalogOverlay overlay,
      ResourceId id,
      PayloadDescriptor<T> desc,
      EngineContext engineContext) {
    EngineContext ctx = engineContext == null ? EngineContext.empty() : engineContext;
    return overlay
        .resolve(id)
        .flatMap(node -> node.engineHint(ctx.normalizedKind(), ctx.normalizedVersion()))
        .flatMap(
            hint -> {
              if (!desc.type().equals(hint.contentType())) {
                return Optional.empty();
              }
              try {
                return Optional.ofNullable(desc.decoder().apply(hint.payload()));
              } catch (Exception ignored) {
                return Optional.empty();
              }
            });
  }

  /** Resolve a PostgreSQL OID using a typed payload descriptor via overlay. */
  public static <T> int oid(
      CatalogOverlay overlay,
      ResourceId id,
      PayloadDescriptor<T> desc,
      java.util.function.ToIntFunction<T> extractor,
      EngineContext engineContext) {
    return payload(overlay, id, desc, engineContext)
        .map(extractor::applyAsInt)
        .filter(v -> v > 0)
        .orElseGet(() -> fallbackOid(id));
  }

  /** Resolve an int[] field using a typed payload descriptor via overlay. */
  public static <T> int[] array(
      CatalogOverlay overlay,
      ResourceId id,
      PayloadDescriptor<T> desc,
      java.util.function.Function<T, int[]> extractor,
      EngineContext engineContext) {
    return payload(overlay, id, desc, engineContext)
        .map(extractor)
        .filter(arr -> arr != null && arr.length > 0)
        .orElseGet(() -> new int[0]);
  }

  // Legacy helpers – prefer PayloadDescriptor-based overloads

  /**
   * Decode an engine-specific payload once and return a typed value. Any exception or missing
   * payload results in Optional.empty().
   */
  public static <T> Optional<T> payload(
      SystemObjectScanContext ctx,
      ResourceId id,
      String payloadType,
      ThrowingFunction<byte[], T> decoder) {
    if (ctx == null) {
      return Optional.empty();
    }
    return payload(
        ctx.overlay(), id, PayloadDescriptor.of(payloadType, decoder), ctx.engineContext());
  }

  /**
   * Resolve a PostgreSQL OID for a graph node.
   *
   * <p>Resolution order: 1. Engine-specific payload (if present and decodable) 2. Deterministic
   * fallback based on ResourceId
   */
  public static int oid(
      SystemObjectScanContext ctx,
      ResourceId id,
      String payloadType,
      ThrowingFunction<byte[], Integer> extractor) {
    if (ctx == null) {
      return fallbackOid(id);
    }
    return payload(
            ctx.overlay(), id, PayloadDescriptor.of(payloadType, extractor), ctx.engineContext())
        .filter(v -> v > 0)
        .orElseGet(() -> fallbackOid(id));
  }

  public static int[] array(
      SystemObjectScanContext ctx,
      ResourceId id,
      String payloadType,
      ThrowingFunction<byte[], int[]> extractor) {
    if (ctx == null) {
      return new int[0];
    }
    return payload(
            ctx.overlay(), id, PayloadDescriptor.of(payloadType, extractor), ctx.engineContext())
        .filter(arr -> arr != null && arr.length > 0)
        .orElseGet(() -> new int[0]);
  }

  /** Stable, deterministic fallback OID */
  public static int fallbackOid(ResourceId id) {
    return Math.abs(id.hashCode());
  }

  /** Default system owner (postgres-compatible) */
  public static int defaultOwnerOid() {
    return 10;
  }

  public static SchemaColumn col(String name, String type) {
    return SchemaColumn.newBuilder().setName(name).setLogicalType(type).setNullable(false).build();
  }
}
