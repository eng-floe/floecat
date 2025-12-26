package ai.floedb.floecat.extensions.floedb.utils;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import java.util.Optional;

public final class ScannerUtils {

  private ScannerUtils() {}

  /**
   * Decode an engine-specific payload using a typed PayloadDescriptor. Any exception or missing
   * payload results in Optional.empty().
   */
  public static <T> Optional<T> payload(
      SystemObjectScanContext ctx, ResourceId id, PayloadDescriptor<T> desc) {
    return ctx.tryResolve(id)
        .flatMap(
            node -> {
              for (EngineHint hint : node.engineHints().values()) {
                if (desc.type().equals(hint.contentType())) {
                  try {
                    return Optional.ofNullable(desc.decoder().apply(hint.payload()));
                  } catch (Exception ignored) {
                    return Optional.empty();
                  }
                }
              }
              return Optional.empty();
            });
  }

  /** Resolve a PostgreSQL OID using a typed payload descriptor. */
  public static <T> int oid(
      SystemObjectScanContext ctx,
      ResourceId id,
      PayloadDescriptor<T> desc,
      java.util.function.ToIntFunction<T> extractor) {
    return payload(ctx, id, desc)
        .map(extractor::applyAsInt)
        .filter(v -> v > 0)
        .orElseGet(() -> fallbackOid(id));
  }

  /** Resolve an int[] field using a typed payload descriptor. */
  public static <T> int[] array(
      SystemObjectScanContext ctx,
      ResourceId id,
      PayloadDescriptor<T> desc,
      java.util.function.Function<T, int[]> extractor) {
    return payload(ctx, id, desc)
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
    return ctx.tryResolve(id)
        .flatMap(
            node -> {
              for (EngineHint hint : node.engineHints().values()) {
                if (payloadType.equals(hint.contentType())) {
                  try {
                    return Optional.ofNullable(decoder.apply(hint.payload()));
                  } catch (Exception ignored) {
                    return Optional.empty();
                  }
                }
              }
              return Optional.empty();
            });
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
    return payload(ctx, id, payloadType, extractor)
        .filter(v -> v > 0)
        .orElseGet(() -> fallbackOid(id));
  }

  public static int[] array(
      SystemObjectScanContext ctx,
      ResourceId id,
      String payloadType,
      ThrowingFunction<byte[], int[]> extractor) {
    return payload(ctx, id, payloadType, extractor)
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
