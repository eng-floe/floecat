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
import ai.floedb.floecat.extensions.floedb.engine.oid.EngineOidGeneratorProvider;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import com.google.protobuf.Message;
import java.util.Objects;
import java.util.Optional;

public final class ScannerUtils {

  private ScannerUtils() {}

  /** Policy controlling how we resolve OIDs when hints are missing. */
  public enum OidPolicy {
    /** SYSTEM semantics: OID must exist in persisted engine hints; otherwise fail. */
    REQUIRE,
    /** USER semantics: allow deterministic fallback OID if missing. */
    FALLBACK
  }

  public static OidPolicy oidPolicy(GraphNodeOrigin origin) {
    return origin == GraphNodeOrigin.SYSTEM ? OidPolicy.REQUIRE : OidPolicy.FALLBACK;
  }

  public static OidPolicy oidPolicy(ResourceId id) {
    return SystemResourceIdGenerator.isSystemId(id) ? OidPolicy.REQUIRE : OidPolicy.FALLBACK;
  }

  private static <T extends Message> void checkDescriptorMessageClass(
      FloePayloads.Descriptor descriptor, Class<T> messageClass) {
    if (!descriptor.messageClass().equals(messageClass)) {
      throw new IllegalArgumentException(
          "Descriptor "
              + descriptor.name()
              + " expects "
              + descriptor.messageClass().getSimpleName()
              + ", got "
              + messageClass.getSimpleName());
    }
  }

  public static <T extends Message> Optional<T> payload(
      SystemObjectScanContext ctx,
      ResourceId id,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass) {
    if (ctx == null) {
      return Optional.empty();
    }
    checkDescriptorMessageClass(descriptor, messageClass);
    return payload(ctx.overlay(), id, descriptor, messageClass, ctx.engineContext());
  }

  /** Resolve a PostgreSQL OID using a typed payload descriptor. */
  public static <T extends Message> int oid(
      SystemObjectScanContext ctx,
      ResourceId id,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass,
      java.util.function.ToIntFunction<T> extractor) {

    OidPolicy policy = oidPolicy(id);
    if (ctx == null) {
      if (policy == OidPolicy.REQUIRE) {
        throw new MissingSystemMetadataException(
            "Missing OID for SYSTEM object id="
                + id
                + " payloadType="
                + descriptor.type()
                + " (no scan context)");
      }
      return fallbackOid(id, descriptor.type());
    }
    return oid(ctx.overlay(), id, descriptor, messageClass, extractor, ctx.engineContext(), policy);
  }

  /** Resolve an int[] field using a typed payload descriptor. */
  public static <T extends Message> int[] array(
      SystemObjectScanContext ctx,
      ResourceId id,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass,
      java.util.function.Function<T, int[]> extractor) {

    OidPolicy policy = oidPolicy(id);
    if (ctx == null) {
      if (policy == OidPolicy.REQUIRE) {
        throw new MissingSystemMetadataException(
            "Missing array field for SYSTEM object id="
                + id
                + " payloadType="
                + descriptor.type()
                + " (no scan context)");
      }
      return new int[0];
    }
    return array(
        ctx.overlay(), id, descriptor, messageClass, extractor, ctx.engineContext(), policy);
  }

  /** Decode an engine-specific payload using a typed Floe payload descriptor via overlay. */
  public static <T extends Message> Optional<T> payload(
      CatalogOverlay overlay,
      ResourceId id,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass,
      EngineContext engineContext) {
    checkDescriptorMessageClass(descriptor, messageClass);
    EngineContext ctx = engineContext == null ? EngineContext.empty() : engineContext;
    return overlay
        .resolve(id)
        .flatMap(
            node ->
                node.engineHint(ctx.normalizedKind(), ctx.normalizedVersion(), descriptor.type()))
        .flatMap(
            hint -> {
              if (!descriptor.type().equals(hint.payloadType())) {
                return Optional.empty();
              }
              try {
                Message decoded = descriptor.decode(hint.payload());
                if (!messageClass.isInstance(decoded)) {
                  return Optional.empty();
                }
                return Optional.of(messageClass.cast(decoded));
              } catch (Exception ignored) {
                return Optional.empty();
              }
            });
  }

  /** Decode a per-column engine hint payload stored on a {@link RelationNode}. */
  public static <T extends Message> Optional<T> columnPayload(
      CatalogOverlay overlay,
      ResourceId relationId,
      long columnId,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass,
      EngineContext engineContext) {
    checkDescriptorMessageClass(descriptor, messageClass);
    return columnHint(overlay, relationId, columnId, engineContext, descriptor.type())
        .flatMap(
            hint -> {
              try {
                Message decoded = descriptor.decode(hint.payload());
                if (!messageClass.isInstance(decoded)) {
                  return Optional.empty();
                }
                return Optional.of(messageClass.cast(decoded));
              } catch (Exception ignored) {
                return Optional.empty();
              }
            });
  }

  public static <T extends Message> Optional<T> columnPayload(
      SystemObjectScanContext ctx,
      ResourceId relationId,
      long columnId,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass) {
    if (ctx == null) {
      return Optional.empty();
    }
    return columnPayload(
        ctx.overlay(), relationId, columnId, descriptor, messageClass, ctx.engineContext());
  }

  private static Optional<EngineHint> columnHint(
      CatalogOverlay overlay,
      ResourceId relationId,
      long columnId,
      EngineContext engineContext,
      String payloadType) {
    if (overlay == null || relationId == null) {
      return Optional.empty();
    }
    EngineContext ctx = engineContext == null ? EngineContext.empty() : engineContext;
    return overlay
        .resolve(relationId)
        .filter(RelationNode.class::isInstance)
        .map(RelationNode.class::cast)
        .map(relation -> relation.columnHints().get(columnId))
        .filter(Objects::nonNull)
        .map(
            hints ->
                hints.get(
                    new EngineHintKey(ctx.normalizedKind(), ctx.normalizedVersion(), payloadType)))
        .filter(Objects::nonNull);
  }

  /** Resolve a PostgreSQL OID using a typed payload descriptor via overlay. */
  public static <T extends Message> int oid(
      CatalogOverlay overlay,
      ResourceId id,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass,
      java.util.function.ToIntFunction<T> extractor,
      EngineContext engineContext) {
    return oid(overlay, id, descriptor, messageClass, extractor, engineContext, OidPolicy.FALLBACK);
  }

  public static <T extends Message> int oid(
      CatalogOverlay overlay,
      ResourceId id,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass,
      java.util.function.ToIntFunction<T> extractor,
      EngineContext engineContext,
      OidPolicy policy) {
    checkDescriptorMessageClass(descriptor, messageClass);
    Optional<Integer> resolved =
        payload(overlay, id, descriptor, messageClass, engineContext)
            .map(extractor::applyAsInt)
            .filter(v -> v > 0);

    if (resolved.isPresent()) {
      return resolved.get();
    }

    if (policy == OidPolicy.REQUIRE) {
      throw new MissingSystemOidException(
          "Missing OID for SYSTEM object id=" + id + " payloadType=" + descriptor.type());
    }
    return fallbackOid(id, descriptor.type());
  }

  /** Resolve an int[] field using a typed payload descriptor via overlay. */
  public static <T extends Message> int[] array(
      CatalogOverlay overlay,
      ResourceId id,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass,
      java.util.function.Function<T, int[]> extractor,
      EngineContext engineContext) {
    return array(
        overlay, id, descriptor, messageClass, extractor, engineContext, OidPolicy.FALLBACK);
  }

  public static <T extends Message> int[] array(
      CatalogOverlay overlay,
      ResourceId id,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass,
      java.util.function.Function<T, int[]> extractor,
      EngineContext engineContext,
      OidPolicy policy) {
    checkDescriptorMessageClass(descriptor, messageClass);
    Optional<int[]> resolved =
        payload(overlay, id, descriptor, messageClass, engineContext)
            .map(extractor)
            .filter(arr -> arr != null && arr.length > 0);

    if (resolved.isPresent()) {
      return resolved.get();
    }
    if (policy == OidPolicy.REQUIRE) {
      throw new MissingSystemMetadataException(
          "Missing array field for SYSTEM object id=" + id + " payloadType=" + descriptor.type());
    }
    return new int[0];
  }

  /** Stable, deterministic fallback OID (USER objects only; callers must enforce policy). */
  public static int fallbackOid(ResourceId id, String payloadType) {
    return EngineOidGeneratorProvider.instance().generate(id, payloadType);
  }

  /** Default system owner (postgres-compatible) */
  public static int defaultOwnerOid() {
    return 10;
  }

  public static SchemaColumn col(String name, String type) {
    return SchemaColumn.newBuilder().setName(name).setLogicalType(type).setNullable(false).build();
  }
}
