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
package ai.floedb.floecat.service.metagraph.overlay.systemobjects;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.UUID;

/**
 * Utility that centralizes the translation between user-facing references and the `_system`
 * snapshot.
 *
 * <p>This class handles three responsibilities:
 *
 * <ol>
 *   <li>Normalize any UUID stamped by {@link SystemResourceIdGenerator} so the account is {@link
 *       SystemNodeRegistry#SYSTEM_ACCOUNT} before the snapshot lookup.
 *   <li>Rewrite namespaces/tables to the current engine’s catalog context before querying the
 *       system graph.
 *   <li>Alias matched system names back to the user catalog so responses remain user-friendly.
 * </ol>
 */
public final class SystemCatalogTranslator {
  private SystemCatalogTranslator() {}

  /**
   * Rewrites {@code candidate} to use the `_system` account when it carries a system marker UUID.
   *
   * <p>If the ID is not system-generated the method returns {@code null} so callers can skip
   * normalization and treat the value as a user resource.
   */
  public static ResourceId normalizeSystemId(ResourceId candidate) {
    if (candidate == null || candidate.getId() == null) {
      return null;
    }
    UUID uuid;
    try {
      uuid = UUID.fromString(candidate.getId());
    } catch (IllegalArgumentException e) {
      return null;
    }
    if (!SystemResourceIdGenerator.isSystemId(uuid)) {
      return null;
    }
    if (SystemNodeRegistry.SYSTEM_ACCOUNT.equals(candidate.getAccountId())) {
      return candidate;
    }
    return candidate.toBuilder().setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT).build();
  }

  /**
   * Converts a user-provided namespace reference into the engine-specific system catalog namespace.
   *
   * <p>The catalog is replaced with {@code ctx.effectiveEngineKind()} when present, and the path is
   * reconstructed so the system graph can resolve the same namespace even if the user supplied a
   * different catalog name.
   */
  public static NameRef toSystemNamespaceRef(NameRef userRef, EngineContext ctx) {
    String catalog = ctx == null ? userRef.getCatalog() : ctx.effectiveEngineKind();
    List<String> nsPath = NameRefUtil.namespacePath(userRef);
    NameRef.Builder builder = NameRef.newBuilder().setCatalog(catalog);
    if (!nsPath.isEmpty()) {
      for (int i = 0; i < nsPath.size() - 1; i++) {
        builder.addPath(nsPath.get(i));
      }
      builder.setName(nsPath.get(nsPath.size() - 1));
    } else {
      builder.setName(userRef.getName());
    }
    return builder.build();
  }

  /**
   * Turns a user-facing relation reference (table/view) into the matching system catalog reference
   * for the current engine.
   */
  public static NameRef toSystemRelationRef(NameRef userRef, EngineContext ctx) {
    String catalog = ctx == null ? userRef.getCatalog() : ctx.effectiveEngineKind();
    return userRef.toBuilder().setCatalog(catalog).build();
  }

  /**
   * Rewrites a system canonical NameRef back to what the user originally requested (i.e., preserves
   * the catalog display name).
   */
  public static NameRef aliasToUserCatalog(NameRef input, NameRef systemName) {
    return systemName.toBuilder().setCatalog(input.getCatalog()).build();
  }
}
