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

package ai.floedb.floecat.extensions.floedb.pgcatalog;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Test ID helpers for pg_catalog scanners.
 *
 * <p>Important: SYSTEM vs USER semantics are enforced by runtime code (SYSTEM marker UUID check
 * and/or node origin). These helpers provide convenient IDs for the scopes that exist today.
 *
 * <p>NOTE: There are currently no USER types and no USER functions. Do not add
 * userType/userFunction helpers unless the product supports them.
 */
final class PgCatalogTestIds {

  private static final String ENGINE = "floedb";
  private static final String PG_NAMESPACE = "pg_catalog";

  // Default account ids used in tests (account id is not used to classify SYSTEM vs USER)
  static final String SYSTEM_ACCOUNT = "_system";
  static final String USER_ACCOUNT = "test";

  private PgCatalogTestIds() {}

  // ----------------------------------------------------------------------
  // SYSTEM ids (SystemNodeRegistry -> SystemResourceIdGenerator marker UUIDs)
  // ----------------------------------------------------------------------

  static ResourceId catalog() {
    return SystemNodeRegistry.systemCatalogContainerId(ENGINE);
  }

  static ResourceId namespace(String namespace) {
    return SystemNodeRegistry.resourceId(
        ENGINE, ResourceKind.RK_NAMESPACE, NameRefUtil.name(namespace));
  }

  static ResourceId table(String name) {
    return SystemNodeRegistry.resourceId(
        ENGINE, ResourceKind.RK_TABLE, NameRefUtil.name(PG_NAMESPACE, name));
  }

  static ResourceId view(String name) {
    return SystemNodeRegistry.resourceId(
        ENGINE, ResourceKind.RK_VIEW, NameRefUtil.name(PG_NAMESPACE, name));
  }

  static ResourceId function(String name) {
    return SystemNodeRegistry.resourceId(
        ENGINE, ResourceKind.RK_FUNCTION, NameRefUtil.name(PG_NAMESPACE, name));
  }

  static ResourceId type(String name) {
    return SystemNodeRegistry.resourceId(
        ENGINE, ResourceKind.RK_TYPE, NameRefUtil.name(PG_NAMESPACE, name));
  }

  static ResourceId type(NameRef name) {
    return SystemNodeRegistry.resourceId(ENGINE, ResourceKind.RK_TYPE, name);
  }

  // ----------------------------------------------------------------------
  // USER ids (UUID strings, guaranteed NOT to be classified as SYSTEM ids)
  // ----------------------------------------------------------------------

  static ResourceId userCatalog() {
    return userCatalog(USER_ACCOUNT);
  }

  static ResourceId userCatalog(String accountId) {
    return ResourceId.newBuilder()
        .setAccountId(normalizeAccount(accountId))
        // NOTE: do not depend on RK_CATALOG existing in proto
        .setKind(ResourceKind.RK_NAMESPACE)
        .setId(userUuid("catalog").toString())
        .build();
  }

  static ResourceId userNamespace(String namespace) {
    return userNamespace(USER_ACCOUNT, namespace);
  }

  static ResourceId userNamespace(String accountId, String namespace) {
    String ns = (namespace == null || namespace.isBlank()) ? "public" : namespace;
    return ResourceId.newBuilder()
        .setAccountId(normalizeAccount(accountId))
        .setKind(ResourceKind.RK_NAMESPACE)
        .setId(userUuid("ns:" + ns).toString())
        .build();
  }

  static ResourceId userTable(String name) {
    return userTable(USER_ACCOUNT, PG_NAMESPACE, name);
  }

  static ResourceId userTable(String accountId, String namespace, String name) {
    String ns = (namespace == null || namespace.isBlank()) ? PG_NAMESPACE : namespace;
    String n = (name == null || name.isBlank()) ? "t" : name;
    return ResourceId.newBuilder()
        .setAccountId(normalizeAccount(accountId))
        .setKind(ResourceKind.RK_TABLE)
        .setId(userUuid("table:" + ns + "." + n).toString())
        .build();
  }

  static ResourceId userView(String name) {
    return userView(USER_ACCOUNT, PG_NAMESPACE, name);
  }

  static ResourceId userView(String accountId, String namespace, String name) {
    String ns = (namespace == null || namespace.isBlank()) ? PG_NAMESPACE : namespace;
    String n = (name == null || name.isBlank()) ? "v" : name;
    return ResourceId.newBuilder()
        .setAccountId(normalizeAccount(accountId))
        .setKind(ResourceKind.RK_VIEW)
        .setId(userUuid("view:" + ns + "." + n).toString())
        .build();
  }

  // ----------------------------------------------------------------------
  // Internals
  // ----------------------------------------------------------------------

  private static String normalizeAccount(String accountId) {
    return (accountId == null || accountId.isBlank()) ? USER_ACCOUNT : accountId;
  }

  /**
   * Deterministic UUID generator for USER ids.
   *
   * <p>We intentionally generate valid UUID strings but guarantee they are NOT classified as SYSTEM
   * ids by {@link SystemResourceIdGenerator#isSystemId(UUID)}.
   */
  private static UUID userUuid(String seed) {
    String s = (seed == null) ? "" : seed;
    UUID u = UUID.nameUUIDFromBytes(("user:" + s).getBytes(StandardCharsets.UTF_8));

    for (int i = 0; i < 16 && SystemResourceIdGenerator.isSystemId(u); i++) {
      long msb = u.getMostSignificantBits();
      msb ^= (0xFFL << 56); // flip first byte
      u = new UUID(msb, u.getLeastSignificantBits());
    }

    if (SystemResourceIdGenerator.isSystemId(u)) {
      long msb = u.getMostSignificantBits() ^ (0xFFL << 48); // flip second byte
      u = new UUID(msb, u.getLeastSignificantBits());
    }

    return u;
  }
}
