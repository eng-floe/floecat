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

package ai.floedb.floecat.service.integration;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/** Stable account-owned table identities mounted under Floe's built-in sys namespace. */
public final class AccountLogSysTables {
  public static final List<String> NAMES = List.of("query_log", "session_log", "audit_log");

  private AccountLogSysTables() {}

  public static ResourceId namespaceId() {
    return SystemNodeRegistry.resourceId(
        "floedb", ResourceKind.RK_NAMESPACE, NameRef.newBuilder().setName("sys").build());
  }

  public static ResourceId tableId(String accountId, String name) {
    if (!NAMES.contains(name)) {
      throw new IllegalArgumentException("Unknown account log table: " + name);
    }
    UUID id =
        UUID.nameUUIDFromBytes(
            ("floe-account-log-table\0" + accountId + "\0" + name)
                .getBytes(StandardCharsets.UTF_8));
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setKind(ResourceKind.RK_TABLE)
        .setId(id.toString())
        .build();
  }

  public static boolean isTableId(ResourceId id) {
    if (id == null || id.getKind() != ResourceKind.RK_TABLE) return false;
    for (String name : NAMES) {
      if (id.equals(tableId(id.getAccountId(), name))) return true;
    }
    return false;
  }
}
