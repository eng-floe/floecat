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

package ai.floedb.floecat.service.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class RolePermissionsTest {

  private static final List<String> READ_PERMS =
      List.of("account.read", "catalog.read", "namespace.read", "table.read", "view.read");
  private static final List<String> FULL_PERMS =
      List.of(
          "account.read",
          "catalog.read",
          "catalog.write",
          "namespace.read",
          "namespace.write",
          "table.read",
          "table.write",
          "view.read",
          "view.write",
          "connector.manage",
          "system-objects.read");
  private static final List<String> INIT_ACCOUNT_PERMS =
      List.of(
          "account.write", "catalog.read", "catalog.write", "namespace.read", "namespace.write");
  private static final List<String> PLATFORM_PERMS = List.of("account.read", "account.write");

  @Test
  void defaultRoleGrantsReadPermissions() {
    var permissions = RolePermissions.permissionsForRoles(List.of("default"), false);

    assertThat(permissions).containsExactlyInAnyOrderElementsOf(READ_PERMS);
  }

  @Test
  void administratorRoleGrantsFullTenantPermissions() {
    var permissions = RolePermissions.permissionsForRoles(List.of("administrator"), false);

    assertThat(permissions).containsExactlyInAnyOrderElementsOf(FULL_PERMS);
  }

  @Test
  void developerRoleGrantsFullTenantPermissions() {
    var permissions = RolePermissions.permissionsForRoles(List.of("developer"), false);

    assertThat(permissions).containsExactlyInAnyOrderElementsOf(FULL_PERMS);
  }

  @Test
  void platformAdminRoleGrantsAccountManagementPermissions() {
    var permissions =
        RolePermissions.permissionsForRoles(List.of(RolePermissions.platformAdminRole()), false);

    assertThat(permissions).containsExactlyInAnyOrderElementsOf(PLATFORM_PERMS);
  }

  @Test
  void systemObjectsRoleGrantsSystemObjectsReadPermission() {
    var permissions =
        RolePermissions.permissionsForRoles(List.of(RolePermissions.SYSTEM_OBJECTS_ROLE), false);

    assertThat(permissions)
        .contains("system-objects.read")
        .doesNotContain(
            "catalog.write",
            "namespace.write",
            "account.write",
            "catalog.read",
            "namespace.read",
            "table.read",
            "view.read");
  }

  @Test
  void initAccountRoleGrantsBootstrapMutationPermissions() {
    var permissions =
        RolePermissions.permissionsForRoles(List.of(RolePermissions.INIT_ACCOUNT_ROLE), false);

    assertThat(permissions).containsExactlyInAnyOrderElementsOf(INIT_ACCOUNT_PERMS);
  }
}
