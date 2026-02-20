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

  @Test
  void routeInitPlannerRoleGrantsReadOnlyPlannerBootstrapPermissions() {
    var permissions =
        RolePermissions.permissionsForRoles(
            List.of(RolePermissions.ROUTE_INIT_PLANNER_ROLE), false);

    assertThat(permissions)
        .contains("catalog.read", "namespace.read", "table.read", "view.read")
        .doesNotContain("catalog.write", "namespace.write", "account.write");
  }

  @Test
  void routeInitAccountRoleGrantsBootstrapMutationPermissions() {
    var permissions =
        RolePermissions.permissionsForRoles(
            List.of(RolePermissions.ROUTE_INIT_ACCOUNT_ROLE), false);

    assertThat(permissions)
        .contains(
            "account.write", "catalog.read", "catalog.write", "namespace.read", "namespace.write");
  }
}
