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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.eclipse.microprofile.config.ConfigProvider;

public final class RolePermissions {
  public static final String PLATFORM_ADMIN_ROLE = "platform-admin";
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
          "connector.manage");
  private static final List<String> PLATFORM_PERMS = List.of("account.read", "account.write");

  private RolePermissions() {}

  public static List<String> permissionsForRoles(Collection<String> roles, boolean devMode) {
    Set<String> normalized = normalizeRoles(roles);
    if (normalized.isEmpty()) {
      normalized.add(devMode ? "developer" : "default");
    }

    String platformRole = platformAdminRole().toLowerCase(Locale.ROOT);
    Set<String> perms = new LinkedHashSet<>();
    for (String role : normalized) {
      if (role.equals(platformRole)) {
        perms.addAll(PLATFORM_PERMS);
        continue;
      }
      switch (role) {
        case "administrator":
        case "developer":
          perms.addAll(FULL_PERMS);
          break;
        case "default":
          perms.addAll(READ_PERMS);
          break;
        default:
          break;
      }
    }
    if (devMode) {
      perms.add("account.write");
    }
    if (perms.isEmpty()) {
      perms.addAll(devMode ? FULL_PERMS : READ_PERMS);
    }
    return List.copyOf(perms);
  }

  public static String platformAdminRole() {
    return ConfigProvider.getConfig()
        .getOptionalValue("floecat.auth.platform-admin.role", String.class)
        .map(String::trim)
        .filter(value -> !value.isBlank())
        .orElse(PLATFORM_ADMIN_ROLE);
  }

  private static Set<String> normalizeRoles(Collection<String> roles) {
    Set<String> normalized = new LinkedHashSet<>();
    if (roles == null) {
      return normalized;
    }
    for (String role : roles) {
      if (role == null || role.isBlank()) {
        continue;
      }
      normalized.add(role.trim().toLowerCase(Locale.ROOT));
    }
    return normalized;
  }
}
