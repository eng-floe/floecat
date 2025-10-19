package ai.floedb.metacat.service.tenancy.impl;

import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TenantRegistry {

  // TODO: build proper tenant repo
  private static final Set<String> VALID_TENANTS = Set.of("t-0001", "t-0002");

  public boolean exists(String tenantId) {
    return tenantId != null && VALID_TENANTS.contains(tenantId);
  }
}
