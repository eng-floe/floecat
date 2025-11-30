package ai.floedb.metacat.gateway.iceberg.rest;

import jakarta.enterprise.context.RequestScoped;

@RequestScoped
public class TenantContext {
  private String tenantId;

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  public String getTenantId() {
    return tenantId;
  }
}
