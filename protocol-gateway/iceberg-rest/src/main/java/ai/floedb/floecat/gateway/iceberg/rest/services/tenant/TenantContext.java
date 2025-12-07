package ai.floedb.floecat.gateway.iceberg.rest.services.tenant;

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
