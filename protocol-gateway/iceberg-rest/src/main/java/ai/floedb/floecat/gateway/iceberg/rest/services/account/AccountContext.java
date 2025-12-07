package ai.floedb.floecat.gateway.iceberg.rest.services.account;

import jakarta.enterprise.context.RequestScoped;

@RequestScoped
public class AccountContext {
  private String accountId;

  public void setAccountId(String accountId) {
    this.accountId = accountId;
  }

  public String getAccountId() {
    return accountId;
  }
}
