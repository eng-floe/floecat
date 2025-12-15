package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;

/** Shared context helpers for tests that need to simulate security/engine metadata. */
public final class SecurityTestSupport {

  private SecurityTestSupport() {}

  public static final class FakePrincipalProvider extends PrincipalProvider {
    private PrincipalContext ctx;

    public FakePrincipalProvider(String accountId) {
      this.ctx = PrincipalContext.newBuilder().setAccountId(accountId).build();
    }

    public void setAccount(String accountId) {
      this.ctx = PrincipalContext.newBuilder().setAccountId(accountId).build();
    }

    @Override
    public PrincipalContext get() {
      return ctx;
    }
  }
}
