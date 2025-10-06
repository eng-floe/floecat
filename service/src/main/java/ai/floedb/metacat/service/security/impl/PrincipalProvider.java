package ai.floedb.metacat.service.security.impl;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import io.grpc.Context;
import jakarta.enterprise.context.RequestScoped;

@RequestScoped
public class PrincipalProvider {
  public static final Context.Key<PrincipalContext> KEY = Context.key("principal");
  public PrincipalContext get() { return KEY.get() != null ? KEY.get() : PrincipalContext.getDefaultInstance(); }
}