package ai.floedb.metacat.service.security.impl;

import io.grpc.Context;
import jakarta.enterprise.context.RequestScoped;

import ai.floedb.metacat.common.rpc.PrincipalContext;

@RequestScoped
public class PrincipalProvider {
  public static final Context.Key<PrincipalContext> KEY = Context.key("principal");
  
  public PrincipalContext get() { 
    return KEY.get() != null ? KEY.get() : PrincipalContext.getDefaultInstance(); 
  }
}
