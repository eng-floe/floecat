package ai.floedb.metacat.service.security.impl;

import java.util.List;

import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;

import ai.floedb.metacat.common.rpc.PrincipalContext;

@ApplicationScoped
public class Authorizer {
  public void require(PrincipalContext principalContext, String permission) {
    if (principalContext.getPermissionsList().contains(permission)) {
      return;
    }
    throw Status.PERMISSION_DENIED.withDescription(
        "missing permission: " + permission).asRuntimeException();
  }

  public void require(PrincipalContext principalContext, List<String> permissions) {
    for (String permission : permissions) {
      if (principalContext.getPermissionsList().contains(permission)) {
        return;
      }
    }
    throw Status.PERMISSION_DENIED.withDescription(
        "missing permissions: " + permissions).asRuntimeException();
  }
}
