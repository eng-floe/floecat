package ai.floedb.metacat.service.security.impl;

import java.util.List;

import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;

import ai.floedb.metacat.common.rpc.PrincipalContext;

@ApplicationScoped
public class Authorizer {
  public void require(PrincipalContext p, String permission) {
    if (p.getPermissionsList().contains(permission)) return;
    throw Status.PERMISSION_DENIED.withDescription("missing permission: " + permission).asRuntimeException();
  }

  public void require(PrincipalContext p, List<String> permissions) {
    for (String permission : permissions) {
      if (p.getPermissionsList().contains(permission)) {
          return;
      }
    }
    throw Status.PERMISSION_DENIED.withDescription("missing permissions: " + permissions).asRuntimeException();
  }
}