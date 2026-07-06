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

package ai.floedb.floecat.service.security.impl;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;

@ApplicationScoped
public class Authorizer {
  public void require(PrincipalContext principalContext, String permission) {
    if (principalContext.getPermissionsList().contains(permission)) {
      return;
    }
    throw denied(principalContext, "missing permission: " + permission);
  }

  public void require(PrincipalContext principalContext, List<String> permissions) {
    for (String permission : permissions) {
      if (principalContext.getPermissionsList().contains(permission)) {
        return;
      }
    }
    throw denied(principalContext, "missing permissions: " + permissions);
  }

  /**
   * The exact default instance is not a caller that lacks a grant — it is a caller whose identity
   * never arrived (interceptor bypassed, or the call context lost before authorization;
   * eng-floe/floecat#361): every channel degrades to {@link PrincipalContext#getDefaultInstance} on
   * a total miss, while every auth path populates at least a subject, account id, or correlation
   * id. Reporting the default instance as UNAUTHENTICATED instead of PERMISSION_DENIED keeps the
   * next propagation bug diagnosable, without re-coding any legitimately-denied principal a future
   * auth mode might produce (even one with a blank subject and no permissions, as long as it
   * carries any other field).
   */
  private static RuntimeException denied(PrincipalContext principalContext, String detail) {
    if (PrincipalContext.getDefaultInstance().equals(principalContext)) {
      return Status.UNAUTHENTICATED
          .withDescription("no authenticated principal on this call (" + detail + ")")
          .asRuntimeException();
    }
    return Status.PERMISSION_DENIED.withDescription(detail).asRuntimeException();
  }
}
