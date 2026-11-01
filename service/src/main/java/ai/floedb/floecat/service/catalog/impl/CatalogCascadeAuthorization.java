/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.Authorizer;

/** The permissions required by every path that recursively deletes a catalog. */
public final class CatalogCascadeAuthorization {
  private CatalogCascadeAuthorization() {}

  public static void require(Authorizer authz, PrincipalContext principal) {
    authz.require(principal, RolePermissions.CATALOG_WRITE);
    authz.require(principal, "namespace.write");
    authz.require(principal, "table.write");
    authz.require(principal, "view.write");
  }
}
