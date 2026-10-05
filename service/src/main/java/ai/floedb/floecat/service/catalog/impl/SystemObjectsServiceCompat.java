/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.query.rpc.GetSqlObjectsRegistryRequest;
import ai.floedb.floecat.query.rpc.GetSystemObjectsRequest;
import ai.floedb.floecat.query.rpc.GetSystemObjectsResponse;
import ai.floedb.floecat.query.rpc.SystemObjectsService;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

/** Deprecated wire-level alias for clients that still call SystemObjectsService. */
@GrpcService
public class SystemObjectsServiceCompat implements SystemObjectsService {

  @Inject SqlCatalogServiceImpl delegate;

  @Override
  public Uni<GetSystemObjectsResponse> getSystemObjects(GetSystemObjectsRequest request) {
    // The legacy implementation selected the catalog from inbound engine headers as well; the
    // request fields remain on the wire for old clients but were never authoritative.
    return delegate
        .getSqlObjectsRegistry(GetSqlObjectsRegistryRequest.getDefaultInstance())
        .map(
            response ->
                GetSystemObjectsResponse.newBuilder().setRegistry(response.getRegistry()).build());
  }
}
