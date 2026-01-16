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

package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.catalog.rpc.GetSchemaRequest;
import ai.floedb.floecat.catalog.rpc.GetSchemaResponse;
import ai.floedb.floecat.catalog.rpc.SchemaService;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jboss.logging.Logger;

/**
 * Catalog-facing SchemaService.
 *
 * <p>Selects the correct physical schema JSON (table-level or snapshot-level), then delegates JSON
 * → logical schema conversion to LogicalSchemaMapper.
 */
@Singleton
@GrpcService
public class SchemaServiceImpl extends BaseServiceImpl implements SchemaService {

  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject LogicalSchemaMapper logicalSchema;
  @Inject CatalogOverlay catalogOverlay;

  private static final Logger LOG = Logger.getLogger(SchemaService.class);

  @Override
  public Uni<GetSchemaResponse> getSchema(GetSchemaRequest request) {
    var L = LogHelper.start(LOG, "GetSchema");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, "table.read");

                  var tableId = request.getTableId();
                  ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId());

                  CatalogOverlay.SchemaResolution resolved =
                      catalogOverlay.schemaFor(
                          correlationId(),
                          tableId,
                          request.hasSnapshot() ? request.getSnapshot() : null);

                  SchemaDescriptor desc =
                      logicalSchema.map(resolved.table(), resolved.schemaJson());

                  return GetSchemaResponse.newBuilder().setSchema(desc).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }
}
