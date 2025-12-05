package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.GetSchemaRequest;
import ai.floedb.metacat.catalog.rpc.GetSchemaResponse;
import ai.floedb.metacat.catalog.rpc.SchemaService;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.query.rpc.SchemaDescriptor;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.LogHelper;
import ai.floedb.metacat.service.query.graph.MetadataGraph;
import ai.floedb.metacat.service.query.resolver.LogicalSchemaMapper;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
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
  @Inject MetadataGraph metadataGraph;

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

                  MetadataGraph.SchemaResolution resolved =
                      metadataGraph.schemaFor(
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
