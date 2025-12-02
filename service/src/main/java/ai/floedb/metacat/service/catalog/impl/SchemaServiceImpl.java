package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.GetSchemaRequest;
import ai.floedb.metacat.catalog.rpc.GetSchemaResponse;
import ai.floedb.metacat.catalog.rpc.SchemaService;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.query.rpc.SchemaDescriptor;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.LogHelper;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.query.resolve.LogicalSchemaMapper;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
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

  @Inject SnapshotRepository snapshotRepo;
  @Inject TableRepository tableRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject LogicalSchemaMapper logicalSchema;

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

                  var table =
                      tableRepo
                          .getById(tableId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId(), "table", Map.of("id", tableId.getId())));

                  // Choose effective schema JSON
                  String schemaJson = table.getSchemaJson();
                  if (request.hasSnapshot()) {
                    var ref = request.getSnapshot();
                    var snap = snapshotRepo.getById(tableId, resolveSnapshotId(tableId, ref));

                    if (snap.isPresent() && !snap.get().getSchemaJson().isBlank()) {
                      schemaJson = snap.get().getSchemaJson();
                    }
                  }

                  SchemaDescriptor desc = logicalSchema.map(table, schemaJson);

                  return GetSchemaResponse.newBuilder().setSchema(desc).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /**
   * Resolve snapshot reference (ID, As-Of, Special).
   *
   * <p>Only used here — Query RPCs use QueryInputResolver instead.
   */
  private long resolveSnapshotId(ai.floedb.metacat.common.rpc.ResourceId tableId, SnapshotRef ref) {

    return switch (ref.getWhichCase()) {
      case SNAPSHOT_ID -> ref.getSnapshotId();
      case AS_OF ->
          snapshotRepo
              .getAsOf(tableId, ref.getAsOf())
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          correlationId(), "snapshot", Map.of("table_id", tableId.getId())))
              .getSnapshotId();
      case SPECIAL -> {
        if (ref.getSpecial() != SpecialSnapshot.SS_CURRENT) {
          throw GrpcErrors.invalidArgument(correlationId(), "snapshot.special.missing", Map.of());
        }
        yield snapshotRepo
            .getCurrentSnapshot(tableId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        correlationId(), "snapshot", Map.of("table_id", tableId.getId())))
            .getSnapshotId();
      }
      default -> throw GrpcErrors.invalidArgument(correlationId(), "snapshot.missing", Map.of());
    };
  }
}
