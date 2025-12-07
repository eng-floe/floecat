package ai.floedb.floecat.service.query.graph.snapshot;

import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc.SnapshotServiceBlockingStub;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.graph.MetadataGraph.SchemaResolution;
import ai.floedb.floecat.service.query.graph.model.TableNode;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Handles snapshot overrides, pinning, and schema resolution for table nodes.
 *
 * <p>This helper encapsulates the logic previously embedded inside MetadataGraph so both
 * QueryInputResolver and SchemaService can share consistent semantics.
 */
public class SnapshotHelper {

  public interface SnapshotClient {
    ai.floedb.floecat.catalog.rpc.GetSnapshotResponse getSnapshot(
        ai.floedb.floecat.catalog.rpc.GetSnapshotRequest request);
  }

  private SnapshotClient snapshotClient;
  private final SnapshotRepository snapshotRepository;

  public SnapshotHelper(
      SnapshotRepository snapshotRepository, SnapshotServiceBlockingStub snapshotStub) {
    this.snapshotRepository = snapshotRepository;
    this.snapshotClient =
        snapshotStub != null
            ? snapshotStub::getSnapshot
            : req -> {
              throw new IllegalStateException("Snapshot client not configured");
            };
  }

  public void setSnapshotClient(SnapshotClient client) {
    this.snapshotClient = client;
  }

  public SnapshotPin snapshotPinFor(
      String correlationId,
      ResourceId tableId,
      SnapshotRef override,
      Optional<Timestamp> asOfDefault) {

    if (override != null && override.hasSnapshotId()) {
      return buildSnapshotPin(tableId, override.getSnapshotId(), null);
    }

    if (override != null && override.hasAsOf()) {
      return buildSnapshotPin(tableId, 0L, override.getAsOf());
    }

    if (asOfDefault.isPresent()) {
      return buildSnapshotPin(tableId, 0L, asOfDefault.get());
    }

    var response =
        snapshotClient.getSnapshot(
            GetSnapshotRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                .build());
    return buildSnapshotPin(tableId, response.getSnapshot().getSnapshotId(), null);
  }

  public SchemaResolution schemaFor(
      String correlationId,
      TableNode tableNode,
      SnapshotRef snapshotRef,
      java.util.function.Supplier<String> schemaSupplier) {
    String schemaJson = schemaJsonFor(correlationId, tableNode, snapshotRef, schemaSupplier);
    return new SchemaResolution(tableNode, schemaJson);
  }

  public String schemaJsonFor(
      String correlationId,
      TableNode tableNode,
      SnapshotRef snapshotRef,
      java.util.function.Supplier<String> schemaSupplier) {
    if (snapshotRef == null || snapshotRef.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
      return schemaSupplier.get();
    }
    Snapshot resolved = resolveSnapshot(correlationId, tableNode.id(), snapshotRef);
    if (resolved == null || resolved.getSchemaJson().isBlank()) {
      return schemaSupplier.get();
    }
    return resolved.getSchemaJson();
  }

  private Snapshot resolveSnapshot(
      String correlationId, ResourceId tableId, SnapshotRef snapshotRef) {
    return switch (snapshotRef.getWhichCase()) {
      case SNAPSHOT_ID ->
          snapshotRepository
              .getById(tableId, snapshotRef.getSnapshotId())
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          correlationId,
                          "snapshot",
                          Map.of(
                              "table_id", tableId.getId(),
                              "snapshot_id", Long.toString(snapshotRef.getSnapshotId()))));
      case AS_OF ->
          snapshotRepository
              .getAsOf(tableId, snapshotRef.getAsOf())
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          correlationId,
                          "snapshot",
                          Map.of(
                              "table_id", tableId.getId(),
                              "as_of",
                                  Instant.ofEpochSecond(
                                          snapshotRef.getAsOf().getSeconds(),
                                          snapshotRef.getAsOf().getNanos())
                                      .toString())));
      case SPECIAL -> {
        if (snapshotRef.getSpecial() != SpecialSnapshot.SS_CURRENT) {
          throw GrpcErrors.invalidArgument(
              correlationId,
              "snapshot.special.unsupported",
              Map.of("requested", snapshotRef.getSpecial().name()));
        }
        yield snapshotRepository
            .getCurrentSnapshot(tableId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        correlationId, "snapshot", Map.of("table_id", tableId.getId())));
      }
      case WHICH_NOT_SET ->
          throw GrpcErrors.invalidArgument(
              correlationId, "snapshot.missing", Map.of("table_id", tableId.getId()));
      default ->
          throw GrpcErrors.invalidArgument(
              correlationId, "snapshot.missing", Map.of("table_id", tableId.getId()));
    };
  }

  private SnapshotPin buildSnapshotPin(ResourceId tableId, long snapshotId, Timestamp ts) {
    SnapshotPin.Builder builder = SnapshotPin.newBuilder().setTableId(tableId);
    if (snapshotId > 0) {
      builder.setSnapshotId(snapshotId);
    }
    if (ts != null) {
      builder.setAsOf(ts);
    }
    return builder.build();
  }
}
