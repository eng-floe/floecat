package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.GetSchemaRequest;
import ai.floedb.metacat.catalog.rpc.GetSchemaResponse;
import ai.floedb.metacat.catalog.rpc.SchemaService;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.query.rpc.SchemaColumn;
import ai.floedb.metacat.query.rpc.SchemaDescriptor;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.LogHelper;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.SnapshotRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jboss.logging.Logger;

@GrpcService
public class SchemaServiceImpl extends BaseServiceImpl implements SchemaService {
  @Inject SnapshotRepository snapshotRepo;
  @Inject TableRepository tableRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;

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

                  String schemaJson = table.getSchemaJson();
                  if (request.hasSnapshot()) {
                    var ref = request.getSnapshot();
                    var snap = snapshotRepo.getById(tableId, resolveSnapshotId(tableId, ref));
                    if (snap.isPresent() && !snap.get().getSchemaJson().isBlank()) {
                      schemaJson = snap.get().getSchemaJson();
                    }
                  }

                  if (schemaJson == null || schemaJson.isBlank()) {
                    return GetSchemaResponse.newBuilder()
                        .setSchema(SchemaDescriptor.getDefaultInstance())
                        .build();
                  }

                  TableFormat fmt = table.getUpstream().getFormat();
                  SchemaDescriptor schema =
                      switch (fmt) {
                        case TF_ICEBERG ->
                            mapIceberg(
                                schemaJson,
                                new HashSet<>(table.getUpstream().getPartitionKeysList()));
                        case TF_DELTA ->
                            mapDelta(
                                schemaJson,
                                new HashSet<>(table.getUpstream().getPartitionKeysList()),
                                new HashMap<>(table.getUpstream().getFieldIdByPathMap()));
                        default -> SchemaDescriptor.getDefaultInstance();
                      };

                  return GetSchemaResponse.newBuilder().setSchema(schema).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private SchemaDescriptor mapIceberg(String schemaJson, Set<String> partitionKeys) {
    Schema iceberg = SchemaParser.fromJson(schemaJson);
    SchemaDescriptor.Builder sb = SchemaDescriptor.newBuilder();
    for (Types.NestedField f : iceberg.columns()) {
      addField(sb, f, "", partitionKeys);
    }
    return sb.build();
  }

  private void addField(
      SchemaDescriptor.Builder sb, Types.NestedField f, String prefix, Set<String> partKeys) {
    String physical = prefix.isEmpty() ? f.name() : prefix + "." + f.name();
    boolean isPartition = partKeys.contains(f.name()) || partKeys.contains(physical);
    sb.addColumns(
        SchemaColumn.newBuilder()
            .setName(f.name())
            .setLogicalType(f.type().toString())
            .setFieldId(f.fieldId())
            .setNullable(!f.isRequired())
            .setPhysicalPath(physical)
            .setPartitionKey(isPartition)
            .build());

    Type t = f.type();
    if (t instanceof Types.StructType st) {
      for (Types.NestedField child : st.fields()) {
        addField(sb, child, physical, partKeys);
      }
    } else if (t instanceof Types.ListType lt && lt.elementType() instanceof Types.StructType st) {
      String childPrefix = physical + "[]";
      for (Types.NestedField child : st.fields()) {
        addField(sb, child, childPrefix, partKeys);
      }
    } else if (t instanceof Types.MapType mt && mt.valueType() instanceof Types.StructType st) {
      String childPrefix = physical + "{}";
      for (Types.NestedField child : st.fields()) {
        addField(sb, child, childPrefix, partKeys);
      }
    }
  }

  private SchemaDescriptor mapDelta(
      String schemaJson, Set<String> partitionKeys, HashMap<String, Integer> fieldIds) {
    SchemaDescriptor.Builder sb = SchemaDescriptor.newBuilder();
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode root = mapper.readTree(schemaJson);
      AtomicInteger nextId = new AtomicInteger(1);
      walkDeltaStruct(sb, root, "", partitionKeys, fieldIds, nextId);
    } catch (Exception e) {
      LOG.warn("Failed to parse Delta schema JSON, returning empty schema", e);
    }
    return sb.build();
  }

  private void walkDeltaStruct(
      SchemaDescriptor.Builder sb,
      JsonNode node,
      String prefix,
      Set<String> partKeys,
      HashMap<String, Integer> fieldIds,
      AtomicInteger nextId) {
    if (node == null || !node.has("fields")) {
      return;
    }
    ArrayNode fields = (ArrayNode) node.get("fields");
    for (JsonNode f : fields) {
      String name = f.path("name").asText();
      String logicalType =
          f.path("type").isTextual() ? f.get("type").asText() : f.path("type").toString();
      boolean nullable = f.path("nullable").asBoolean(true);
      String physical = prefix.isEmpty() ? name : prefix + "." + name;
      boolean isPartition = partKeys.contains(name) || partKeys.contains(physical);
      int fieldId = fieldIds.getOrDefault(physical, nextId.getAndIncrement());

      sb.addColumns(
          SchemaColumn.newBuilder()
              .setName(name)
              .setLogicalType(logicalType)
              .setFieldId(fieldId)
              .setNullable(nullable)
              .setPhysicalPath(physical)
              .setPartitionKey(isPartition)
              .build());

      JsonNode typeNode = f.get("type");
      if (typeNode != null
          && typeNode.isObject()
          && "struct".equals(typeNode.path("type").asText(""))) {
        walkDeltaStruct(sb, typeNode, physical, partKeys, fieldIds, nextId);
      }
      if (typeNode != null
          && typeNode.isObject()
          && "array".equals(typeNode.path("type").asText(""))) {
        JsonNode elem = typeNode.get("elementType");
        if (elem != null && elem.isObject() && "struct".equals(elem.path("type").asText(""))) {
          walkDeltaStruct(sb, elem, physical + "[]", partKeys, fieldIds, nextId);
        }
      }
      if (typeNode != null
          && typeNode.isObject()
          && "map".equals(typeNode.path("type").asText(""))) {
        JsonNode valueType = typeNode.get("valueType");
        if (valueType != null
            && valueType.isObject()
            && "struct".equals(valueType.path("type").asText(""))) {
          walkDeltaStruct(sb, valueType, physical + "{}", partKeys, fieldIds, nextId);
        }
      }
    }
  }

  private long resolveSnapshotId(
      ai.floedb.metacat.common.rpc.ResourceId tableId, SnapshotRef ref) {
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
