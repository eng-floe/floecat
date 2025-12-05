package ai.floedb.metacat.service.query.resolver;

import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.query.rpc.SchemaColumn;
import ai.floedb.metacat.query.rpc.SchemaDescriptor;
import ai.floedb.metacat.service.query.graph.model.TableNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jboss.logging.Logger;

/**
 * LogicalSchemaMapper -------------------
 *
 * <p>Converts physical table metadata (Iceberg/Delta or generic catalog schemas) into the unified
 * logical SchemaDescriptor consumed by the query planner.
 *
 * <p>This class is *pure*: - no RPCs - no snapshot lookups - no QueryContext interaction
 */
@ApplicationScoped
public class LogicalSchemaMapper {

  private static final Logger LOG = Logger.getLogger(LogicalSchemaMapper.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Build a logical SchemaDescriptor from catalog metadata and a schema JSON string.
   *
   * <p>Caller is responsible for selecting the correct schemaJson (table schema vs snapshot
   * schema).
   */
  public SchemaDescriptor map(Table table, String schemaJson) {
    if (schemaJson == null || schemaJson.isBlank()) {
      return SchemaDescriptor.getDefaultInstance();
    }

    TableFormat fmt = table.getUpstream().getFormat();
    Set<String> partitionKeys = new HashSet<>(table.getUpstream().getPartitionKeysList());
    Map<String, Integer> fieldIds = new HashMap<>(table.getUpstream().getFieldIdByPathMap());

    return mapInternal(fmt, schemaJson, partitionKeys, fieldIds);
  }

  /** Builds a logical schema descriptor directly from a cached {@link TableNode}. */
  public SchemaDescriptor map(TableNode node, String overrideSchemaJson) {
    String schemaJson =
        (overrideSchemaJson == null || overrideSchemaJson.isBlank())
            ? node.schemaJson()
            : overrideSchemaJson;
    if (schemaJson == null || schemaJson.isBlank()) {
      return SchemaDescriptor.getDefaultInstance();
    }
    return mapInternal(
        node.format(),
        schemaJson,
        new HashSet<>(node.partitionKeys()),
        new HashMap<>(node.fieldIdByPath()));
  }

  public SchemaDescriptor map(TableNode node) {
    return map(node, node.schemaJson());
  }

  private SchemaDescriptor mapInternal(
      TableFormat format,
      String schemaJson,
      Set<String> partitionKeys,
      Map<String, Integer> fieldIds) {
    return switch (format) {
      case TF_ICEBERG -> mapIceberg(schemaJson, partitionKeys);
      case TF_DELTA -> mapDelta(schemaJson, partitionKeys, fieldIds);
      default -> mapGeneric(schemaJson);
    };
  }

  // =========================================================================
  // ICEBERG
  // =========================================================================

  private SchemaDescriptor mapIceberg(String schemaJson, Set<String> partitionKeys) {
    try {
      Schema iceberg = SchemaParser.fromJson(schemaJson);
      SchemaDescriptor.Builder sb = SchemaDescriptor.newBuilder();

      for (Types.NestedField field : iceberg.columns()) {
        addIcebergField(sb, field, "", partitionKeys);
      }

      return sb.build();

    } catch (Exception e) {
      LOG.warn("Invalid Iceberg schema, falling back to generic: " + schemaJson, e);
      return mapGeneric(schemaJson);
    }
  }

  private void addIcebergField(
      SchemaDescriptor.Builder sb,
      Types.NestedField field,
      String prefix,
      Set<String> partitionKeys) {

    String physical = prefix.isEmpty() ? field.name() : prefix + "." + field.name();

    boolean isPartition = partitionKeys.contains(field.name()) || partitionKeys.contains(physical);

    sb.addColumns(
        SchemaColumn.newBuilder()
            .setName(field.name())
            .setLogicalType(field.type().toString())
            .setFieldId(field.fieldId())
            .setNullable(!field.isRequired())
            .setPhysicalPath(physical)
            .setPartitionKey(isPartition)
            .build());

    Type t = field.type();

    // struct
    if (t instanceof Types.StructType st) {
      for (Types.NestedField child : st.fields()) {
        addIcebergField(sb, child, physical, partitionKeys);
      }
      return;
    }

    // list<struct>
    if (t instanceof Types.ListType lt && lt.elementType() instanceof Types.StructType st2) {
      String childPrefix = physical + "[]";
      for (Types.NestedField child : st2.fields()) {
        addIcebergField(sb, child, childPrefix, partitionKeys);
      }
      return;
    }

    // map<*, struct>
    if (t instanceof Types.MapType mt && mt.valueType() instanceof Types.StructType st3) {
      String childPrefix = physical + "{}";
      for (Types.NestedField child : st3.fields()) {
        addIcebergField(sb, child, childPrefix, partitionKeys);
      }
    }
  }

  // =========================================================================
  // DELTA
  // =========================================================================

  private SchemaDescriptor mapDelta(
      String schemaJson, Set<String> partitionKeys, Map<String, Integer> fieldIds) {

    SchemaDescriptor.Builder sb = SchemaDescriptor.newBuilder();

    try {
      JsonNode root = MAPPER.readTree(schemaJson);
      AtomicInteger nextId = new AtomicInteger(1);
      walkDeltaStruct(sb, root, "", partitionKeys, fieldIds, nextId);

    } catch (Exception e) {
      LOG.warn("Failed to parse Delta schema JSON; returning empty schema", e);
    }

    return sb.build();
  }

  private void walkDeltaStruct(
      SchemaDescriptor.Builder sb,
      JsonNode node,
      String prefix,
      Set<String> partitionKeys,
      Map<String, Integer> fieldIds,
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

      boolean isPartition = partitionKeys.contains(name) || partitionKeys.contains(physical);

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
      if (typeNode == null || !typeNode.isObject()) {
        continue;
      }

      // struct
      if ("struct".equals(typeNode.path("type").asText(""))) {
        walkDeltaStruct(sb, typeNode, physical, partitionKeys, fieldIds, nextId);
      }

      // list<struct>
      if ("array".equals(typeNode.path("type").asText(""))) {
        JsonNode elem = typeNode.get("elementType");
        if (elem != null && elem.isObject() && "struct".equals(elem.path("type").asText(""))) {

          walkDeltaStruct(sb, elem, physical + "[]", partitionKeys, fieldIds, nextId);
        }
      }

      // map<*, struct>
      if ("map".equals(typeNode.path("type").asText(""))) {
        JsonNode val = typeNode.get("valueType");
        if (val != null && val.isObject() && "struct".equals(val.path("type").asText(""))) {

          walkDeltaStruct(sb, val, physical + "{}", partitionKeys, fieldIds, nextId);
        }
      }
    }
  }

  // =========================================================================
  // GENERIC FALLBACK (non-Iceberg, non-Delta)
  // =========================================================================

  /**
   * Expected generic schema form:
   *
   * <p>{ "cols": [ { "name": "id", "type": "int" }, ... ] }
   *
   * <p>Used for: - internal test tables - connectors without Iceberg/Delta metadata - legacy
   * catalog formats
   */
  private SchemaDescriptor mapGeneric(String schemaJson) {
    SchemaDescriptor.Builder sb = SchemaDescriptor.newBuilder();

    try {
      JsonNode root = MAPPER.readTree(schemaJson);
      JsonNode colsNode = root.path("cols");

      if (!colsNode.isArray()) {
        LOG.warn("Generic schema JSON does not contain 'cols' array: " + schemaJson);
        return sb.build();
      }

      int fieldId = 1;

      for (JsonNode col : colsNode) {
        String name = col.path("name").asText();
        String type = col.path("type").asText();

        sb.addColumns(
            SchemaColumn.newBuilder()
                .setName(name)
                .setLogicalType(type)
                .setFieldId(fieldId++) // deterministic order
                .setNullable(true) // assume nullable
                .setPhysicalPath(name)
                .setPartitionKey(false)
                .build());
      }

    } catch (Exception e) {
      LOG.warn("Failed to parse generic schema JSON; returning empty schema", e);
    }

    return sb.build();
  }
}
