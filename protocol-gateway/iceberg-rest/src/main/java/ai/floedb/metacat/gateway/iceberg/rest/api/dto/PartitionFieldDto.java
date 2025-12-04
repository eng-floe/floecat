package ai.floedb.metacat.gateway.iceberg.rest.api.dto;

import ai.floedb.metacat.catalog.rpc.PartitionField;

/** DTO for a partition spec field. */
public record PartitionFieldDto(int fieldId, String name, String transform) {
  public static PartitionFieldDto fromProto(PartitionField field) {
    if (field == null) {
      return null;
    }
    return new PartitionFieldDto(field.getFieldId(), field.getName(), field.getTransform());
  }
}
