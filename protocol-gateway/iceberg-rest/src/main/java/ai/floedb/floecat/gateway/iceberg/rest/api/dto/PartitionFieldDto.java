package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import ai.floedb.floecat.catalog.rpc.PartitionField;

public record PartitionFieldDto(int fieldId, String name, String transform) {
  public static PartitionFieldDto fromProto(PartitionField field) {
    if (field == null) {
      return null;
    }
    return new PartitionFieldDto(field.getFieldId(), field.getName(), field.getTransform());
  }
}
