package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.PartitionSpecInfo;
import java.util.List;
import java.util.stream.Collectors;

/** DTO capturing partition spec metadata. */
public record PartitionSpecDto(int specId, String specName, List<PartitionFieldDto> fields) {
  public static PartitionSpecDto fromProto(PartitionSpecInfo spec) {
    if (spec == null) {
      return null;
    }
    List<PartitionFieldDto> fields =
        spec.getFieldsList().stream().map(PartitionFieldDto::fromProto).collect(Collectors.toList());
    String name = spec.getSpecName();
    return new PartitionSpecDto(spec.getSpecId(), name == null ? "" : name, fields);
  }
}
