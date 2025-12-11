package ai.floedb.floecat.gateway.iceberg.rest.support.mapper;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.NamespaceInfoDto;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class NamespaceResponseMapper {
  private NamespaceResponseMapper() {}

  public static NamespaceInfoDto toInfo(Namespace ns) {
    List<String> path = toPath(ns);
    Map<String, String> props = new LinkedHashMap<>(ns.getPropertiesMap());
    if (ns.hasDescription() && ns.getDescription() != null && !ns.getDescription().isBlank()) {
      props.putIfAbsent("description", ns.getDescription());
    }
    if (ns.hasPolicyRef() && ns.getPolicyRef() != null && !ns.getPolicyRef().isBlank()) {
      props.putIfAbsent("policy_ref", ns.getPolicyRef());
    }
    return new NamespaceInfoDto(path, Map.copyOf(props));
  }

  public static List<String> toPath(Namespace ns) {
    return ns.getParentsList().isEmpty()
        ? List.of(ns.getDisplayName())
        : concat(ns.getParentsList(), ns.getDisplayName());
  }

  private static List<String> concat(List<String> parents, String name) {
    List<String> out = new ArrayList<>(parents);
    out.add(name);
    return out;
  }
}
