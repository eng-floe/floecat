package ai.floedb.floecat.gateway.iceberg.rest.support;

import java.util.Map;
import java.util.function.BiConsumer;

public final class MetadataLocationUtil {

  public static final String PRIMARY_KEY = "metadata-location";
  public static final String LEGACY_KEY = "metadata_location";

  private MetadataLocationUtil() {}

  public static String metadataLocation(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return null;
    }
    String location = props.get(PRIMARY_KEY);
    if (location == null || location.isBlank()) {
      location = props.get(LEGACY_KEY);
    }
    return (location == null || location.isBlank()) ? null : location;
  }

  public static void setMetadataLocation(Map<String, String> props, String metadataLocation) {
    if (props == null) {
      return;
    }
    setMetadataLocation(props::put, metadataLocation);
  }

  public static void setMetadataLocation(
      BiConsumer<String, String> setter, String metadataLocation) {
    if (setter == null || metadataLocation == null || metadataLocation.isBlank()) {
      return;
    }
    setter.accept(PRIMARY_KEY, metadataLocation);
    setter.accept(LEGACY_KEY, metadataLocation);
  }

  public static boolean updateMetadataLocation(Map<String, String> props, String metadataLocation) {
    if (props == null || metadataLocation == null || metadataLocation.isBlank()) {
      return false;
    }
    boolean mutated = !metadataLocation.equals(props.get(PRIMARY_KEY));
    props.put(PRIMARY_KEY, metadataLocation);
    if (!metadataLocation.equals(props.get(LEGACY_KEY))) {
      mutated = true;
    }
    props.put(LEGACY_KEY, metadataLocation);
    return mutated;
  }

  public static String metadataDirectory(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return null;
    }
    String trimmed = metadataLocation;
    while (trimmed.endsWith("/") && trimmed.length() > 1) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    int slash = trimmed.lastIndexOf('/');
    if (slash < 0) {
      return null;
    }
    return trimmed.substring(0, slash);
  }

  public static boolean isPointer(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return false;
    }
    int slash = metadataLocation.lastIndexOf('/');
    String file = slash >= 0 ? metadataLocation.substring(slash + 1) : metadataLocation;
    return "metadata.json".equalsIgnoreCase(file);
  }
}
