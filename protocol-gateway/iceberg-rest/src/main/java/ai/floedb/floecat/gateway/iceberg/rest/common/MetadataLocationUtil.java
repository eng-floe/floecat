package ai.floedb.floecat.gateway.iceberg.rest.common;

import java.util.Map;
import java.util.function.BiConsumer;

public final class MetadataLocationUtil {

  public static final String PRIMARY_KEY = "metadata-location";

  private MetadataLocationUtil() {}

  public static String metadataLocation(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return null;
    }
    String location = props.get(PRIMARY_KEY);
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
  }

  public static boolean updateMetadataLocation(Map<String, String> props, String metadataLocation) {
    if (props == null || metadataLocation == null || metadataLocation.isBlank()) {
      return false;
    }
    boolean mutated = !metadataLocation.equals(props.get(PRIMARY_KEY));
    props.put(PRIMARY_KEY, metadataLocation);
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

  public static String canonicalMetadataDirectory(String metadataLocation) {
    String directory = metadataDirectory(metadataLocation);
    if (directory == null || directory.isBlank()) {
      return directory;
    }
    return directory;
  }
}
