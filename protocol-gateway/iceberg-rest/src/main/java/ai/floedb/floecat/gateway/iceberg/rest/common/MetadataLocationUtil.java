package ai.floedb.floecat.gateway.iceberg.rest.common;

import java.util.Map;
import java.util.function.BiConsumer;

public final class MetadataLocationUtil {

  public static final String PRIMARY_KEY = "metadata-location";
  private static final String METADATA_MIRROR_SEGMENT = "/.floecat-metadata";

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
    return stripMetadataMirrorPrefix(directory);
  }

  public static boolean isPointer(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return false;
    }
    int slash = metadataLocation.lastIndexOf('/');
    String file = slash >= 0 ? metadataLocation.substring(slash + 1) : metadataLocation;
    return "metadata.json".equalsIgnoreCase(file);
  }

  public static boolean isMirrorMetadataLocation(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return false;
    }
    try {
      java.net.URI uri = java.net.URI.create(metadataLocation);
      String path = uri.getPath();
      return path != null && path.startsWith(METADATA_MIRROR_SEGMENT + "/");
    } catch (IllegalArgumentException e) {
      return metadataLocation.startsWith(METADATA_MIRROR_SEGMENT + "/");
    }
  }

  public static String stripMetadataMirrorPrefix(String location) {
    if (location == null || location.isBlank()) {
      return location;
    }
    try {
      java.net.URI uri = java.net.URI.create(location);
      if (uri.getScheme() != null && uri.getAuthority() != null) {
        String path = uri.getPath();
        if (path != null && path.startsWith(METADATA_MIRROR_SEGMENT)) {
          String stripped = path.substring(METADATA_MIRROR_SEGMENT.length());
          if (stripped.isBlank()) {
            stripped = "/";
          }
          return uri.getScheme() + "://" + uri.getAuthority() + stripped;
        }
        return location;
      }
    } catch (IllegalArgumentException e) {
      // ignore; handle raw path below
    }
    if (location.startsWith(METADATA_MIRROR_SEGMENT + "/")) {
      return location.substring(METADATA_MIRROR_SEGMENT.length());
    }
    return location;
  }
}
