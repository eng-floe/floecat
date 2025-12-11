package ai.floedb.floecat.gateway.iceberg.rest.support;

import java.net.URI;

public final class MirrorLocationUtil {
  private static final String METADATA_MIRROR_SEGMENT = "/.floecat-metadata";

  private MirrorLocationUtil() {}

  public static boolean isMirrorMetadataLocation(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return false;
    }
    try {
      URI uri = URI.create(metadataLocation);
      String path = uri.getPath();
      return path != null && path.startsWith(METADATA_MIRROR_SEGMENT + "/");
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static String stripMetadataMirrorPrefix(String location) {
    if (location == null || location.isBlank()) {
      return location;
    }
    try {
      URI uri = URI.create(location);
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
      // fall through to raw handling
    }
    if (location.startsWith(METADATA_MIRROR_SEGMENT + "/")) {
      return location.substring(METADATA_MIRROR_SEGMENT.length());
    }
    return location;
  }
}
