package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.io.FileIO;

public final class FileIoFactory {
  private static final String DEFAULT_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";
  private static final Set<String> IO_PROP_PREFIXES =
      Set.of("s3.", "s3a.", "s3n.", "fs.", "client.", "aws.", "hadoop.");

  private FileIoFactory() {}

  public static FileIO createFileIo(
      Map<String, String> props, IcebergGatewayConfig config, boolean allowConfigOverrides) {
    Map<String, String> normalized =
        props == null ? new LinkedHashMap<>() : new LinkedHashMap<>(props);
    if (allowConfigOverrides && config != null) {
      config.metadataFileIoRoot().ifPresent(root -> normalized.put("fs.floecat.test-root", root));
    }
    RuntimeFileIoOverrides.mergeInto(normalized);
    String impl =
        allowConfigOverrides && config != null
            ? config.metadataFileIo().orElse(normalized.getOrDefault("io-impl", DEFAULT_IO_IMPL))
            : normalized.getOrDefault("io-impl", DEFAULT_IO_IMPL);
    impl = impl == null ? DEFAULT_IO_IMPL : impl.trim();
    try {
      Class<?> clazz = Class.forName(impl);
      Object instance = clazz.getDeclaredConstructor().newInstance();
      if (!(instance instanceof FileIO fileIO)) {
        throw new IllegalArgumentException(impl + " does not implement FileIO");
      }
      fileIO.initialize(filterIoProperties(normalized));
      return fileIO;
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to instantiate FileIO " + impl, e);
    }
  }

  public static Map<String, String> filterIoProperties(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return Map.of();
    }
    Map<String, String> filtered = new LinkedHashMap<>();
    props.forEach(
        (key, value) -> {
          if (key == null || value == null) {
            return;
          }
          for (String prefix : IO_PROP_PREFIXES) {
            if (key.startsWith(prefix)) {
              filtered.put(key, value);
              return;
            }
          }
          if ("io-impl".equals(key)) {
            filtered.put(key, value);
          }
        });
    return filtered;
  }
}
