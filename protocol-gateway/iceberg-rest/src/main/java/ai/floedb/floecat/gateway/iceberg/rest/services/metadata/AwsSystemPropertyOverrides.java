package ai.floedb.floecat.gateway.iceberg.rest.services.metadata;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

public final class AwsSystemPropertyOverrides {

  private AwsSystemPropertyOverrides() {}

  public static boolean mergeInto(Map<String, String> target) {
    if (target == null) {
      return false;
    }
    Map<String, String> overrides = resolveOverrides();
    if (overrides.isEmpty()) {
      return false;
    }
    overrides.forEach(target::putIfAbsent);
    return true;
  }

  private static Map<String, String> resolveOverrides() {
    Map<String, String> props = new LinkedHashMap<>();
    resolveValue("aws.s3.endpoint-override")
        .ifPresent(
            endpoint -> {
              props.put("s3.endpoint", endpoint);
              props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            });
    resolveValue("aws.region").ifPresent(region -> props.put("s3.region", region));
    resolveValue("aws.accessKeyId").ifPresent(key -> props.put("s3.access-key-id", key));
    resolveValue("aws.secretAccessKey")
        .ifPresent(secret -> props.put("s3.secret-access-key", secret));
    resolveValue("aws.sessionToken").ifPresent(token -> props.put("s3.session-token", token));
    if (resolveBoolean("aws.s3.force-path-style")) {
      props.put("s3.path-style-access", "true");
    }
    return props;
  }

  private static Optional<String> resolveValue(String name) {
    Optional<String> sys = optionalValue(System.getProperty(name));
    if (sys.isPresent()) {
      return sys;
    }
    Optional<String> cfg = resolveConfigValue(name);
    if (cfg.isPresent()) {
      return cfg;
    }
    String envName = toEnvName(name);
    return optionalValue(System.getenv(envName));
  }

  private static Optional<String> optionalValue(String value) {
    return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
  }

  private static Optional<String> resolveConfigValue(String name) {
    try {
      Config config = ConfigProvider.getConfig();
      if (config != null) {
        return config.getOptionalValue(name, String.class).filter(v -> !v.isBlank());
      }
    } catch (IllegalStateException | NoClassDefFoundError ignored) {
    }
    return Optional.empty();
  }

  private static boolean resolveBoolean(String name) {
    return resolveValue(name).map(Boolean::parseBoolean).orElse(false);
  }

  private static String toEnvName(String name) {
    return name.replace('.', '_').replace('-', '_').toUpperCase(Locale.ROOT);
  }
}
