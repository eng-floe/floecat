package ai.floedb.floecat.connector.iceberg.impl;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

final class RuntimeFileIoOverrides {
  private static final String ENV_OVERRIDE_PREFIX = "FLOECAT_FILEIO_OVERRIDE_";
  private static final String SYS_OVERRIDE_PREFIX = "floecat.fileio.override.";
  private static final Map<String, String> LEGACY_AWS_MAPPINGS =
      Map.of(
          "aws.s3.endpoint-override", "s3.endpoint",
          "aws.region", "s3.region",
          "aws.accessKeyId", "s3.access-key-id",
          "aws.secretAccessKey", "s3.secret-access-key",
          "aws.sessionToken", "s3.session-token");

  private RuntimeFileIoOverrides() {}

  static void mergeInto(Map<String, String> target) {
    if (target == null) {
      return;
    }
    Map<String, String> resolved = resolveOverrides();
    if (resolved.isEmpty()) {
      return;
    }
    resolved.forEach(
        (k, v) -> {
          if (k != null && !k.isBlank() && v != null && !v.isBlank()) {
            target.putIfAbsent(k, v);
          }
        });
  }

  private static Map<String, String> resolveOverrides() {
    Map<String, String> overrides = new LinkedHashMap<>();
    collectSystemOverrides(overrides);
    collectEnvironmentOverrides(overrides);
    collectLegacyAwsMappings(overrides);
    collectLegacyAwsFlags(overrides);
    return overrides;
  }

  private static void collectSystemOverrides(Map<String, String> overrides) {
    Properties props = System.getProperties();
    for (String name : props.stringPropertyNames()) {
      if (name.startsWith(SYS_OVERRIDE_PREFIX)) {
        String key = name.substring(SYS_OVERRIDE_PREFIX.length());
        addOverride(overrides, key, props.getProperty(name));
      } else if (isDirectFileIoProperty(name)) {
        addOverride(overrides, name, props.getProperty(name));
      }
    }
  }

  private static void collectEnvironmentOverrides(Map<String, String> overrides) {
    System.getenv()
        .forEach(
            (key, value) -> {
              if (key.startsWith(ENV_OVERRIDE_PREFIX)) {
                String property = key.substring(ENV_OVERRIDE_PREFIX.length());
                property = property.replace("__", "/").replace('_', '.').toLowerCase(Locale.ROOT);
                addOverride(overrides, property, value);
              }
            });
  }

  private static void collectLegacyAwsMappings(Map<String, String> overrides) {
    LEGACY_AWS_MAPPINGS.forEach(
        (legacy, target) -> resolveValue(legacy).ifPresent(value -> addOverride(overrides, target, value)));
  }

  private static void collectLegacyAwsFlags(Map<String, String> overrides) {
    if (resolveBoolean("aws.s3.force-path-style")) {
      addOverride(overrides, "s3.path-style-access", "true");
    }
    resolveValue("aws.requestChecksumCalculation")
        .ifPresent(value -> addOverride(overrides, "aws.requestChecksumCalculation", value));
    resolveValue("aws.responseChecksumValidation")
        .ifPresent(value -> addOverride(overrides, "aws.responseChecksumValidation", value));
  }

  private static boolean isDirectFileIoProperty(String name) {
    return name.startsWith("s3.")
        || name.startsWith("s3a.")
        || name.startsWith("s3n.")
        || name.startsWith("fs.")
        || name.startsWith("client.")
        || name.startsWith("aws.")
        || name.startsWith("hadoop.")
        || "io-impl".equals(name);
  }

  private static void addOverride(Map<String, String> overrides, String key, String value) {
    if (key == null || key.isBlank() || value == null || value.isBlank()) {
      return;
    }
    overrides.putIfAbsent(key.trim(), value.trim());
  }

  private static Optional<String> resolveValue(String name) {
    Optional<String> sys = optionalValue(System.getProperty(name));
    if (sys.isPresent()) {
      return sys;
    }
    String envName = toEnvName(name);
    return optionalValue(System.getenv(envName));
  }

  private static Optional<String> optionalValue(String value) {
    return value == null || value.isBlank() ? Optional.empty() : Optional.of(value);
  }

  private static boolean resolveBoolean(String name) {
    return resolveValue(name).map(Boolean::parseBoolean).orElse(false);
  }

  private static String toEnvName(String name) {
    return name.replace('.', '_').replace('-', '_').toUpperCase(Locale.ROOT);
  }
}
