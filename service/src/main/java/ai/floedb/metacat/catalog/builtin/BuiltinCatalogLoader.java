package ai.floedb.metacat.catalog.builtin;

import ai.floedb.metacat.query.rpc.BuiltinRegistry;
import com.google.protobuf.TextFormat;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Loads immutable builtin catalog files (`<engine_kind>.pb|pbtxt`) from the configured location and
 * caches them by engine kind.
 */
@ApplicationScoped
public class BuiltinCatalogLoader {

  static final String DEFAULT_LOCATION = "classpath:builtins";
  private static final Pattern SAFE_ENGINE_KIND = Pattern.compile("^[A-Za-z0-9._-]+$");

  @ConfigProperty(name = "metacat.builtins.location", defaultValue = DEFAULT_LOCATION)
  String configuredLocation;

  private final Map<String, BuiltinCatalogData> cache = new ConcurrentHashMap<>();

  private String location;

  @PostConstruct
  void init() {
    location = configuredLocation == null ? DEFAULT_LOCATION : configuredLocation.trim();
    if (location.isBlank()) {
      location = DEFAULT_LOCATION;
    }
  }

  public BuiltinCatalogData getCatalog(String engineKind) {
    String sanitized = sanitizeEngineKind(engineKind);
    return cache.computeIfAbsent(sanitized, this::loadCatalog);
  }

  private BuiltinCatalogData loadCatalog(String engineKind) {
    BuiltinCatalogData data = loadFromBase(engineKind, engineKind);
    if (data != null) {
      return data;
    }
    throw new BuiltinCatalogNotFoundException(engineKind);
  }

  private BuiltinCatalogData loadFromBase(String baseName, String engineKind) {
    try (InputStream binary = openStream(baseName + ".pb")) {
      if (binary != null) {
        return BuiltinCatalogProtoMapper.fromProto(BuiltinRegistry.parseFrom(binary), engineKind);
      }
    } catch (IOException e) {
      throw new BuiltinCatalogLoadException(engineKind, e);
    }

    try (InputStream text = openStream(baseName + ".pbtxt")) {
      if (text != null) {
        var builder = BuiltinRegistry.newBuilder();
        try (var reader = new InputStreamReader(text, StandardCharsets.UTF_8)) {
          TextFormat.getParser().merge(reader, builder);
        }
        return BuiltinCatalogProtoMapper.fromProto(builder.build(), engineKind);
      }
    } catch (IOException e) {
      throw new BuiltinCatalogLoadException(engineKind, e);
    }

    return null;
  }

  private static String sanitizeEngineKind(String engineKind) {
    if (engineKind == null) {
      throw new IllegalArgumentException("engine_kind must be provided");
    }
    String trimmed = engineKind.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException("engine_kind must be provided");
    }
    if (!SAFE_ENGINE_KIND.matcher(trimmed).matches()) {
      throw new IllegalArgumentException(
          "engine_kind may only contain letters, digits, '.', '_', or '-'");
    }
    return trimmed;
  }

  private InputStream openStream(String fileName) throws IOException {
    Objects.requireNonNull(fileName, "fileName");

    // Classpath mode
    if (location.startsWith("classpath:")) {
      String base = location.substring("classpath:".length());
      if (base.startsWith("/")) {
        base = base.substring(1);
      }
      String resource =
          base.isBlank() ? fileName : base.endsWith("/") ? base + fileName : base + "/" + fileName;
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      return cl.getResourceAsStream(resource);
    }

    // Filesystem mode
    String basePath = location.startsWith("file:") ? location.substring(5) : location;
    Path dir = Paths.get(basePath);
    Path resolved = dir.resolve(fileName);
    if (Files.exists(resolved)) {
      return Files.newInputStream(resolved);
    }
    return null;
  }
}
